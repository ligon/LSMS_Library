"""GH #770: currency-denominated ``u`` labels get no inferred kg factor.

``conversion_to_kgs`` infers a kg-per-unit factor for *every* non-metric
label that carries an Expenditure.  ``u='Value'`` used to get one for
exactly the same reason ``Heap`` / ``Bowl`` / ``Piece`` do -- GhanaLSS got
``value -> 0.49139``, Panama ``value -> 0.34892``.  ``_apply_kg_conversion``
then multiplied the cedi/balboa amount by it and relabelled the row
``u='kg'``, so ``food_quantities(units='kgs')`` served **currency as mass**
and ``food_prices(units='kgvalue')`` returned the CONSTANT ``1/0.49139 =
2.035038`` for every item in six of GhanaLSS's seven waves.

The diagnosis is *not* "Value is non-physical".  Value = Quantity x Price,
so ``Value / Price = Quantity``: prices are the units in which value
measures quantity, and the kg factor for ``Value`` exists and equals
``1/price``.  The defect is one of GRANULARITY -- ``conversion_to_kgs``
returns one factor per unit LABEL, while ``1/price`` varies over
``(j, t, m)``.  NaN is the honest interim state; recovering the factor from
a price source is separate work (Step 2 of #770).

What follows is the data-free half.  The data-gated half (measured row
counts on GhanaLSS and Panama) is at the bottom, skipped where the
microdata is unavailable.

Note on the two tests that look like they already cover this:
``test_food_prices_units_kwarg.py::test_food_prices_kgvalue_drops_lcu_rows``
and ``::test_food_quantities_kgs_carries_lcu_rows`` assert the intended
POST-fix behaviour and passed BEFORE the fix -- their fixture's lone
``u='Value'`` row sits in a household with no kg anchor, so no factor could
be inferred for it in that frame.  They look like protection and were not;
the positive control below is what stops this exclusion quietly widening.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import (
    KNOWN_METRIC,
    _CURRENCY_DENOMINATED_UNITS,
    _apply_kg_conversion,
    _get_kg_factors,
    conversion_to_kgs,
    food_prices_from_acquired,
    food_quantities_from_acquired,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def inference_frame():
    """A frame in which inference DOES fire, for both kinds of label.

    Shaped like the reproduction in GH #770 (legacy ``(t, m, i, u)`` index,
    ``i`` the item).  Per market: a ``kg`` anchor at 5/kg, a ``Heap`` at 15
    per heap (so 3 kg/heap is inferable), and currency rows in two casings.

    Before the fix ``conversion_to_kgs`` returned a ``Value`` key here; the
    inferred number did not depend on ``Quantity`` at all (verified in the
    plan by scaling the ``Value`` rows' Quantity 1x / 3x / 100x -- the factor
    did not move), which is why a "Quantity == Expenditure" detector would
    have been testing the symptom rather than the cause.
    """
    rows = []
    for m in ('A', 'B'):
        for k in range(6):
            rows.append(('2020', m, 'maize', 'kg', 2.0, 10.0))
            rows.append(('2020', m, 'maize', 'Heap', 1.0, 15.0))
            rows.append(('2020', m, 'maize', 'Value', 7.0 + k, 7.0 + k))
            rows.append(('2020', m, 'maize', 'VALUE', 3.0 + k, 3.0 + k))
    return pd.DataFrame(
        rows, columns=['t', 'm', 'i', 'u', 'Quantity', 'Expenditure'],
    ).set_index(['t', 'm', 'i', 'u'])


@pytest.fixture
def canonical_frame():
    """Canonical ``(t, v, i, j, u, s)`` food_acquired with a kg anchor.

    Unlike the fixture in ``test_food_prices_units_kwarg.py``, the ``Value``
    rows here share a household/item cell WITH kg rows, so the pre-fix
    inference had everything it needed to fabricate a factor for them.
    """
    tuples, qty, exp = [], [], []
    for k in range(6):
        tuples.append(('2020', 'C1', 'H1', 'maize', 'kg', 'purchased'))
        qty.append(2.0)
        exp.append(10.0)
        tuples.append(('2020', 'C1', 'H1', 'maize', 'Heap', 'purchased'))
        qty.append(1.0)
        exp.append(15.0)
        tuples.append(('2020', 'C1', 'H1', 'maize', 'Value', 'purchased'))
        qty.append(7.0 + k)
        exp.append(7.0 + k)
    idx = pd.MultiIndex.from_tuples(
        tuples, names=['t', 'v', 'i', 'j', 'u', 's'])
    return pd.DataFrame({'Quantity': qty, 'Expenditure': exp}, index=idx)


# ---------------------------------------------------------------------------
# The registry, and its link to country._RESERVED_U_SENTINELS
# ---------------------------------------------------------------------------

def test_registry_is_lowercase_and_regex_free():
    """Lower-cased keys (the lookup lower-cases), and no compiled regex.

    A compiled-regex module constant would land in the hashed import closure
    and is un-serialisable there -- GH #780.
    """
    assert _CURRENCY_DENOMINATED_UNITS == {'value'}
    for u in _CURRENCY_DENOMINATED_UNITS:
        assert isinstance(u, str) and u == u.lower()


# ---------------------------------------------------------------------------
# conversion_to_kgs / _get_kg_factors
# ---------------------------------------------------------------------------

def test_conversion_to_kgs_emits_no_currency_key(inference_frame):
    got = conversion_to_kgs(inference_frame)
    lowered = {str(k).lower() for k in got}
    assert not (lowered & _CURRENCY_DENOMINATED_UNITS), got


def test_conversion_to_kgs_positive_control(inference_frame):
    """A genuinely PHYSICAL non-metric label in the SAME frame still gets one.

    Without this the exclusion could widen to every inferred unit and every
    other assertion in this file would still pass.  ``Heap`` is the right
    control and ``kg`` is not: ``kg`` is in ``KNOWN_METRIC`` and never
    depends on the inference at all.
    """
    assert 'heap' not in KNOWN_METRIC          # so it MUST come from inference
    got = {str(k).lower(): v for k, v in conversion_to_kgs(inference_frame).items()}
    assert 'heap' in got, got
    # 15 per heap against 5 per kg -> 3 kg per heap.
    assert got['heap'] == pytest.approx(3.0)


def test_currency_labels_excluded_case_insensitively(inference_frame):
    """'Value' and 'VALUE' are both gone; the physical control survives both."""
    got = {str(k).lower() for k in conversion_to_kgs(inference_frame)}
    assert 'value' not in got
    assert 'heap' in got


def test_get_kg_factors_emits_no_currency_key(inference_frame):
    factors = _get_kg_factors(inference_frame)
    assert not (set(factors) & _CURRENCY_DENOMINATED_UNITS), factors
    assert factors['heap'] == pytest.approx(3.0)     # positive control again
    assert factors['kg'] == 1                        # KNOWN_METRIC untouched


def test_all_currency_frame_infers_nothing_without_raising(inference_frame):
    """The edge case this change CREATES: filtering can empty the frame.

    ``_get_kg_factors`` swallows ValueError / ZeroDivisionError / KeyError
    but lets TypeError / AttributeError propagate as programmer bugs, so an
    empty-frame crash here would surface as a hard failure, not a fallback.
    GhanaLSS 1987-88 and 1988-89 are 100% ``u='Value'``, so this is a real
    corpus shape, not a hypothetical.
    """
    u = inference_frame.index.get_level_values('u').astype(str).str.lower()
    only_currency = inference_frame[u.isin(_CURRENCY_DENOMINATED_UNITS)]
    assert len(only_currency) > 0
    assert conversion_to_kgs(only_currency) == {}
    factors = _get_kg_factors(only_currency)
    assert not (set(factors) & _CURRENCY_DENOMINATED_UNITS)


def test_currency_rows_cannot_move_other_units_factors(inference_frame):
    """Dropping the currency rows changes nothing else.

    The corpus-level form of this assertion is "the factor map loses exactly
    the ``value`` key"; this is its data-free twin.  It is the reason the
    filter sits before the ``pkg`` baseline rather than only in the
    ``v_infer`` step -- a currency row with a non-null ``Quantity_kg`` would
    otherwise enter ``pkg`` via the ``.where(..., Quantity_kg)`` fill.
    """
    u = inference_frame.index.get_level_values('u').astype(str).str.lower()
    without = inference_frame[~u.isin(_CURRENCY_DENOMINATED_UNITS)]
    assert _get_kg_factors(inference_frame) == _get_kg_factors(without)


# ---------------------------------------------------------------------------
# _apply_kg_conversion and the derived tables
# ---------------------------------------------------------------------------

def test_apply_kg_conversion_leaves_quantity_kg_nan(inference_frame):
    v = _apply_kg_conversion(inference_frame, _get_kg_factors(inference_frame))
    u = v.index.get_level_values('u').astype(str).str.lower()
    is_currency = u.isin(_CURRENCY_DENOMINATED_UNITS)
    assert v.loc[np.asarray(is_currency), 'Quantity_kg'].isna().all()
    # positive control: the physical non-metric label IS converted
    assert v.loc[np.asarray(u == 'heap'), 'Quantity_kg'].notna().all()


def test_food_quantities_kgs_carries_currency_rows_natively(canonical_frame):
    """Carry rule (the docstring's own promise) now actually holds."""
    out = food_quantities_from_acquired(canonical_frame, units='kgs')
    u = out.index.get_level_values('u').astype(str)
    assert (u == 'Value').any()
    # 6 rows, Quantity 7..12, summed to one (t, i, j, u, s) cell
    val = out[u == 'Value']['Quantity'].sum()
    assert val == pytest.approx(sum(7.0 + k for k in range(6)))
    # and the physical control IS relabelled kg
    assert (u == 'kg').any()


def test_food_prices_kgvalue_drops_currency_rows(canonical_frame):
    """``kgvalue`` has no defined answer for a currency row, so it drops it.

    ``food_prices_from_acquired`` ends in ``_drop_unpriceable``; the row does
    not come back as NaN, it ceases to exist.  Before the fix these rows
    survived carrying the constant ``1 / inferred_factor``.
    """
    out = food_prices_from_acquired(canonical_frame, units='kgvalue')
    u = out.index.get_level_values('u').astype(str)
    assert not (u == 'Value').any()
    # Positive control, and the sharper half of it: the inferred factor
    # yields a PHYSICALLY SENSIBLE price.  Heap costs 15 for 3 kg, the kg
    # anchor 10 for 2 kg -- both 5 per kg, which is exactly the premise the
    # inference rests on.  ``Value`` produced a constant instead, and that
    # is the difference this whole change is about.
    assert out.loc[u == 'Heap', 'Price'].unique() == pytest.approx([5.0])
    assert out.loc[u == 'kg', 'Price'].unique() == pytest.approx([5.0])


def test_food_prices_unitvalue_unaffected(canonical_frame):
    """``unitvalue`` never used a kg factor, so it is untouched (delta 0)."""
    out = food_prices_from_acquired(canonical_frame, units='unitvalue')
    u = out.index.get_level_values('u').astype(str)
    assert (u == 'Value').any()
    # Expenditure / Quantity for a currency row is 1 -- cedi per cedi.
    assert out[u == 'Value']['Price'].unique() == pytest.approx([1.0])


def _unpriceable_frame(labels):
    """(t, i, j, u, s) frame with an all-NaN Price, one row per ``u`` label."""
    idx = pd.MultiIndex.from_tuples(
        [('2020', 'H1', 'maize', u, 'purchased') for u in labels],
        names=['t', 'i', 'j', 'u', 's'])
    return pd.DataFrame({'Price': [np.nan] * len(labels)}, index=idx)


def test_drop_unpriceable_is_silent_when_the_whole_loss_is_by_design():
    """An all-currency loss must NOT fire the threshold (GH #770 review).

    This is the concentration defect the review measured on GhanaLSS
    ``kgvalue``: 3,492,915 of 3,493,339 lost rows were by-design currency
    and 424 were everything else, so the warning read 79.84% forever and
    could no longer report a NEW defect in its own cell.  With currency in
    its own bucket the residual loss here is 0 of 0 and nothing is emitted
    -- but the rows are still COUNTED, so the drop is not invisible.
    """
    from lsms_library.transformations import (
        UnpriceableRowsWarning, _drop_unpriceable,
    )
    v = _unpriceable_frame(['Value'] * 4)
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter('always')
        out = _drop_unpriceable(v, 'kgvalue', np.array([True] * 4))
    assert not [w for w in rec
                if issubclass(w.category, UnpriceableRowsWarning)]
    tally = out.attrs['price_rows_dropped']
    assert tally['currency'] == 4          # counted, not hidden
    assert tally['expected'] == 0          # excluded from the denominator
    assert tally['expected_gross'] == 4    # ... and the raw figure kept
    assert tally['dropped_expected'] == 0
    assert tally['lost_fraction'] == 0.0


def test_drop_unpriceable_reports_the_residual_loss_beside_the_bucket():
    """A REAL loss still fires, and the message separates the two causes.

    The discriminating case: 1 genuinely-broken row among 4 currency rows.
    Pre-bucket this was 5/5 = 100% with one undifferentiated explanation;
    post-bucket it is 1/1 with the 4 named separately as by-design.
    """
    from lsms_library.transformations import (
        UnpriceableRowsWarning, _drop_unpriceable,
    )
    v = _unpriceable_frame(['Value'] * 4 + ['Heap'])
    with pytest.warns(UnpriceableRowsWarning) as rec:
        _drop_unpriceable(v, 'kgvalue', np.array([True] * 5))
    msg = str(rec[0].message)
    assert 'dropped 1 of 1' in msg          # the residual, not 5 of 5
    assert 'A further 4 row(s)' in msg      # the bucket, reported
    assert 'CURRENCY-DENOMINATED' in msg
    assert '#770' in msg
    # the inf explanation must stay, scoped to inf
    assert '0-as-missing' in msg


def test_drop_unpriceable_omits_the_currency_sentence_when_none_present():
    """NIT 3: don't explain u='Value' on a drop that is entirely about litres.

    Observed in ``test_volume_as_mass_kwarg.py`` -- the widened text lectured
    about currency on a pure litre drop.  The sentence is now gated on the
    bucket being non-empty.
    """
    from lsms_library.transformations import (
        UnpriceableRowsWarning, _drop_unpriceable,
    )
    v = _unpriceable_frame(['litre'] * 3)
    with pytest.warns(UnpriceableRowsWarning) as rec:
        _drop_unpriceable(v, 'kgvalue', np.array([True] * 3))
    msg = str(rec[0].message)
    assert 'dropped 3 of 3' in msg
    assert 'CURRENCY-DENOMINATED' not in msg
    assert 'A further' not in msg
    assert '0-as-missing' in msg            # the inf explanation still there


def test_case_variant_labels_with_different_factors_warn():
    """A silent 4x ambiguity becomes a reported one (GH #770 review, A2).

    ``conversion_to_kgs`` groups on the RAW label, so ``Bowl``/``bowl`` are
    two units; ``_get_kg_factors`` lower-cases and keeps whichever it merges
    first.  Deterministic, but arbitrary -- and measured corpus-wide it is
    11 live groups in 3 countries with factors differing up to 4.2x.
    """
    from lsms_library.transformations import UnitLabelCollisionWarning
    rows = []
    for m in ['A', 'B']:
        for k in range(6):
            rows.append(('2020', m, f'h{k}', 'maize', 'kg', 2.0, 10.0))
            # 'Bowl' trades at 3 kg-equivalents, 'bowl' at 1 -- same lowercase
            rows.append(('2020', m, f'h{k}', 'maize', 'Bowl', 1.0, 15.0))
            rows.append(('2020', m, f'h{k}', 'maize', 'bowl', 1.0, 5.0))
    df = pd.DataFrame(rows, columns=['t', 'm', 'i', 'j', 'u',
                                     'Quantity', 'Expenditure'])
    df = df.set_index(['t', 'm', 'i', 'j', 'u'])
    with pytest.warns(UnitLabelCollisionWarning) as rec:
        factors = _get_kg_factors(df)
    msg = str(rec[0].message)
    assert "'bowl'" in msg and 'CASE' in msg
    assert 'using' in msg                     # names the winner
    assert factors['bowl'] in (3.0, 1.0)      # one of them, deterministically


def test_no_collision_warning_for_inert_known_metric_keys():
    """``kg``/``litre`` disagreements are seeded by KNOWN_METRIC -> not noise.

    Five of the corpus's 16 raw collision groups are exactly this; reporting
    them would bury the 11 that actually decide a served factor.
    """
    from lsms_library.transformations import UnitLabelCollisionWarning
    rows = []
    for m in ['A', 'B']:
        for k in range(6):
            rows.append(('2020', m, f'h{k}', 'maize', 'kg', 2.0, 10.0))
            rows.append(('2020', m, f'h{k}', 'maize', 'Kg', 1.0, 20.0))
    df = pd.DataFrame(rows, columns=['t', 'm', 'i', 'j', 'u',
                                     'Quantity', 'Expenditure'])
    df = df.set_index(['t', 'm', 'i', 'j', 'u'])
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter('always')
        factors = _get_kg_factors(df)
    assert not [w for w in rec
                if issubclass(w.category, UnitLabelCollisionWarning)]
    assert factors['kg'] == 1                 # KNOWN_METRIC still wins


def test_drop_unpriceable_unit_modes_keep_currency_in_the_denominator():
    """``unitvalue`` CAN price a currency row, so nothing is excluded there.

    The exclusion is mode-conditional on purpose: it is licensed by "there is
    no kg factor", which is true only of the ``kg*`` modes.
    """
    from lsms_library.transformations import _drop_unpriceable
    v = _unpriceable_frame(['Value'] * 4)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        out = _drop_unpriceable(v, 'unitvalue', np.array([True] * 4))
    tally = out.attrs['price_rows_dropped']
    assert tally['currency'] == 0
    assert tally['expected'] == 4
    assert tally['dropped_expected'] == 4
    assert tally['lost_fraction'] == 1.0


# ---------------------------------------------------------------------------
# Data-gated: the measured corpus numbers (GH #770 plan, 2026-09-05)
# ---------------------------------------------------------------------------

#: Measured on `development` @ 30c06c56 with Step 1 applied.
GHANALSS_FOOD_PRICES_ROWS = 407_478
GHANALSS_FOOD_PRICES_WAVES = ['2016-17']
PANAMA_KGPRICE_ROWS = 483_661


def _country(name):
    try:
        import lsms_library as ll
        c = ll.Country(name)
        fa = c.food_acquired()
    except Exception as e:                    # pragma: no cover - no microdata
        pytest.skip(f'{name} food_acquired not buildable here: {e}')
    if fa is None or len(fa) == 0:            # pragma: no cover
        pytest.skip(f'{name} food_acquired is empty here')
    return c, fa


@pytest.mark.slow
def test_ghanalss_factor_map_loses_exactly_the_currency_key():
    """The provable control: ONE key moves, corpus-wide.

    "2016-17 is unchanged" would be the intuitive control and it is FALSE --
    that wave shrinks 7.13% because it too carries 31,295 grouped ``Value``
    rows.  What IS provable is that no non-currency key of the factor map
    moves.
    """
    c, fa = _country('GhanaLSS')
    u = fa.index.get_level_values('u').astype(str).str.lower()
    assert u.isin(_CURRENCY_DENOMINATED_UNITS).any(), 'no Value rows to test'
    got = _get_kg_factors(fa)
    assert not (set(got) & _CURRENCY_DENOMINATED_UNITS)
    without = fa[~u.isin(_CURRENCY_DENOMINATED_UNITS)]
    assert got == _get_kg_factors(without)


@pytest.mark.slow
def test_ghanalss_food_prices_is_one_wave_and_not_constant():
    """Six of seven waves return EMPTY, and that is the intended outcome.

    EL's ruling, recorded: "empty food prices is better than a made up
    constant."  Before this fix the six empty waves returned 1,067,978 rows
    all carrying the single value 2.035038.
    """
    c, _ = _country('GhanaLSS')
    p = c.food_prices()
    assert len(p) == GHANALSS_FOOD_PRICES_ROWS
    waves = sorted(set(map(str, p.index.get_level_values('t'))))
    assert waves == GHANALSS_FOOD_PRICES_WAVES
    # the defect signature was a within-wave std of exactly zero
    assert p.groupby('t')['Price'].std().min() > 0


@pytest.mark.slow
def test_ghanalss_unitvalue_and_units_modes_are_unaffected():
    """Only the kg-denominated modes touch the factor map."""
    c, _ = _country('GhanaLSS')
    assert len(c.food_prices(units='unitvalue')) == 1_505_752
    assert len(c.food_prices(units='unitprice')) == 187_699
    assert len(c.food_quantities(units='units')) == 1_683_305


@pytest.mark.slow
def test_panama_kgprice_loses_its_currency_rows():
    """``kgprice`` calls ``_get_kg_factors`` too, so Panama moves as well.

    GhanaLSS is untouched in this mode only because its ``Value`` rows carry
    a reported ``Price`` 0.0% of the time; Panama's carry one 97.4% of the
    time, so 17,366 rows go.
    """
    c, _ = _country('Panama')
    assert len(c.food_prices(units='kgprice')) == PANAMA_KGPRICE_ROWS
    # the *value modes are untouched in Panama: its Value rows have no kg
    # anchor effect on any other unit
    assert len(c.food_prices(units='unitvalue')) == 501_027
