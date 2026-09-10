"""GH #850 -- the food-side kg inference: item axis, per-unit price, provenance.

Every test here states a PROPERTY on a synthetic frame rather than pinning a
corpus number, so it survives a cache re-warm and says what broke.  The corpus
numbers live in ``slurm_logs/gh850_design/DESIGN.org`` and in the movement
tables of the PR.

The three defects under test, and the one-line statement of each:

(a) *No item axis.*  ``conversion_to_kgs`` returned ONE factor per unit label
    for a whole country, so a bunch of bananas and a bunch of groundnuts got
    one number.  Malawi's ``Piece`` is shared by 133 food items.
(b) *Step 2 medianed ``Expenditure``*, not ``Expenditure / Quantity``, while
    the docstring described the result as a per-unit price -- so the factor
    was kilograms per transaction ROW.  Asked what a kilogram weighs, the old
    chain answered between 0.87 and 2.0.
(c) *The metric vocabulary.*  ``Millilitre``, ``Grams``, ``Litres`` and every
    plural spelling of a self-describing label (``Sack (50 kgs)``) fell
    through to the inference and were served an invented weight.
"""
import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import (
    FOOD_KG_FACTOR_LAYERS,
    FOOD_KG_MIN_BASELINE,
    FOOD_KG_MIN_BASELINE_TIGHT,
    FOOD_KG_MIN_REPORTS,
    FOOD_KG_TIGHT_TOLERANCE,
    FOOD_KG_BASELINE_MAX_SPREAD,
    FOOD_KG_WAVE_SPREAD_REPORT,
    KNOWN_METRIC,
    SURVEY_MEDIAN_MIN_REPORTS,
    _get_kg_factors,
    conversion_to_kgs,
    food_kg_factors,
    food_prices_from_acquired,
    food_quantities_from_acquired,
)

IDX = ['t', 'i', 'j', 'u']


def frame(rows, **cols):
    """``(t, i, j, u)``-indexed frame from ``(t, i, j, u, Quantity,
    Expenditure)`` tuples."""
    idx = pd.MultiIndex.from_tuples([r[:4] for r in rows], names=IDX)
    out = pd.DataFrame({'Quantity': [r[4] for r in rows],
                        'Expenditure': [r[5] for r in rows]}, index=idx)
    for k, v in cols.items():
        out[k] = v
    return out


def kg_rows(item, n, *, price_per_kg=100.0, qty=3.0, t='2020', start=0):
    """*n* households buying *item* by the kilogram at a known price."""
    return [(t, f'h{k}', item, 'kg', qty, qty * price_per_kg)
            for k in range(start, start + n)]


# ---------------------------------------------------------------------------
# (b) the kilogram weighs a kilogram
# ---------------------------------------------------------------------------

def test_the_kilogram_weighs_a_kilogram():
    """The one factor whose answer is known a priori.

    The inference mints a factor for ``u='kg'`` as readily as for anything
    else (``_get_kg_factors`` then discards it, because ``KNOWN_METRIC``
    seeded the key first), and the right answer is 1.0 by definition.  Before
    GH #850 this returned ``median(Quantity)`` -- 3.0 on this frame.
    """
    df = frame(kg_rows('rice', 20, qty=3.0))
    assert conversion_to_kgs(df, index=['t', 'i'])['kg'] == pytest.approx(1.0)
    assert conversion_to_kgs(df, index=['t', 'i'],
                             item_col='j')[('rice', 'kg')] == pytest.approx(1.0)


def test_scaling_every_quantity_leaves_the_factors_alone():
    """A quantity of two is not a kilogram of two.

    Double every ``Quantity`` and every ``Expenditure`` with it -- the same
    transactions in bigger lots at the same prices -- and the inferred
    kilograms per unit must not move.  Before GH #850 they doubled.
    """
    rows = []
    for k in range(20):
        rows += [('2020', f'h{k}', 'A', 'kg', 2.0, 200.0),
                 ('2020', f'h{k}', 'A', 'bunch', 1.0, 300.0)]
    one = conversion_to_kgs(frame(rows), index=['t', 'i'], item_col='j')
    doubled = frame([(t, i, j, u, q * 2, e * 2) for t, i, j, u, q, e in rows])
    two = conversion_to_kgs(doubled, index=['t', 'i'], item_col='j')
    assert one == pytest.approx(two)
    assert one[('A', 'bunch')] == pytest.approx(3.0)


# ---------------------------------------------------------------------------
# (a) the item axis
# ---------------------------------------------------------------------------

def test_two_items_one_unit_get_two_factors():
    """A's bunch is 10x B's bunch; the fix must say so, and the u-pooled
    fallback must land between them rather than on either."""
    rows = []
    for k in range(20):
        rows += [('2020', f'h{k}', 'A', 'kg', 2.0, 200.0),
                 ('2020', f'h{k}', 'A', 'bunch', 1.0, 1000.0),
                 ('2020', f'h{k}', 'B', 'kg', 2.0, 200.0),
                 ('2020', f'h{k}', 'B', 'bunch', 1.0, 100.0)]
    df = frame(rows)
    per_ju = conversion_to_kgs(df, index=['t', 'i'], item_col='j')
    assert per_ju[('A', 'bunch')] == pytest.approx(10.0)
    assert per_ju[('B', 'bunch')] == pytest.approx(1.0)
    assert per_ju[('A', 'bunch')] / per_ju[('B', 'bunch')] == pytest.approx(10.0)

    pooled = conversion_to_kgs(df, index=['t', 'i'])['bunch']
    assert 1.0 < pooled < 10.0

    # ... and the per-row ladder serves each item its own factor.
    f = food_kg_factors(df)
    got = f[f.index.get_level_values('u') == 'bunch']
    a = got[got.index.get_level_values('j') == 'A']['kg_per_unit']
    b = got[got.index.get_level_values('j') == 'B']['kg_per_unit']
    assert set(a.round(6)) == {10.0}
    assert set(b.round(6)) == {1.0}


def test_item_arm_default_is_off_and_keeps_the_dict_return():
    """``_get_kg_factors`` is SHARED with the crop side (``_kg_factor_series``
    -> ``harvest_kg``), so its return TYPE is load-bearing."""
    df = frame(kg_rows('rice', 20) + [('2020', f'h{k}', 'rice', 'tin', 1.0, 50.0)
                                      for k in range(20)])
    factors = _get_kg_factors(df)
    assert isinstance(factors, dict)
    assert all(isinstance(k, str) for k in factors)
    assert all(isinstance(v, float) or isinstance(v, int)
               for v in factors.values())
    # and the free function's default arm is still keyed on the label alone
    assert all(isinstance(k, str)
               for k in conversion_to_kgs(df, index=['t', 'i']))


# ---------------------------------------------------------------------------
# the floors, and the dispersion-gated exception (D4 / D5)
# ---------------------------------------------------------------------------

def test_min_reports_floor_falls_back_to_the_unit_rung_and_says_so():
    """A ``(j, u)`` cell with ``min_reports - 1`` supporting rows must be
    served by the ``unit`` rung, and the provenance must name it."""
    thin = FOOD_KG_MIN_REPORTS - 1
    rows = kg_rows('A', 20) + kg_rows('B', 20)
    # B has plenty of 'tin' rows; A has too few.
    rows += [('2020', f'h{k}', 'A', 'tin', 1.0, 100.0) for k in range(thin)]
    rows += [('2020', f'h{k}', 'B', 'tin', 1.0, 100.0) for k in range(20)]
    df = frame(rows)
    per_ju = conversion_to_kgs(df, index=['t', 'i'], item_col='j')
    assert ('B', 'tin') in per_ju
    assert ('A', 'tin') not in per_ju

    f = food_kg_factors(df)
    tins = f[f.index.get_level_values('u') == 'tin']
    src = tins.groupby(tins.index.get_level_values('j'))['KgFactorSource']
    assert set(src.get_group('A')) == {'unit'}
    assert set(src.get_group('B')) == {'item_unit'}


def test_min_baseline_floor_refuses_to_mint_a_factor():
    """A ``(t, j)`` baseline with fewer than ``min_baseline`` kg-known rows --
    and disagreeing ones, so the tight exception does not rescue it -- must
    produce no ``(j, u)`` factor at all.

    This is the floor that binds: without it one household's single kilogram
    purchase sets an item's price nationally.
    """
    thin = FOOD_KG_MIN_BASELINE - 1
    rows = [('2020', f'h{k}', 'A', 'kg', 1.0, 100.0 * (k + 1))
            for k in range(thin)]              # spread = thin-fold, not tight
    rows += [('2020', f'h{k}', 'A', 'tin', 1.0, 100.0) for k in range(20)]
    per_ju = conversion_to_kgs(frame(rows), index=['t', 'i'], item_col='j')
    assert ('A', 'tin') not in per_ju


def test_tight_exception_admits_agreeing_reports_and_refuses_disagreeing():
    """D5, in two frames that differ only in how much the baseline reports
    disagree.

    Three kilogram reports are below :data:`FOOD_KG_MIN_BASELINE`.  When they
    agree -- max/min within :data:`FOOD_KG_TIGHT_TOLERANCE` -- the baseline is
    accepted and the factor is minted, tagged ``item_unit_tight`` so a
    consumer can drop it without dropping the rung.  When they disagree it is
    not, and the row falls to the ``unit`` rung.
    """
    n = FOOD_KG_MIN_BASELINE_TIGHT
    assert n < FOOD_KG_MIN_BASELINE

    def build(prices):
        rows = [('2020', f'h{k}', 'A', 'kg', 1.0, p) for k, p in enumerate(prices)]
        rows += [('2020', f'h{k}', 'A', 'tin', 1.0, 200.0) for k in range(20)]
        # a second item with a fat baseline, so the u rung exists to fall to
        rows += kg_rows('B', 20, start=100)
        rows += [('2020', f'h{k}', 'B', 'tin', 1.0, 100.0) for k in range(100, 120)]
        return frame(rows)

    tol = FOOD_KG_TIGHT_TOLERANCE
    agree = [100.0, 100.0 * (1 + (tol - 1) / 2), 100.0]      # inside tolerance
    differ = [100.0, 100.0 * (tol + 1.0), 100.0]             # outside it

    per_tight = conversion_to_kgs(build(agree), index=['t', 'i'], item_col='j')
    assert ('A', 'tin') in per_tight

    per_loose = conversion_to_kgs(build(differ), index=['t', 'i'], item_col='j')
    assert ('A', 'tin') not in per_loose

    f = food_kg_factors(build(agree))
    a_tin = f[(f.index.get_level_values('j') == 'A')
              & (f.index.get_level_values('u') == 'tin')]
    assert set(a_tin['KgFactorSource']) == {'item_unit_tight'}

    g = food_kg_factors(build(differ))
    a_tin = g[(g.index.get_level_values('j') == 'A')
              & (g.index.get_level_values('u') == 'tin')]
    assert set(a_tin['KgFactorSource']) == {'unit'}


def test_food_floor_matches_the_crop_side():
    """D4: the step-2 floor is the food twin of the crop side's
    ``survey_median`` threshold and is set equal to it on purpose."""
    assert FOOD_KG_MIN_REPORTS == SURVEY_MEDIAN_MIN_REPORTS


# ---------------------------------------------------------------------------
# grain: no row may be deleted by a groupby
# ---------------------------------------------------------------------------

def test_na_item_key_survives_and_reaches_the_unit_rung():
    """CLAUDE.md, "Grain Collapse" 3b: ``groupby`` defaults to
    ``dropna=True`` and DELETES an NA-keyed row.  A row whose item is NA must
    come back with the rest of the frame and be served by the ``unit`` rung,
    not served a factor pooled over "no item" and not vanish."""
    rows = kg_rows('A', 20) + [('2020', f'h{k}', 'A', 'tin', 1.0, 100.0)
                               for k in range(20)]
    rows += [('2020', 'hx', None, 'tin', 1.0, 100.0)]
    df = frame(rows)
    f = food_kg_factors(df)
    assert len(f) == len(df)
    na = f[pd.isna(f.index.get_level_values('j'))]
    assert len(na) == 1
    assert set(na['KgFactorSource']) == {'unit'}

    q = food_quantities_from_acquired(df, units='kgs')
    assert len(q) > 0


def test_layers_partition_the_frame():
    """``sum(counts.values()) == len(df)``: the invariant
    ``harvest_kg_factors`` already documents, on the food ladder."""
    rows = kg_rows('A', 20) + kg_rows('B', 20)
    rows += [('2020', f'h{k}', 'A', 'tin', 1.0, 100.0) for k in range(20)]
    rows += [('2020', f'h{k}', 'B', 'gourd', 1.0, 100.0) for k in range(2)]
    rows += [('2020', f'h{k}', 'A', '500 g packet', 1.0, 50.0) for k in range(3)]
    df = frame(rows)
    f = food_kg_factors(df)
    counts = f.attrs['kg_factor_sources']
    assert set(counts) == set(FOOD_KG_FACTOR_LAYERS)
    assert sum(counts.values()) == len(df)
    assert set(f['KgFactorSource']) <= set(FOOD_KG_FACTOR_LAYERS)
    # and every layer's rows really do come from that layer
    for layer, col in (('metric', 'kg_metric'), ('item_unit', 'kg_item_unit'),
                       ('unit', 'kg_unit')):
        sub = f[f['KgFactorSource'] == layer]
        assert sub[col].notna().all(), layer


def test_survey_kg_outranks_every_inference():
    """A survey-supplied per-row ``Quantity_kg`` (GH #378) beats the ladder,
    and ``kg_per_unit`` reports the factor that survey kilogram IMPLIES -- so
    ``Quantity * kg_per_unit`` reproduces the delivered kilograms on every
    row, not on all but those."""
    rows = kg_rows('A', 20) + [('2020', f'h{k}', 'A', 'tin', 2.0, 100.0)
                               for k in range(20)]
    df = frame(rows)
    df['Quantity_kg'] = np.nan
    df.iloc[-1, df.columns.get_loc('Quantity_kg')] = 7.0
    f = food_kg_factors(df)
    assert f['KgFactorSource'].iloc[-1] == 'survey_kg'
    assert f['kg_per_unit'].iloc[-1] == pytest.approx(3.5)   # 7 kg / 2 units
    assert f.attrs['kg_factor_sources']['survey_kg'] == 1


# ---------------------------------------------------------------------------
# (c) the metric vocabulary
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("label, expected", [
    ('Millilitre', 1/1000), ('Grams', 1/1000), ('Litres', 1.0),
    ('Gramme', 1/1000), ('Mili Liter', 1/1000),
    ('Sack (100 kgs)', 100.0), ('Tin (Debe) (20 lts)', 20.0),
    ('Cup/Mug(0.5lt)', 0.5),
])
def test_metric_spellings_are_seeded_never_inferred(label, expected):
    """Defect (c).  Each label states its own kilograms; the library must
    READ them.  Before GH #850 every one of these fell through to the
    price-ratio inference -- ``Millilitre`` came back at 0.743 kg in Malawi.
    """
    rows = kg_rows('A', 20)
    rows += [('2020', f'h{k}', 'A', label, 1.0, 100.0) for k in range(20)]
    df = frame(rows)
    assert _get_kg_factors(df)[label.lower()] == pytest.approx(expected)
    # and it must not be a key of the INFERENCE: nothing about it is inferred
    inferred = conversion_to_kgs(df, index=['t', 'i'], item_col='j')
    served = food_kg_factors(df)
    lab = served[served.index.get_level_values('u') == label]
    assert set(lab['KgFactorSource']) == {'metric'}
    assert lab['kg_per_unit'].unique() == pytest.approx([expected])
    del inferred          # the inference may hold a key; the ladder ignores it


# ---------------------------------------------------------------------------
# the two kg-denominated price modes agree
# ---------------------------------------------------------------------------

def test_kgprice_and_kgvalue_use_the_same_factor():
    """``units='kgprice'`` used to map ``u -> factor`` through its own dict,
    so with an item axis the two branches could disagree on a row.  With a
    reported Price equal to Expenditure/Quantity they must now agree
    row for row."""
    rows = []
    for k in range(20):
        rows += [('2020', f'h{k}', 'A', 'kg', 2.0, 200.0),
                 ('2020', f'h{k}', 'A', 'bunch', 1.0, 1000.0),
                 ('2020', f'h{k}', 'B', 'kg', 2.0, 200.0),
                 ('2020', f'h{k}', 'B', 'bunch', 1.0, 100.0)]
    df = frame(rows)
    df['Price'] = df['Expenditure'] / df['Quantity']
    value = food_prices_from_acquired(df, units='kgvalue')
    price = food_prices_from_acquired(df, units='kgprice')
    common = value.index.intersection(price.index)
    assert len(common) > 0
    assert value.loc[common, 'Price'].to_numpy() == pytest.approx(
        price.loc[common, 'Price'].to_numpy())
    # and the per-item factors really did differ, or this proves nothing
    f = food_kg_factors(df)
    bunches = f[f.index.get_level_values('u') == 'bunch']['kg_per_unit']
    assert bunches.nunique() == 2


def test_derived_tables_carry_the_provenance_counts():
    """``attrs['kg_factor_sources']`` rides on the derived tables, stamped
    after the final aggregation (which builds a fresh frame and would
    otherwise drop it)."""
    rows = kg_rows('A', 20) + [('2020', f'h{k}', 'A', 'tin', 1.0, 100.0)
                               for k in range(20)]
    df = frame(rows)
    q = food_quantities_from_acquired(df, units='kgs')
    p = food_prices_from_acquired(df, units='kgvalue')
    assert sum(q.attrs['kg_factor_sources'].values()) == len(df)
    assert q.attrs['kg_factor_sources'] == p.attrs['kg_factor_sources']
    # a mode that consults no ladder reports none, rather than reporting zeros
    assert 'kg_factor_sources' not in food_prices_from_acquired(
        df, units='unitvalue').attrs


def test_item_axis_degrades_when_the_frame_has_no_item_level():
    """No ``j`` -> the item rungs serve nothing and the layers still
    partition."""
    rows = kg_rows('A', 20) + [('2020', f'h{k}', 'A', 'tin', 1.0, 100.0)
                               for k in range(20)]
    df = frame(rows).droplevel('j')
    f = food_kg_factors(df)
    counts = f.attrs['kg_factor_sources']
    assert sum(counts.values()) == len(df)
    assert counts['item_unit'] == 0 and counts['item_unit_tight'] == 0
    assert counts['unit'] > 0


def test_currency_denominated_labels_get_no_factor():
    """GH #770 survives the rewrite: ``u='Value'`` is dropped from the whole
    inference and is served by the ``none`` layer."""
    rows = kg_rows('A', 20)
    rows += [('2020', f'h{k}', 'A', 'Value', 100.0, 100.0) for k in range(20)]
    f = food_kg_factors(frame(rows))
    val = f[f.index.get_level_values('u') == 'Value']
    assert set(val['KgFactorSource']) == {'none'}
    assert val['kg_per_unit'].isna().all()


# ---------------------------------------------------------------------------
# Uganda's hand-curated answer key (data-gated)
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
def test_uganda_curated_json_agrees_on_the_labels_the_parser_now_reads():
    """``Uganda/_/conversion_to_kgs.json`` is a hand-curated kg-per-unit table
    for exactly the container labels defect (c) mishandled.

    The framework must NOT read it at runtime -- a stored inferred factor is
    the anti-pattern this whole issue is about, and the file is dead to the
    API (``uganda.food_acquired_to_canonical`` drops the ``Kgs`` column it
    feeds).  But as an ANSWER KEY it is the only external referent Uganda's
    unit vocabulary has, so agreement on the labels the parser now reads is
    worth asserting.

    Measured 2026-09-09: of 118 curated labels, 102 are parseable and 96
    agree exactly.  The six that do not are RANGE labels, and they are named
    in :data:`_UGANDA_RANGE_LABELS` rather than smoothed away -- see the test
    below.
    """
    import json

    from lsms_library.paths import countries_root
    from lsms_library.transformations import _parse_explicit_metric

    path = countries_root() / 'Uganda' / '_' / 'conversion_to_kgs.json'
    if not path.exists():
        pytest.skip('Uganda conversion_to_kgs.json not present')
    curated = json.loads(path.read_text())
    checked, disagree = 0, []
    for label, kg in curated.items():
        if label in _UGANDA_RANGE_LABELS:
            continue
        parsed = _parse_explicit_metric(str(label))
        if parsed is None:
            continue
        checked += 1
        if parsed != pytest.approx(float(kg), rel=0.01):
            disagree.append((label, kg, parsed))
    assert checked >= 90, f'only {checked} labels were parseable'
    assert not disagree, disagree


#: Uganda container labels that state a RANGE ("1 - 2 kg", "up to 1kg",
#: "Above 10 lts").  The curator took a midpoint; ``_parse_explicit_metric``
#: takes the first number its pattern matches, so the two disagree BY
#: CONSTRUCTION and neither is wrong.  Named here rather than smoothed into a
#: tolerance, because a tolerance wide enough to cover "Above 10 lts" -> 10 vs
#: 15 would also swallow a real error.
#:
#: Reading a range is a defensible future change (midpoint, or an explicit
#: interval); it would move Uganda's kilograms, so it is not folded into
#: GH #850.  The test below pins TODAY's behaviour so that change goes red
#: here first.
_UGANDA_RANGE_LABELS = frozenset({
    'Fish Cut piece (up to 1kg)', 'Fish Cut piece (1 - 2 kg)',
    'Fish Cut piece(Above 2  kg)', 'Gourd (1-5lts)', 'Gourd (5-10lts)',
    'Gourd (Above 10 lts)',
})


@pytest.mark.parametrize("label, endpoint, curated", [
    ('Fish Cut piece (up to 1kg)', 1.0, 0.75),
    ('Fish Cut piece (1 - 2 kg)', 2.0, 1.5),
    ('Fish Cut piece(Above 2  kg)', 2.0, 3.0),
    ('Gourd (1-5lts)', 5.0, 3.5),
    ('Gourd (5-10lts)', 10.0, 7.5),
    ('Gourd (Above 10 lts)', 10.0, 15.0),
])
def test_range_labels_are_read_as_one_endpoint_not_a_midpoint(
        label, endpoint, curated):
    """The parser reads a range label as one of its endpoints.  Stated, not
    fixed -- see :data:`_UGANDA_RANGE_LABELS`."""
    from lsms_library.transformations import _parse_explicit_metric
    assert _parse_explicit_metric(label) == pytest.approx(endpoint)
    assert endpoint != pytest.approx(curated)


@pytest.mark.parametrize("spelling", ['kgs', 'grams', 'litres', 'millilitre'])
def test_new_metric_keys_are_in_known_metric(spelling):
    assert spelling in KNOWN_METRIC


# ---------------------------------------------------------------------------
# GH #850 red team (2026-09-10): the per-wave floor, D6 disclosure, and the
# dispersion gate that is prepared but not armed
# ---------------------------------------------------------------------------

def _waves(n_waves, rows_per_wave, *, baseline=10):
    """*n_waves* waves, each with a fat kg baseline and *rows_per_wave* Bunch
    rows.  A Bunch is truly 2 kg in every wave."""
    rows = []
    for w in range(n_waves):
        rows += [(f'T{w}', f'h{k}', 'A', 'kg', 2.0, 200.0)
                 for k in range(baseline)]
        rows += [(f'T{w}', f'z{k}', 'A', 'Bunch', 1.0, 200.0)
                 for k in range(rows_per_wave)]
    return frame(rows)


def test_min_reports_gates_the_wave_not_the_pooled_support():
    """One report per wave in five waves must NOT clear a floor that four
    reports in a single wave does not.

    Until the GH #850 red team measured it, ``min_reports`` screened
    ``support`` summed over waves, so it did -- while the docstring said the
    floor was "the number of step-2 rows behind a ``(t, item, u)`` estimate"
    and the crop side's ``_survey_median_factors`` gates per wave.  The floor
    now means what it says.
    """
    n = FOOD_KG_MIN_REPORTS
    spread = conversion_to_kgs(_waves(n, 1), index=['t', 'i'], item_col='j')
    assert ('A', 'Bunch') not in spread, 'n waves x 1 row must not clear the floor'
    one = conversion_to_kgs(_waves(1, n), index=['t', 'i'], item_col='j')
    assert one[('A', 'Bunch')] == pytest.approx(2.0)
    short = conversion_to_kgs(_waves(1, n - 1), index=['t', 'i'], item_col='j')
    assert ('A', 'Bunch') not in short


def test_detail_frame_discloses_how_many_waves_and_how_far_apart():
    """D6.  The delivered factor is a median ACROSS waves; a consumer is
    entitled to know across how many and whether they agreed.

    The red team's case: a `Tin` whose per-wave truth is 1, 2 and 3 kg is
    served 2.0 in all three waves -- every row in the first is 2x over and
    every row in the third 0.67x under -- and nothing used to say so.
    """
    rows = []
    for wave, truth in (('T1', 1.0), ('T2', 2.0), ('T3', 3.0)):
        rows += [(wave, f'h{k}', 'Maize', 'kg', 2.0, 200.0) for k in range(8)]
        rows += [(wave, f'z{k}', 'Maize', 'Tin', 1.0, 100.0 * truth)
                 for k in range(8)]
    df = frame(rows)
    detail = conversion_to_kgs(df, index=['t', 'i'], item_col='j', _detail=True)
    tin = detail.loc[('Maize', 'Tin')]
    assert tin['kg_per_unit'] == pytest.approx(2.0)
    assert tin['n_waves'] == 3
    assert tin['wave_spread'] == pytest.approx(3.0)

    f = food_kg_factors(df)
    ws = f.attrs['kg_factor_wave_spread']
    assert ws['threshold'] == FOOD_KG_WAVE_SPREAD_REPORT
    assert ws['rows_over_threshold'] == 24        # every Tin row
    assert ws['rows_single_wave'] == 0
    assert ws['rows_served_by_item_rung'] == 24
    # the per-row columns carry the same facts
    tins = f[f.index.get_level_values('u') == 'Tin']
    assert set(tins['kg_item_n_waves']) == {3.0}
    assert set(tins['kg_item_wave_spread'].round(6)) == {3.0}


def test_a_single_wave_factor_is_counted_as_such():
    """A cell estimated in ONE of the waves it serves is the Niger case
    (``(Mil, Tiya)``: 2018-19 has no kg-known millet row at all, so 4,886 rows
    are served a factor measured on a wave they are not in)."""
    rows = []
    for wave in ('T1', 'T2'):
        rows += [(wave, f'z{k}', 'A', 'Bunch', 1.0, 200.0) for k in range(8)]
    rows += [('T1', f'h{k}', 'A', 'kg', 2.0, 200.0) for k in range(10)]
    f = food_kg_factors(frame(rows))
    ws = f.attrs['kg_factor_wave_spread']
    assert ws['rows_single_wave'] == 16          # both waves' Bunch rows
    assert ws['rows_over_threshold'] == 0        # one wave cannot disagree


def test_baseline_max_spread_refuses_an_incoherent_baseline_by_default():
    """A five-report baseline whose reports differ by 125x used to set an
    item's kilograms in silence; at the armed default it is refused.

    ``FOOD_KG_MIN_BASELINE`` is an ABSOLUTE floor and was the only gate on the
    strict rung, so a four-report baseline differing by 26% was refused by the
    tight exception while this one was admitted.  @ligon armed the gate at 10
    on 2026-09-10 after the 3/5/10/30 sweep.
    """
    prices = [100.0, 100.0, 100.0, 100.0, 12500.0]      # max/min = 125
    rows = [('T1', f'h{k}', 'A', 'kg', 1.0, p) for k, p in enumerate(prices)]
    rows += [('T1', f'z{k}', 'A', 'Bunch', 1.0, 200.0) for k in range(10)]
    df = frame(rows)
    assert FOOD_KG_BASELINE_MAX_SPREAD == 10.0
    assert ('A', 'Bunch') not in conversion_to_kgs(df, index=['t', 'i'],
                                                   item_col='j')
    for cand in (3, 5, 10, 30):
        assert ('A', 'Bunch') not in conversion_to_kgs(
            df, index=['t', 'i'], item_col='j', baseline_max_spread=cand), cand
    # None restores the ungated rung -- the pre-decision behaviour
    assert ('A', 'Bunch') in conversion_to_kgs(df, index=['t', 'i'],
                                               item_col='j',
                                               baseline_max_spread=None)


def test_a_coherent_baseline_is_untouched_by_the_gate():
    """The gate must refuse incoherence, not thin evidence: a baseline whose
    reports agree passes at every candidate and at the default."""
    rows = [('T1', f'h{k}', 'A', 'kg', 1.0, 100.0) for k in range(10)]
    rows += [('T1', f'z{k}', 'A', 'Bunch', 1.0, 200.0) for k in range(10)]
    df = frame(rows)
    for spread in (None, 3, 5, 10, 30):
        got = conversion_to_kgs(df, index=['t', 'i'], item_col='j',
                                baseline_max_spread=spread)
        assert got[('A', 'Bunch')] == pytest.approx(2.0), spread


def test_the_gate_uses_p90_over_p10_once_there_are_ten_reports():
    """Below ten reports the extremes ARE the evidence and ``max/min`` reads
    them; at ten or more a robust interdecile spread exists and one wild
    report must not condemn the cell.  Same single outlier, two verdicts."""
    # 19 agreeing reports + 1 wild one: max/min = 125, p90/p10 = 1.0.  (One
    # outlier in exactly ten still moves an interpolated p90 -- 13.4 -- and is
    # refused; the decile only becomes robust once the tail has room.)
    prices = [100.0] * 19 + [12500.0]
    rows = [('T1', f'h{k}', 'A', 'kg', 1.0, p) for k, p in enumerate(prices)]
    rows += [('T1', f'z{k}', 'A', 'Bunch', 1.0, 200.0) for k in range(10)]
    assert ('A', 'Bunch') in conversion_to_kgs(frame(rows), index=['t', 'i'],
                                               item_col='j')
    # the same outlier in a cell of 5 is judged on max/min and refused
    prices = [100.0] * 4 + [12500.0]
    rows = [('T1', f'h{k}', 'A', 'kg', 1.0, p) for k, p in enumerate(prices)]
    rows += [('T1', f'z{k}', 'A', 'Bunch', 1.0, 200.0) for k in range(10)]
    assert ('A', 'Bunch') not in conversion_to_kgs(frame(rows),
                                                   index=['t', 'i'],
                                                   item_col='j')


def test_a_refused_row_is_tagged_counted_and_falls_to_the_unit_rung():
    """A refused row is not dropped: it takes the u-pooled ``unit`` factor --
    the one the library gave every row before GH #850 -- or ``none``, and it
    says so.  The gate's counts are NOT a layer and ride alongside the
    partition, as ``reported_implausible`` does on the crop side."""
    rows = []
    # item A: incoherent baseline (refused); item B: coherent (kept).  Both
    # sell by the Bunch, and B's households also buy A's Bunch so the u rung
    # has something to fall back to.
    for k in range(10):
        rows += [('T1', f'h{k}', 'B', 'kg', 1.0, 100.0),
                 ('T1', f'h{k}', 'B', 'Bunch', 1.0, 200.0),
                 ('T1', f'h{k}', 'A', 'Bunch', 1.0, 200.0)]
    rows += [('T1', f'h{k}', 'A', 'kg', 1.0, p)
             for k, p in enumerate([100.0, 100.0, 100.0, 100.0, 12500.0])]
    df = frame(rows)
    f = food_kg_factors(df)
    gate = f.attrs['kg_factor_baseline_gate']
    assert gate['baseline_max_spread'] == FOOD_KG_BASELINE_MAX_SPREAD
    assert gate['rows_refused'] > 0
    # the invariant a reader will check
    assert gate['rows_refused'] == (gate['refused_to_unit']
                                    + gate['refused_to_none'])
    assert sum(f.attrs['kg_factor_sources'].values()) == len(df)

    a_bunch = f[(f.index.get_level_values('j') == 'A')
                & (f.index.get_level_values('u') == 'Bunch')]
    assert a_bunch['baseline_refused_spread'].all()
    assert set(a_bunch['KgFactorSource']) <= {'unit', 'none'}
    b_bunch = f[(f.index.get_level_values('j') == 'B')
                & (f.index.get_level_values('u') == 'Bunch')]
    assert not b_bunch['baseline_refused_spread'].any()
    assert set(b_bunch['KgFactorSource']) == {'item_unit'}

    off = food_kg_factors(df, baseline_max_spread=None)
    assert off.attrs['kg_factor_baseline_gate']['rows_refused'] == 0
    assert not off['baseline_refused_spread'].any()


def test_the_gate_never_costs_a_row_that_a_higher_rung_serves():
    """A seeded metric label or a survey kilogram outranks the item rung, so
    the gate cost those rows nothing and must not count them."""
    rows = [('T1', f'h{k}', 'A', 'kg', 1.0, p)
            for k, p in enumerate([100.0, 100.0, 100.0, 100.0, 12500.0])]
    rows += [('T1', f'z{k}', 'A', 'Bunch', 1.0, 200.0) for k in range(10)]
    f = food_kg_factors(frame(rows))
    kg_rows = f[f.index.get_level_values('u') == 'kg']
    assert set(kg_rows['KgFactorSource']) == {'metric'}
    assert not kg_rows['baseline_refused_spread'].any()
