"""GH #962: food-unit sentinels, accepted residual codes, and their kg contract.

Two halves, both config-only (no microdata, no cache):

* **Uganda** (GH #967): missing units retain quantity reports and value-only
  acquisitions carry ``u='Value'``.  Exercises the country reader and
  canonicalizer with small raw frames, then the ordinary runtime derivations.
* **The framework contract**, on synthetic ``(t, i, j, u, s)`` frames:

  - every spelling in :data:`transformations._U_SENTINELS` (and a NaN ``u``)
    with enough priced rows to activate the price-ratio inference is served
    ``KgFactorSource == 'none'``, while a real unit on the same priced
    population IS inferred;
  - an **accepted residual code** (a unit no source names, e.g. Uganda's
    ``136``) is NOT a missing unit: it is served an inferred factor --
    the u-pooled ``unit`` rung -- and so contributes to
    ``food_quantities(units='kgs')``.  The units skill and the
    ``tests/test_u_code_leak.py`` header used to say the opposite;
  - ``_U_SENTINELS`` is a CLOSED vocabulary, so an unlisted spelling of "no
    unit" is treated as a unit (pinned, not endorsed), and a static sweep of
    the country tree refuses any literal a module writes into a unit
    column (``fillna``, null ``replace``, ``where``/``mask``, or direct
    assignment) that is neither classified nor recorded, with evidence, in
    :data:`MINTED_UNIT_SPELLINGS`.  Raw survey labels and org/YAML mapping
    tables are outside the sweep.
"""
import ast
import importlib.util
import re
from pathlib import Path
from unittest.mock import mock_open

import numpy as np
import pandas as pd
import pytest

from lsms_library import local_tools
from lsms_library.paths import countries_root
from lsms_library.transformations import (
    FOOD_KG_MIN_BASELINE,
    FOOD_KG_MIN_REPORTS,
    U_UNKNOWN,
    _CURRENCY_DENOMINATED_UNITS,
    _U_SENTINELS,
    food_expenditures_from_acquired,
    food_kg_factors,
    food_prices_from_acquired,
    food_quantities_from_acquired,
)


UGANDA = (Path(__file__).resolve().parents[1] / 'lsms_library' /
          'countries' / 'Uganda' / '_')
COUNT = 'Number of Units (General)'


@pytest.fixture(scope='module')
def uganda():
    spec = importlib.util.spec_from_file_location('uganda_962', UGANDA / 'uganda.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _wide_row(household, unit=U_UNKNOWN, **values):
    row = dict(t='2020', i=household, j='rice', u=unit,
               market=100.0, farmgate=80.0)
    row.update({f'{measure}_{source}': np.nan
                for measure in ('quantity', 'value')
                for source in ('home', 'away', 'own', 'inkind')})
    row.update(values)
    return row


def _wide(rows):
    return pd.DataFrame(rows).set_index(['t', 'i', 'j', 'u']).astype(float)


def _read_raw(uganda, monkeypatch, units, *, labels=None):
    raw = _wide([_wide_row(f'h{k}', quantity_home=2.0, value_home=200.0)
                 for k in range(len(units))]).reset_index()
    raw = raw.drop(columns='t').rename(columns={'i': 'HHID', 'j': 'item', 'u': 'units'})
    raw['item'] = 101
    raw['units'] = units
    monkeypatch.setattr(uganda, 'get_dataframe', lambda *a, **k: raw.copy())
    monkeypatch.setattr(uganda, 'harmonized_food_labels', lambda: {101: 'rice'})
    if labels is not None:
        monkeypatch.setattr(uganda, 'harmonized_unit_labels', lambda: labels)
    monkeypatch.setattr(uganda, 'open', mock_open(read_data='{}'), raising=False)
    monkeypatch.chdir(UGANDA)
    return uganda.food_acquired('synthetic.dta', {col: col for col in raw.columns})


def test_raw_zero_variants_and_blanks_are_unknown(uganda, monkeypatch):
    units = pd.Series([0, 0.0, '0', '0.0', None, np.nan, pd.NA, '', ' \t'],
                      dtype=object)
    out = _read_raw(uganda, monkeypatch, units, labels={0: U_UNKNOWN, 1: 'Kg'})
    assert len(out) == len(units)
    assert set(out.index.get_level_values('u')) == {U_UNKNOWN}
    assert out['quantity_home'].eq(2).all()
    assert out['value_home'].eq(200).all()


@pytest.mark.parametrize('dtype', ['float64', 'Float64', 'Int64', 'category', 'string'])
def test_raw_missing_unit_accepts_nullable_and_categorical_columns(uganda, monkeypatch, dtype):
    out = _read_raw(uganda, monkeypatch, pd.Series([0, None, 1], dtype=dtype),
                    labels={0: U_UNKNOWN, 1: 'Kg'})
    assert out.index.get_level_values('u').tolist() == [U_UNKNOWN, U_UNKNOWN, 'Kg']


def test_unlisted_fractional_codes_warn_and_drop_without_becoming_kg(uganda, monkeypatch):
    with pytest.warns(UserWarning, match='dropping 3 of 4 rows'):
        out = _read_raw(uganda, monkeypatch, pd.Series([1, 1.5, '1.5', 999]),
                        labels={0: U_UNKNOWN, 1: 'Kg'})
    assert len(out) == 1
    assert out.index.tolist() == [('h0', 'rice', 'Kg')]


def test_unexpected_blank_mapping_retains_each_code_identity(uganda, monkeypatch):
    monkeypatch.setattr(local_tools, 'get_categorical_mapping',
                        lambda **kwargs: {136: np.nan, '142090': ' ', 1.5: 'Fractional code'})
    labels = uganda.harmonized_unit_labels()
    assert labels == {136: '136', 142090: '142090', 1.5: 'Fractional code'}


def test_curated_catalog_keeps_physical_sizes_counts_and_residuals(uganda, monkeypatch):
    residuals = [136, 156087, 139093, 142092, 156096, 156090, 181087, 142090,
                 153001, 154001, 160001]
    codes = [0, 1, 85, 125051, 125052, 123060] + residuals
    out = _read_raw(uganda, monkeypatch, pd.Series(codes))
    assert out.index.get_level_values('u').tolist() == [
        U_UNKNOWN, 'Kg', COUNT, 'Packet (1/2 lt / 1/2 kg)',
        'Packet (1/4 lt / 1/4 kg)', 'Fish Cut piece(Above 2  kg)',
        *map(str, residuals),
    ]
    # These are recorded sizes, not prices from which to infer a size.
    canonical = uganda.food_acquired_to_canonical(
        out.assign(t='2020').set_index('t', append=True))
    factors = food_kg_factors(canonical)
    for unit, factor in [('Kg', 1.0), ('Packet (1/2 lt / 1/2 kg)', 0.5),
                         ('Packet (1/4 lt / 1/4 kg)', 0.25)]:
        rows = factors.xs(unit, level='u')
        assert rows['kg_per_unit'].eq(factor).all()
        assert rows['KgFactorSource'].eq('metric').all()


def test_sources_are_classified_separately_and_wide_prices_survive(uganda):
    wide = _wide([_wide_row('h', quantity_home=np.nan, quantity_away=0,
                           value_home=30, value_away=5,
                           quantity_own=2, value_own=10,
                           quantity_inkind=0, value_inkind=7)])
    before = wide.copy(deep=True)
    out = uganda.food_acquired_to_canonical(wide)
    pd.testing.assert_frame_equal(wide, before)
    assert len(out) == 3
    assert out.loc[('2020', 'h', 'rice', 'Value', 'purchased')].tolist()[:2] == [35, 35]
    assert out.loc[('2020', 'h', 'rice', U_UNKNOWN, 'produced')].tolist() == [2, 10, 80]
    assert out.loc[('2020', 'h', 'rice', 'Value', 'inkind')].tolist()[:2] == [7, 7]
    assert out.xs('Value', level='u')['Price'].isna().all()


@pytest.mark.parametrize('home,away,unit,quantity', [
    (np.nan, np.nan, 'Value', 12),
    (0, np.nan, 'Value', 12),
    (0, 0, 'Value', 12),
    (2, -2, U_UNKNOWN, 0),
    (-3, np.nan, U_UNKNOWN, -3),
    (np.nan, -3, U_UNKNOWN, -3),
    (2, np.nan, U_UNKNOWN, 2),
])
def test_original_purchase_quantities_control_value_classification(uganda, home, away, unit, quantity):
    out = uganda.food_acquired_to_canonical(_wide([
        _wide_row('h', quantity_home=home, quantity_away=away, value_home=12)]))
    assert len(out) == 1
    row = out.loc[('2020', 'h', 'rice', unit, 'purchased')]
    assert row['Quantity'] == quantity
    assert row['Expenditure'] == 12
    assert pd.isna(row['Price']) if unit == 'Value' else row['Price'] == 100


@pytest.mark.parametrize('source,suffix', [('produced', 'own'), ('inkind', 'inkind')])
def test_nonpurchased_negative_quantities_remain_unknown(uganda, source, suffix):
    out = uganda.food_acquired_to_canonical(_wide([
        _wide_row('h', **{f'quantity_{suffix}': -2, f'value_{suffix}': 10})]))
    assert len(out) == 1
    assert out.loc[('2020', 'h', 'rice', U_UNKNOWN, source), 'Quantity'] == -2


@pytest.mark.parametrize('unit', ['Kg', COUNT])
def test_known_units_keep_quantities_expenditure_and_reported_prices(uganda, unit):
    wide = _wide([_wide_row('reported', unit, quantity_home=2, value_home=20),
                  _wide_row('missing', unit, value_home=30),
                  _wide_row('zero', unit, quantity_home=0, value_home=40)])
    out = uganda.food_acquired_to_canonical(wide)
    assert len(out) == 3
    assert set(out.index.get_level_values('u')) == {unit}
    expected = wide[['quantity_home', 'value_home', 'market']].rename(columns={
        'quantity_home': 'Quantity', 'value_home': 'Expenditure', 'market': 'Price'})
    pd.testing.assert_frame_equal(out.droplevel('s').sort_index(), expected.sort_index())


def test_duplicate_partition_precedes_aggregation_and_preserves_expenditures(uganda):
    wide = _wide([_wide_row('split', quantity_home=2, value_home=10),
                  _wide_row('split', value_home=7),
                  _wide_row('value', value_home=7),
                  _wide_row('value', quantity_home=0, value_home=3),
                  _wide_row('cancel', quantity_home=-2, value_home=3),
                  _wide_row('cancel', quantity_home=2, value_home=7)])
    out = uganda.food_acquired_to_canonical(wide)
    assert len(out) == 4
    split = out.xs('split', level='i')
    assert set(split.index.get_level_values('u')) == {U_UNKNOWN, 'Value'}
    assert split.xs(U_UNKNOWN, level='u')['Quantity'].iloc[0] == 2
    assert split.xs('Value', level='u')['Quantity'].iloc[0] == 7
    assert out.loc[('2020', 'value', 'rice', 'Value', 'purchased'), 'Quantity'] == 10
    assert out.loc[('2020', 'cancel', 'rice', U_UNKNOWN, 'purchased'), 'Quantity'] == 0
    assert out['Expenditure'].sum() == wide['value_home'].sum()


def test_empty_and_nonpositive_expense_keep_existing_filter(uganda):
    out = uganda.food_acquired_to_canonical(_wide([
        _wide_row('empty'), _wide_row('zero', value_home=0),
        _wide_row('negative', quantity_home=0, value_home=-2),
        _wide_row('quantity-only', quantity_home=4)]))
    assert len(out) == 1
    assert out.loc[('2020', 'quantity-only', 'rice', U_UNKNOWN, 'purchased'), 'Quantity'] == 4


def test_runtime_sentinel_semantics_with_working_inference_control(uganda):
    rows = []
    for k in range(20):
        rows.extend([
            _wide_row(f'h{k}', 'Kg', quantity_home=2, value_home=200),
            _wide_row(f'h{k}', COUNT, quantity_home=2, value_home=600),
            _wide_row(f'h{k}', quantity_home=2, value_home=200),
            _wide_row(f'v{k}', value_home=200, value_own=50, value_inkind=30),
        ])
    out = uganda.food_acquired_to_canonical(_wide(rows))
    factors = food_kg_factors(out)
    for sentinel in (U_UNKNOWN, 'Value'):
        sentinel_factors = factors.xs(sentinel, level='u')
        assert len(sentinel_factors) >= 20
        assert sentinel_factors['kg_per_unit'].isna().all()
        assert sentinel_factors['KgFactorSource'].eq('none').all()
    # A real count unit DOES infer a factor on the same priced population.
    assert factors.xs(COUNT, level='u')['kg_per_unit'].eq(3).all()
    assert factors.xs('Kg', level='u')['kg_per_unit'].eq(1).all()
    quantities = food_quantities_from_acquired(out)
    assert quantities.xs(U_UNKNOWN, level='u')['Quantity'].eq(2).all()
    values = quantities.xs('Value', level='u')
    assert values.xs('purchased', level='s')['Quantity'].eq(200).all()
    assert values.xs('produced', level='s')['Quantity'].eq(50).all()
    assert values.xs('inkind', level='s')['Quantity'].eq(30).all()
    native = food_quantities_from_acquired(out, units='units')
    assert native.xs(COUNT, level='u')['Quantity'].eq(2).all()
    unitvalues = food_prices_from_acquired(out, units='unitvalue')
    assert unitvalues.xs('Value', level='u')['Price'].eq(1).all()
    unitprices = food_prices_from_acquired(out, units='unitprice')
    assert 'Value' not in unitprices.index.get_level_values('u')
    assert unitprices.xs(U_UNKNOWN, level='u')['Price'].eq(100).all()
    for mode in ('kgvalue', 'kgprice'):
        prices = food_prices_from_acquired(out, units=mode)
        assert not prices.empty
        assert not {U_UNKNOWN, 'Value'} & set(prices.index.get_level_values('u'))
    for basis in ('purchased', 'total'):
        expected = out if basis == 'total' else out.xs('purchased', level='s')
        expenditures = food_expenditures_from_acquired(out, basis=basis)
        assert expenditures['Expenditure'].sum() == expected['Expenditure'].sum()


def _categorical_wide(rows):
    wide = _wide(rows).reset_index()
    wide['u'] = pd.Categorical(wide['u'], categories=['Kg', U_UNKNOWN], ordered=True)
    return wide.set_index(['t', 'i', 'j', 'u'])


@pytest.mark.parametrize('unit', ['Kg', U_UNKNOWN])
def test_categorical_units_without_retyping_preserve_dtype_and_prices(uganda, unit):
    wide = _categorical_wide([
        _wide_row('reported', unit, quantity_home=2, value_home=20),
        _wide_row('empty', unit),
    ])
    before = wide.copy(deep=True)
    out = uganda.food_acquired_to_canonical(wide)
    expected = wide.loc[[('2020', 'reported', 'rice', unit)],
                        ['quantity_home', 'value_home', 'market']].rename(columns={
        'quantity_home': 'Quantity', 'value_home': 'Expenditure', 'market': 'Price'})
    expected = expected.assign(s='purchased').set_index('s', append=True)
    pd.testing.assert_frame_equal(out, expected)
    pd.testing.assert_frame_equal(wide, before)


def test_categorical_unknown_can_become_value(uganda):
    wide = _categorical_wide([_wide_row('h', value_home=20)])
    before = wide.copy(deep=True)
    out = uganda.food_acquired_to_canonical(wide)
    assert len(out) == 1
    row = out.loc[('2020', 'h', 'rice', 'Value', 'purchased')]
    assert row['Quantity'] == row['Expenditure'] == 20
    assert pd.isna(row['Price'])
    pd.testing.assert_frame_equal(wide, before)


def test_categorical_mixed_units_and_sources_remain_separate(uganda):
    wide = _categorical_wide([
        _wide_row('mixed', value_home=30, quantity_own=2, value_own=10,
                  quantity_inkind=0, value_inkind=7),
        _wide_row('physical', 'Kg', quantity_home=2, value_home=200,
                  quantity_own=1, value_own=80),
        _wide_row('cancel', quantity_home=2, quantity_away=-2, value_home=10),
    ])
    before = wide.copy(deep=True)
    out = uganda.food_acquired_to_canonical(wide)
    assert len(out) == 6
    assert out.loc[('2020', 'mixed', 'rice', 'Value', 'purchased'), 'Quantity'] == 30
    assert out.loc[('2020', 'mixed', 'rice', U_UNKNOWN, 'produced')].tolist() == [2, 10, 80]
    assert out.loc[('2020', 'mixed', 'rice', 'Value', 'inkind'), 'Quantity'] == 7
    assert out.loc[('2020', 'physical', 'rice', 'Kg', 'purchased')].tolist() == [2, 200, 100]
    assert out.loc[('2020', 'physical', 'rice', 'Kg', 'produced')].tolist() == [1, 80, 80]
    assert out.loc[('2020', 'cancel', 'rice', U_UNKNOWN, 'purchased')].tolist() == [0, 10, 100]
    assert out.xs('Value', level='u')['Price'].isna().all()
    pd.testing.assert_frame_equal(wide, before)



# ---------------------------------------------------------------------------
# The framework contract on synthetic frames (GH #962)
# ---------------------------------------------------------------------------

#: Enough households that every inferred rung clears its floor.
_N = max(FOOD_KG_MIN_REPORTS, FOOD_KG_MIN_BASELINE) + 3
_ITEMS = ('rice', 'maize', 'beans')


def _canonical(rows):
    df = pd.DataFrame(rows, columns=['t', 'i', 'j', 'u', 's',
                                     'Quantity', 'Expenditure'])
    return df.set_index(['t', 'i', 'j', 'u', 's'])


def _priced_population(probe_unit, *, probe_items=_ITEMS, probe_households=_N):
    """Kg rows at 100/kg, a ``Tas`` control at 300/Tas, and *probe_unit*.

    Every household buys every item in kilograms (a live ``(t, j)``
    baseline and a live per-household u-pooled ratio) and in ``Tas``; the
    probe unit is priced identically to ``Tas``, so any rung that serves
    ``Tas`` would serve it 3.0 kg/unit.  Whatever separates the two is the
    sentinel rule and nothing else.
    """
    rows = []
    for k in range(_N):
        for j in _ITEMS:
            rows.append(('2020', f'h{k}', j, 'Kg', 'purchased', 2.0, 200.0))
            rows.append(('2020', f'h{k}', j, 'Tas', 'purchased', 2.0, 600.0))
    for k in range(probe_households):
        for j in probe_items:
            rows.append(('2020', f'h{k}', j, probe_unit, 'purchased', 2.0, 600.0))
    return _canonical(rows)


def _probe_mask(df, probe_unit):
    u = pd.Series(df.index.get_level_values('u'), dtype=object)
    if pd.isna(probe_unit):
        return u.isna().to_numpy()
    return u.eq(probe_unit).to_numpy()


_SENTINEL_SPELLINGS = sorted(_U_SENTINELS) + [U_UNKNOWN, 'MANQUANT',
                                              '  UNKNOWN ', np.nan]


@pytest.mark.parametrize('spelling', _SENTINEL_SPELLINGS, ids=repr)
def test_sentinel_spelling_gets_no_price_ratio_factor(spelling):
    df = _priced_population(spelling)
    probe = _probe_mask(df, spelling)
    assert probe.sum() == _N * len(_ITEMS) >= FOOD_KG_MIN_REPORTS
    factors = food_kg_factors(df)
    # The real-unit control on the SAME priced population is inferred, so
    # the inference was live; the sentinel is refused by rule, not by luck.
    control = factors.xs('Tas', level='u')
    assert control['KgFactorSource'].isin(
        ['item_unit', 'item_unit_tight', 'unit']).all()
    assert np.allclose(control['kg_per_unit'], 3.0)
    assert factors.loc[probe, 'KgFactorSource'].eq('none').all()
    assert factors.loc[probe, 'kg_per_unit'].isna().all()


def test_every_sentinel_spelling_is_exercised():
    """Adding a spelling to ``_U_SENTINELS`` extends the test above; this
    pins that the parametrisation is derived from the set, not copied."""
    covered = {str(s).strip().lower() for s in _SENTINEL_SPELLINGS
               if not pd.isna(s)}
    assert set(_U_SENTINELS) <= covered


def test_sentinel_rows_keep_survey_kilograms():
    """``survey_kg`` outranks the sentinel rule: the survey's own kilograms
    are not an inference from a pooled group."""
    df = _priced_population(U_UNKNOWN).assign(Quantity_kg=np.nan)
    probe = _probe_mask(df, U_UNKNOWN)
    df.loc[probe, 'Quantity_kg'] = 5.0
    factors = food_kg_factors(df)
    assert factors.loc[probe, 'KgFactorSource'].eq('survey_kg').all()
    assert np.allclose(factors.loc[probe, 'kg_per_unit'], 2.5)


@pytest.mark.parametrize('code', ['136', '659', '254'])
def test_accepted_residual_code_receives_the_unit_pooled_factor(code):
    """An accepted residual is a unit nobody can name -- NOT a missing unit.

    Shaped like Uganda's 24 rows: a rare code on an item with no baseline of
    its own (so no ``(item, unit)`` cell), bought by households that also buy
    in kilograms.  It is served the u-pooled ``unit`` rung, disclosed in
    ``KgFactorSource``.
    """
    df = _priced_population(code, probe_items=('oil',), probe_households=3)
    probe = _probe_mask(df, code)
    factors = food_kg_factors(df)
    assert factors.loc[probe, 'KgFactorSource'].eq('unit').all()
    assert np.allclose(factors.loc[probe, 'kg_per_unit'], 3.0)
    assert factors.attrs['kg_factor_sources']['unit'] == probe.sum()


def test_accepted_residual_code_is_included_in_kilograms():
    """...and therefore its kilograms ARE in ``food_quantities(units='kgs')``
    -- the claim "only the kg aggregate excludes them" was never true."""
    df = _priced_population('136', probe_items=('oil',), probe_households=3)
    kgs = food_quantities_from_acquired(df)
    assert '136' not in set(kgs.index.get_level_values('u'))
    oil = kgs.xs('oil', level='j').xs('kg', level='u')['Quantity']
    assert np.allclose(oil, 2.0 * 3.0)
    # Contrast: the same rows spelled as the missing-unit sentinel carry no
    # kilograms and are served in their native unit.
    unknown = food_quantities_from_acquired(
        _priced_population(U_UNKNOWN, probe_items=('oil',), probe_households=3))
    assert 'oil' not in set(
        unknown.xs('kg', level='u').index.get_level_values('j'))
    assert unknown.xs(U_UNKNOWN, level='u')['Quantity'].eq(2.0).all()


@pytest.mark.parametrize('spelling', ['None', 'N/A', 'NA', '---'])
def test_unlisted_missing_unit_spelling_is_treated_as_a_unit(spelling):
    """PINNED GAP, not policy: ``_U_SENTINELS`` is closed, so a new spelling
    of "no unit" is a unit to the ladder and acquires a pooled price-ratio
    factor.  This is why a spelling must be written as ``U_UNKNOWN`` -- and
    why :func:`test_no_unclassified_missing_unit_spelling_is_minted` exists.
    If one of these spellings is ever added to the sentinel set, move it to
    the test above (that is a policy change needing its own accounting)."""
    assert spelling.strip().lower() not in _U_SENTINELS
    df = _priced_population(spelling)
    probe = _probe_mask(df, spelling)
    factors = food_kg_factors(df)
    assert factors.loc[probe, 'KgFactorSource'].ne('none').all()


# --- static guard: no new missing-unit spelling is minted silently ---------

#: Spellings a module may mint into a unit column with no ledger entry:
#: ``_U_SENTINELS`` (served ``none``) and the currency-denominated sentinel
#: ``'Value'`` (``transformations._CURRENCY_DENOMINATED_UNITS``, the
#: lowercase twin of ``country._RESERVED_U_SENTINELS``'s ``'Value'``), which
#: ``food_kg_factors`` also serves ``none``.  Neither can acquire a
#: price-ratio factor.  ``'Kg'`` is deliberately NOT here: a ``'Kg'`` minted
#: onto a row whose unit was never recorded fabricates kilograms, the worst
#: outcome a minted spelling can have, so every ``'Kg'`` site is ledgered.
_CLASSIFIED_SPELLINGS = _U_SENTINELS | _CURRENCY_DENOMINATED_UNITS

@pytest.mark.parametrize('spelling', sorted(_CLASSIFIED_SPELLINGS) + ['Value'])
def test_every_classified_spelling_gets_no_price_ratio_factor(spelling):
    """The exemption from the ledger is EARNED: a classified spelling with
    a full priced population is served ``none``, so minting it cannot
    acquire kilograms."""
    df = _priced_population(spelling)
    probe = _probe_mask(df, spelling)
    assert food_kg_factors(df).loc[probe, 'KgFactorSource'].eq('none').all()


#: Every literal a country module mints into a unit column that is NOT in
#: :data:`_CLASSIFIED_SPELLINGS`, keyed by
#: ``(path relative to countries_root, spelling)``, with the evidence for it.
#: A new entry is a CLAIM about what the spelling means; it needs evidence,
#: and if it means "no unit was recorded" it is a sentinel leak to report,
#: not to file here quietly.
MINTED_UNIT_SPELLINGS = {
    ('Nigeria/_/nigeria.py', 'None'): (
        'LIVE SENTINEL LEAK (GH #962 inventory, 2026-10-17): food_acquired_for_'
        'wave fills a NaN raw unit code with "None" before the API-time label '
        'map, so it can only mean "no unit recorded"; 847 served rows (422 '
        'priced), all receive item_unit/unit factors, 1,133 kg of 1.545e6. '
        'Reported as a residual; not fixed here (policy).'),
    ('Nigeria/2012-13/_/individual_unit_values.py', 'None'): (
        'Orphan script: individual_unit_values is in no data_scheme.yml or '
        'Makefile, so nothing served reads it.'),
    ('Nigeria/2015-16/_/individual_unit_values.py', 'None'): (
        'Orphan script, as 2012-13.'),
    ('Nigeria/2018-19/_/individual_unit_values.py', 'None'): (
        'Orphan script, as 2012-13.'),
    ('Ethiopia/_/ethiopia.py', 'Other (Specify)'): (
        'crop_production (not food): a harvest quantity with an unrecorded '
        'unit is tagged with the existing u-table value "Other (Specify)", '
        'so _unit_sentinel_mask does not see it.  Crop-side sentinel '
        'question, outside #962 food scope; reported as a residual.'),
    ('Ethiopia/_/ethiopia.py', 'Kg'): (
        'crop_production, two sites.  (a) harvest u (`out["u"] = "Kg"`): '
        'the else-branch for a wave with no harvest unit column; the site '
        'comments that W1 reports kilograms directly.  (b) sale unit '
        '(fillna): the site comments that W1/W2 sale quantities are kilos; '
        'whether a NaN DECODED sale unit in a later wave is also kilos is '
        'UNVERIFIED (#962 inventory did not cover crop tables).'),
    ('EthiopiaRHS/_/community_prices.py', 'Kg'): (
        'community_prices (module constant U): the ERHS price instruments '
        'record price per kilogram (module docstring: price1234_rev p_r1..'
        'p_r4, rd6_kgpr_Mkt kg_pr6, price2009 price_r7).  A real unit, '
        'stated by the source design, not a missing-unit spelling.'),
    ('EthiopiaRHS/_/ethiopiarhs.py', 'Kg'): (
        'crop_production: ERHS aggregates harvest to one kilogram Quantity '
        'per (i, j) (crop_production docstring); u is the constant unit of '
        'that column, not a fill for a missing unit.'),
    ('Tanzania/_/food_quantities.py', 'Kg'): (
        'Legacy script reached by no Makefile rule (the food_quantities '
        'target runs food_prices_quantities_and_expenditures.py) and '
        'derived food tables are computed at runtime.  Kg is set only after '
        'a successful conversion_to_kgs.json lookup, i.e. a real '
        'conversion, not a fill.'),
    ('Tanzania/_/food_unitvalues.py', 'Kg'): (
        'Legacy script, as food_quantities.py (read only by the equally '
        'legacy food_expenditures.py; no Makefile rule).'),
}

#: A unit column, by subscript key or receiver text: ``u`` itself, or any
#: name containing ``unit`` -- EXCEPT area units (``AreaUnit``,
#: ``area_unit``): plot area is converted to hectares by its own path and
#: never reaches the kg ladder (``Hectares``/``hectares``/``sotka`` are
#: minted there, all real).  The substring is deliberately loose
#: (``unit_price`` would match); a false positive costs a ledger entry, a
#: false negative is a silent leak.
_UNIT_RECEIVER = re.compile(r"\[['\"]u['\"]\]|unit", re.I)
_UNIT_KEY = re.compile(r"^u$|unit", re.I)
_AREA = re.compile(r"area", re.I)
_NULL_ATTRS = frozenset({'nan', 'NaN', 'NA', 'NaT'})


def _string_constants(tree):
    out = {'U_UNKNOWN': U_UNKNOWN}
    for node in tree.body:
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)):
            out[node.targets[0].id] = node.value.value
    return out


def _str_value(node, consts):
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name) and node.id in consts:
        return consts[node.id]
    return None   # a column/Series/computed value, not a minted spelling


def _is_null(node):
    if isinstance(node, ast.Constant) and node.value is None:
        return True
    if isinstance(node, ast.Attribute) and node.attr in _NULL_ATTRS:
        return True
    return isinstance(node, ast.Name) and node.id in {'nan', 'NA'}


def _subscript_key(node):
    """``X['u']`` -> ``'u'``; ``X.loc[mask, 'u']`` -> ``'u'``."""
    key = node.slice
    if isinstance(key, ast.Tuple) and key.elts:
        key = key.elts[-1]
    if isinstance(key, ast.Constant) and isinstance(key.value, str):
        return key.value
    return None


def _is_unit_key(key):
    return bool(key) and bool(_UNIT_KEY.search(key)) and not _AREA.search(key)


def _unit_aliases(tree):
    """Bare names bound to a unit column anywhere in the module
    (``u = df['u']``), so ``u.fillna('N/A')`` is a unit receiver."""
    names = set()
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and isinstance(node.value, ast.Subscript)
                and _is_unit_key(_subscript_key(node.value))):
            names.add(node.targets[0].id)
    return names


def _is_unit_receiver(node, aliases):
    if isinstance(node, ast.Name) and node.id in aliases:
        return True
    text = ast.unparse(node)
    return bool(_UNIT_RECEIVER.search(text)) and not _AREA.search(text)


def _minted_in_source(source):
    """``(lineno, spelling, idiom)`` for every literal string the module
    writes into a unit column.  Idioms covered:

    * ``<unit>.fillna(<str>)`` / ``.fillna(value=<str>)``;
    * ``<any>.fillna({<unit key>: <str>})``;
    * ``<unit>.replace(<null>, <str>)`` and ``.replace({<null>: <str>})``;
    * ``<unit>.where(cond, <str>)`` / ``.mask(cond, <str>)``;
    * ``X[<unit key>] = <str>`` / ``X.loc[..., <unit key>] = <str>``;

    where ``<str>`` is a literal or a module-level string constant and
    ``<unit>`` is a unit-column expression or a bare name bound to one.
    NOT covered: labels the survey itself recorded, org/YAML ``mapping:``
    tables, ``np.where``/``map``/``apply`` and computed strings.  A guard,
    not a proof; the inventory in each country's CONTENTS.org is the rest.
    """
    tree = ast.parse(source)
    consts = _string_constants(tree)
    aliases = _unit_aliases(tree)
    found = []

    def add(node, value, idiom):
        if value is not None:
            found.append((node.lineno, value, idiom))

    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            value = _str_value(node.value, consts)
            for target in node.targets:
                if (isinstance(target, ast.Subscript)
                        and _is_unit_key(_subscript_key(target))):
                    add(node, value, 'assign')
            continue
        if not (isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)):
            continue
        attr, recv = node.func.attr, node.func.value
        kw = {k.arg: k.value for k in node.keywords if k.arg}
        if attr == 'fillna':
            arg = node.args[0] if node.args else kw.get('value')
            if isinstance(arg, ast.Dict):
                for k, v in zip(arg.keys, arg.values):
                    if k is not None and _is_unit_key(_str_value(k, consts)):
                        add(node, _str_value(v, consts), 'fillna-dict')
            elif arg is not None and _is_unit_receiver(recv, aliases):
                add(node, _str_value(arg, consts), 'fillna')
        elif attr == 'replace' and _is_unit_receiver(recv, aliases):
            to = node.args[0] if node.args else kw.get('to_replace')
            val = node.args[1] if len(node.args) > 1 else kw.get('value')
            if isinstance(to, ast.Dict) and val is None:
                for k, v in zip(to.keys, to.values):
                    if k is not None and _is_null(k):
                        add(node, _str_value(v, consts), 'replace-dict')
            elif to is not None and val is not None and _is_null(to):
                add(node, _str_value(val, consts), 'replace')
        elif attr in ('where', 'mask') and _is_unit_receiver(recv, aliases):
            other = node.args[1] if len(node.args) > 1 else kw.get('other')
            if other is not None:
                add(node, _str_value(other, consts), attr)
    return found


def _minted_unit_spellings():
    """``(relpath, lineno, spelling)`` over every module in the country
    tree (see :func:`_minted_in_source` for the idioms)."""
    root = countries_root()
    found = []
    for path in sorted(Path(root).rglob('*.py')):
        try:
            source = path.read_text(encoding='utf-8')
            sites = _minted_in_source(source)
        except (SyntaxError, UnicodeDecodeError):
            continue
        rel = path.relative_to(root).as_posix()
        found.extend((rel, line, value) for line, value, _ in sites)
    return found


@pytest.mark.parametrize('snippet', [
    "df['u'] = df['u'].fillna('ZZ')",
    "df['u'] = df['u'].fillna(value='ZZ')",
    "MISSING = 'ZZ'\ndf['u'] = df['u'].fillna(MISSING)",
    "df = df.fillna({'u': 'ZZ'})",
    "df = df.fillna(value={'unit': 'ZZ'})",
    "df['u'] = df['u'].replace(np.nan, 'ZZ')",
    "df['u'] = df['u'].replace(None, 'ZZ')",
    "df['u'] = df['u'].replace({pd.NA: 'ZZ'})",
    "df['u'] = df['u'].where(df['u'].notna(), 'ZZ')",
    "df['u'] = df['u'].mask(df['u'].isna(), other='ZZ')",
    "df.loc[df['u'].isna(), 'u'] = 'ZZ'",
    "df['u'] = 'ZZ'",
    "out['unit_code'] = 'ZZ'",
    "u = df['u']\nu = u.fillna('ZZ')",
    "s = df['u']\ns.fillna('ZZ')",
])
def test_the_sweep_catches_each_minting_idiom(snippet):
    """Every idiom the sweep claims to cover is caught on a synthetic
    module -- the red team's GH #962 mutation set, which the fillna-only
    sweep caught two of."""
    assert [v for _, v, _ in _minted_in_source(snippet)] == ['ZZ']


@pytest.mark.parametrize('snippet', [
    "df['u'] = df['u'].fillna(df['unit_label'])",   # a column, not a literal
    "df['Price'] = df['Price'].fillna('ZZ')",       # not a unit column
    "df['AreaUnit'] = 'Hectares'",                  # area unit, own path
    "df['u'] = df['u'].replace('kgs', 'Kg')",       # recode, not a null fill
])
def test_the_sweep_ignores_what_is_not_a_minted_unit(snippet):
    assert _minted_in_source(snippet) == []


def test_the_sweep_sees_the_known_minting_sites():
    """The sweep is not vacuous: it finds the sentinel writers we know of
    (Niger's ``U_NA``, the EHCVM ``Manquant`` plot_inputs fills, Uganda's
    ``where``/``mask`` fills, the ``u='Value'`` assignments) as well as the
    ledgered non-sentinel ones."""
    found = {(p, v) for p, _, v in _minted_unit_spellings()}
    assert ('Niger/_/niger.py', 'Unknown') in found
    assert ('Togo/2018/_/plot_inputs.py', 'Manquant') in found
    assert ('Uganda/_/uganda.py', 'Unknown') in found
    assert ('Uganda/_/uganda.py', 'Value') in found
    assert ('Panama/1997/_/food_acquired.py', 'Value') in found
    assert set(MINTED_UNIT_SPELLINGS) <= found, (
        'stale MINTED_UNIT_SPELLINGS entries (the site no longer mints the '
        f'spelling -- delete them): {sorted(set(MINTED_UNIT_SPELLINGS) - found)}')


def test_no_unclassified_missing_unit_spelling_is_minted():
    """A module that writes a missing unit must use a ``_U_SENTINELS``
    spelling (``transformations.U_UNKNOWN``).  Anything else outside
    :data:`_CLASSIFIED_SPELLINGS` can acquire a price-ratio factor silently
    (see the pinned gap above), so it must be recorded in
    :data:`MINTED_UNIT_SPELLINGS` with evidence.  This does not reject an
    unfamiliar LOCAL unit: only literals a module MINTS into a unit column,
    by the idioms :func:`_minted_in_source` lists, are checked -- never
    labels the survey itself recorded, and never org/YAML mapping tables."""
    unclassified = [
        f'{p}:{line} writes u = {v!r}'
        for p, line, v in _minted_unit_spellings()
        if v.strip().lower() not in _CLASSIFIED_SPELLINGS
        and (p, v) not in MINTED_UNIT_SPELLINGS]
    assert not unclassified, (
        'unclassified minted unit spelling(s); write U_UNKNOWN for a missing '
        'unit, or record the spelling with evidence in '
        'MINTED_UNIT_SPELLINGS:\n  ' + '\n  '.join(unclassified))
