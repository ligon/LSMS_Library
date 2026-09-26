"""Uganda missing units retain quantity reports and carry value-only sources.

Exercise the country reader and canonicalizer with small raw frames, then
the ordinary runtime derivations. No microdata or cached tables are needed.
"""
import importlib.util
from pathlib import Path
from unittest.mock import mock_open

import numpy as np
import pandas as pd
import pytest

from lsms_library import local_tools
from lsms_library.transformations import (
    U_UNKNOWN,
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
