"""Invented regression operands for the revised tree-sale window ruling.

No source/cache acquisition. Actual helper/fold code executes; only vocabulary
leaves are identity mappings over already-canonical invented labels.
"""
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

from lsms_library.countries.Mali._ import mali as M
from lsms_library import local_tools as L


@pytest.fixture(autouse=True)
def invented_only(monkeypatch):
    def denied(*args, **kwargs):
        raise AssertionError('invented test attempted real IO')
    monkeypatch.setattr(L, 'get_dataframe', denied)
    monkeypatch.setattr(L, 'to_parquet', denied)
    monkeypatch.setattr(L, '_ensure_dvc_pulled', denied)
    monkeypatch.setattr(M, '_crop_labels', lambda values: values.astype('string'))
    monkeypatch.setattr(M, '_unit_labels', lambda values: values.astype('string'))


def raw(wave, months, harvest, sold, value):
    v = M._TREE_VARS[wave]
    return pd.DataFrame({v['months']: months, v['q']: harvest, v['qs']: sold,
                         v['v']: value, v['u']: ['Kg'] * len(months),
                         v['us']: ['Kg'] * len(months)})


def numeric(actual, expected):
    mask = actual.isna().to_numpy()
    expected = np.asarray(expected, dtype='float64')
    np.testing.assert_array_equal(mask, np.isnan(expected), strict=True)
    np.testing.assert_array_equal(actual.to_numpy(dtype='float64', na_value=np.nan)[~mask],
                                  expected[~mask], strict=True)


def assembled(measured, wave, *, units=None):
    frame = measured.copy()
    frame['t'], frame['i'], frame['plot_id'] = wave, 'invented-household', pd.NA
    frame['crop'], frame['perennial'], frame['intercropped'] = 'Invented fruit', True, pd.NA
    frame['planting_month'], frame['harvest_month'] = pd.NA, pd.NA
    if units is not None:
        frame['Unit_sold'] = units
    return frame


@pytest.mark.parametrize('wave', ['2014-15', '2017-18'])
@pytest.mark.parametrize('months', [0, 99, 3])
def test_recorded_sale_does_not_depend_on_harvest_months(wave, months):
    operand = raw(wave, [months], [4.0], [2.0], [30.0])
    before = operand.copy(deep=True)
    result = M.tree_crop_rows(operand, wave)
    numeric(result.Quantity, [12.0 if months == 3 else np.nan])
    numeric(result.Quantity_sold, [2.0])
    numeric(result.Value_sold, [30.0])
    assert result.index.equals(operand.index) and result.attrs == {}
    assert operand.equals(before)


@pytest.mark.parametrize('wave', ['2014-15', '2017-18'])
def test_quantity_and_value_codes_are_not_interchanged(wave):
    operand = raw(wave, [3] * 6, [33, 99, 9999, np.float32(9999.9), 0, 4],
                  [2, 99, 9999, np.float32(9999.9), 0, 5],
                  [999998, 999999, 9999, 9, 0, np.nan])
    result = M.tree_crop_rows(operand, wave)
    numeric(result.Quantity, [99, np.nan, np.nan, np.nan, 0, 12])
    numeric(result.Quantity_sold, [2, np.nan, np.nan, np.nan, 0, 5])
    numeric(result.Value_sold, [999998, np.nan, 9999, 9, 0, np.nan])


@pytest.mark.parametrize('wave', ['2014-15', '2017-18'])
def test_duplicate_fold_sums_recorded_sales_after_rowwise_harvest(wave):
    measured = M.tree_crop_rows(raw(wave, [2, 3], [10, 10], [3, 7], [100, 200]), wave)
    numeric(measured.Quantity, [20, 30])
    numeric(measured.Quantity_sold, [3, 7])
    final = M.crop_production_finalize(assembled(measured, wave))
    assert len(final) == 1 and final.index.is_unique
    numeric(final.Quantity, [50])
    numeric(final.Quantity_sold, [10])
    numeric(final.Value_sold, [300])
    numeric(final.months_harvested, [2])  # preserve the existing first metadata rule
    stamp = M.stamp_tree_derivations(final)
    assert stamp.iloc[0] == '+'.join(sorted([M.DERIVATION_TREES, M.DERIVATION_TREE_SALES]))


def test_field_masks_and_all_missing_sales_keep_their_scope():
    measured = pd.DataFrame({'Quantity': [99.0, 99.0, 1.0, 2.0],
                            'Quantity_sold': [99.0, 4.0, np.nan, np.nan],
                            'Value_sold': [999999.0, 999998.0, np.nan, np.nan],
                            'u': ['Kg'] * 4, 'Unit_sold': ['Kg'] * 4,
                            'months_harvested': [pd.NA, 3, pd.NA, pd.NA]})
    frame = assembled(measured, '2014-15')
    frame['i'] = ['field', 'tree', 'unrecorded', 'unrecorded']
    frame['perennial'] = [False, True, False, False]
    result = M.crop_production_finalize(frame).reset_index().set_index('i')
    assert pd.isna(result.loc['field', 'Quantity'])
    assert pd.isna(result.loc['field', 'Quantity_sold'])
    assert result.loc['field', 'Value_sold'] == 999999
    assert result.loc['tree', 'Quantity'] == 99  # genuine tree total survives
    assert result.loc['tree', 'Value_sold'] == 999998
    assert pd.isna(result.loc['unrecorded', 'Quantity_sold'])
    assert pd.isna(result.loc['unrecorded', 'Value_sold'])


def test_duplicate_sold_unit_collision_retains_existing_refusal():
    measured = M.tree_crop_rows(raw('2014-15', [2, 3], [10, 10], [3, 7], [100, 200]), '2014-15')
    result = M.crop_production_finalize(assembled(measured, '2014-15', units=['Kg', 'Sac']))
    assert len(result) == 1
    assert result.Quantity_sold.isna().all() and result.Unit_sold.isna().all()
    numeric(result.Quantity, [50])
    numeric(result.Value_sold, [300])


def test_registry_separates_harvest_and_sale_without_source_reads():
    path = Path(M.__file__).with_name('derivations.yml')
    registry = yaml.safe_load(path.read_text())
    harvest, sale = registry[M.DERIVATION_TREES], registry[M.DERIVATION_TREE_SALES]
    assert harvest['columns'] == ['Quantity']
    assert sale['columns'] == ['Quantity_sold', 'Value_sold']
    assert harvest['function'].endswith(':derive_tree_twelve_month_total')
    assert sale['function'].endswith(':derive_tree_sale_window')
    assert harvest['inputs'].endswith(':inputs_tree_twelve_month_total')
    assert sale['inputs'].endswith(':inputs_tree_sale_window')
