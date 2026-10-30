"""Missing reported seed-purchase amounts do not become group-sum zeros."""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def eth(monkeypatch):
    path = Path(__file__).resolve().parents[1] / 'lsms_library/countries/Ethiopia/_/ethiopia.py'
    spec = importlib.util.spec_from_file_location('_eth1184', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, '_eth_crop_label_map_for_wave', lambda t: {1: 'Maize', 2: 'Teff'})
    return module


def build(eth, amounts, flags=None, fields=(1,), quantity=4., direct=False):
    if flags is None:
        flags = [1] * len(amounts)
    planting = pd.DataFrame({'hh': ['H'] * len(fields), 'holder': [7] * len(fields),
                             'parcel': [1] * len(fields), 'field': fields,
                             'crop': [1] * len(fields), 'qty': [quantity] * len(fields)})
    seeds = pd.DataFrame({'hh': ['H'] * len(amounts), 'holder': [7] * len(amounts),
                         'parcel': [1] * len(amounts), 'field': [1] * len(amounts),
                         'crop': [1] * len(amounts), 'bought': flags,
                         'amount': pd.array(amounts, dtype='Float64')})
    colmap = dict(hhid='hh', holder_id='holder', parcel_id='parcel', field_id='field',
                  seed_src='planting', seed_crop_code='crop', seed_qty='qty',
                  p_holder_id='holder', p_crop_code='crop', p_purch_flag='bought',
                  p_purch_qty='amount')
    if direct:
        # The shipped W1 driver uses these actual source/key spellings.
        planting = planting.rename(columns={'field': 'field_id', 'parcel': 'parcel_id'})
        seeds = seeds.rename(columns={'field': 'field_id', 'parcel': 'parcel_id'})
        colmap.update(field_id='field_id', parcel_id='parcel_id',
                      p_field_id='field_id', p_parcel_id='parcel_id')
    return eth.plot_inputs_for_wave('2013-14', None, planting, seeds, colmap)


@pytest.mark.parametrize('amounts', [[None], [None, None]])
def test_yes_all_missing_amounts_stay_missing_and_keep_reported_purchase(eth, amounts):
    result = build(eth, amounts, quantity=None)
    assert len(result) == 1
    assert result.Purchased.tolist() == [True]
    assert result.Quantity_purchased.isna().all()
    assert result.Quantity.isna().all()
    assert result.u.tolist() == ['Kg']


@pytest.mark.parametrize('amounts,flags,expected', [
    ([0.], [1], 0.), ([0., None], [1, 1], 0.),
    ([2.000000000000001, None], [1, 1], 2.000000000000001),
    ([None], [2], 0.), ([None, None], [1, 2], 0.),
])
def test_reported_zero_no_and_partial_known_sum_are_preserved(eth, amounts, flags, expected):
    result = build(eth, amounts, flags)
    np.testing.assert_array_equal(result.Quantity_purchased.to_numpy(), np.array([expected]))
    np.testing.assert_array_equal(result.Quantity.to_numpy(), np.array([4.]))
    assert result.Quantity_purchased.dtype == pd.Float64Dtype()
    assert result.Purchased.tolist() == [1 in flags]


@pytest.mark.parametrize('amount', [None, 3.])
def test_holder_crop_with_two_plots_does_not_attach_purchase(eth, amount):
    result = build(eth, [amount], fields=(1, 2))
    assert len(result) == 2
    assert result.Purchased.isna().all() and result.Quantity_purchased.isna().all()
    np.testing.assert_array_equal(result.Quantity.to_numpy(), np.array([4., 4.]))
    assert result.index.get_level_values('plot_id').tolist() == ['7_1_1', '7_1_2']


def test_direct_plot_crop_join_keeps_missing_yes_amount_without_aggregation(eth):
    result = build(eth, [None], direct=True)
    assert len(result) == 1 and result.Purchased.tolist() == [True]
    assert result.Quantity_purchased.isna().all()
    assert result.Quantity.tolist() == [4.]
