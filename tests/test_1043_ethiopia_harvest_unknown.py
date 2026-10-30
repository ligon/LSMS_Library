"""Missing harvest units are absent, not the questionnaire's reported Other."""
import importlib.util
import warnings
import numpy as np
import pandas as pd
import pytest
from lsms_library.paths import countries_root
from lsms_library.transformations import U_UNKNOWN, _unit_sentinel_mask, harvest_kg_factors


@pytest.fixture(scope='module')
def eth():
    path = countries_root() / 'Ethiopia/_/ethiopia.py'
    spec = importlib.util.spec_from_file_location('_eth1043', path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


COLS = dict(hhid='hh', holder_id='holder', parcel_id='parcel', field_id='field',
            crop_code='crop', quantity='qty', s_crop_code='crop', s_sold_flag='sold',
            s_qty='sqty', s_value='sval')


def build(eth, units, quantity=5., value=None):
    h = pd.DataFrame({'hh':['hh'], 'holder':['holder'], 'parcel':[1],
                      'field':[1], 'crop':[7], 'qty':[quantity]})
    sale = None if value is None else pd.DataFrame({'holder':['holder'], 'crop':[7],
                                                   'sold':[1], 'sqty':[np.nan], 'sval':[value]})
    return eth.crop_production_for_wave('2013-14', h, None, sale, COLS,
                                       unit_labels=units)


@pytest.mark.parametrize('absent', [None, np.nan, pd.NA, '', '   ', '0'])
def test_missing_harvest_unit_is_unknown_with_payload_and_row_retained(eth, absent):
    out = build(eth, pd.Series([absent], dtype=object))
    assert len(out) == 1 and out.Quantity.tolist() == [5.]
    assert out.index.get_level_values('u').tolist() == [U_UNKNOWN]
    assert _unit_sentinel_mask(out).all()
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        factors = harvest_kg_factors(out)
    assert factors.KgFactorSource.tolist() == ['none']


@pytest.mark.parametrize('label,expected', [('99. Other(Specify)', 'Other (Specify)'),
                                          ('Other (Specify)', 'Other (Specify)'),
                                          ('321', '321'), ('1. Kilogram', 'Kg')])
def test_reported_unit_is_preserved_and_not_misclassified_missing(eth, label, expected):
    out = build(eth, pd.Series([label]))
    assert len(out) == 1 and out.Quantity.tolist() == [5.]
    assert out.index.get_level_values('u').tolist() == [expected]
    assert not _unit_sentinel_mask(out).any()


def test_sale_value_only_row_still_survives_with_unknown_harvest_unit(eth):
    out = build(eth, pd.Series([None], dtype=object), quantity=np.nan, value=30.)
    assert len(out) == 1 and out.Value_sold.tolist() == [30.]
    assert out.Quantity.isna().all() and out.Quantity_sold.isna().all()
    assert out.index.get_level_values('u').tolist() == [U_UNKNOWN]


def test_unitless_empty_placeholder_is_still_dropped(eth):
    assert build(eth, pd.Series([None], dtype=object), quantity=np.nan).empty


def test_w1_instrument_defined_kilograms_remain_kilograms(eth):
    assert build(eth, None).index.get_level_values('u').tolist() == ['Kg']
