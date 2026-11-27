"""IHS5 numeric crop-code retention, with invented operands only."""
import importlib.util
import warnings

import pandas as pd
import pytest

from lsms_library.paths import countries_root


@pytest.fixture
def mw():
    file = countries_root() / 'Malawi/_/malawi.py'
    spec = importlib.util.spec_from_file_location('malawi_1724_raw', file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def harvest(mw, household, codes):
    raw = pd.DataFrame({'hhid': household, 'plotkey': 'R1', 'code': codes,
                        'q': 12.0, 'unit': 9, 'state': 3})
    return mw._harvest_block(
        raw, hhid='hhid', plotkey='plotkey', cropcode='code', qty='q',
        unit='unit', condition='state', t='2019-20')


def test_only_admitted_native_code49_is_retained_at_harvest_grain(mw):
    xs = mw.keep_unmapped_seasonal_harvest_code(harvest(mw, 'xs', [48, 49, 98]), 49)
    panel = harvest(mw, 'panel', [48, 49])
    with pytest.warns(UserWarning, match='2 harvest row'):
        out = mw.assemble_crop_production('2019-20', [xs, panel], [])
    assert set(out.index.get_level_values('crop')) == {'49', 'Other'}
    added = out.xs('49', level='crop')
    assert added.index.get_level_values('i').tolist() == ['xs']
    assert added['Quantity'].tolist() == [12.0]
    assert added['production_obs'].tolist() == ['reported:G:49']
    assert added['Derivation'].isna().all()
    assert '_keep_raw_harvest_code' not in out


def test_fallback_does_not_duplicate_or_move_an_existing_farm_sale(mw):
    original = harvest(mw, 'xs', [49])
    source = pd.DataFrame({'hhid': ['xs'], 'code': [49], 'sold': [1],
                           'qs': [5.0], 'vs': [100.0], 'us': [9], 'state': [3]})
    sale = mw._sale_block(source, hhid='hhid', cropcode='code', sold_flag='sold',
                         qty_sold='qs', value_sold='vs', unit_sold='us', condition='state')
    with pytest.warns(UserWarning, match='1 harvest row'):
        before = mw.assemble_crop_production('2019-20', [original], [sale])
    marked = mw.keep_unmapped_seasonal_harvest_code(original, 49)
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        out = mw.assemble_crop_production('2019-20', [marked], [sale])
    farm = out.index.get_level_values('plot_id') == mw.FARM_LEVEL
    pd.testing.assert_frame_equal(out[farm], before)
    assert out.attrs == before.attrs
    assert out.loc[~farm, ['Quantity_sold', 'Value_sold', 'Unit_sold',
                           'Condition_sold', 'Derivation']].isna().all().all()
    assert out['Value_sold'].sum() == 100.0


def test_raw_fallback_never_overrides_a_reviewed_name(mw):
    source = harvest(mw, 'xs', [49]).assign(crop='Already named')
    marked = mw.keep_unmapped_seasonal_harvest_code(source, 49)
    out = mw.assemble_crop_production('2019-20', [marked], [])
    assert out.index.get_level_values('crop').tolist() == ['Already named']
