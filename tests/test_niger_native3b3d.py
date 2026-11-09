"""Native Niger3B/3D transport: invented operands, real identity/API helpers."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import sys
import warnings

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

import lsms_library as ll
from lsms_library import local_tools
from lsms_library.country import Country, _no_v_join_tables
from lsms_library.feature import Feature, _canonical_index_levels, _required_index_levels
from lsms_library.paths import countries_root

TABLES = [('cultivation_records', 'cultivation_obs', '3b'),
          ('farm_cost_operands', 'cost_obs', '3d')]


@pytest.fixture
def drivers(monkeypatch):
    root = countries_root() / 'Niger'
    monkeypatch.syspath_prepend(str(root / '_'))
    monkeypatch.delitem(sys.modules, 'niger', raising=False)
    modules = {}
    for table, _, _ in TABLES:
        path = root / '2011-12' / '_' / (table + '.py')
        spec = importlib.util.spec_from_file_location('native_' + table, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        modules[table] = module
    return modules


def source(role):
    # Deliberately non-RangeIndex, repeated household and distinct slots/rows.
    frame = pd.DataFrame({'hid': np.array([811, 811, 812, 813], dtype='int16'),
                          'passage': np.array([1] * 4, dtype='int8'),
                          'grappe': np.array([8] * 4, dtype='int16'),
                          'menage': np.array([11, 11, 12, 13], dtype='int8')},
                         index=pd.Index([9, 3, 8, 2], name='source_index'))
    if role == '3b':
        frame['as03bqa'] = np.array([1, 2, 1, 1], dtype='int8')
        frame['as03bq01'] = np.array([1] * 4, dtype='int8')
        frame['as03bq02'] = pd.array(['field', 'field', '', None], dtype='str')
        frame['as03bq03'] = np.array([1] * 4, dtype='int8')
        frame['as03bq04'] = np.array([1, 2, 1, 2], dtype='int8')
        frame['as03bq05'] = [1., 1., np.nan, np.nan]
        frame['as03bq06'] = pd.array(['mil', 'mil', 'rice', 'other'], dtype='str')
        frame['as03bq07'] = [1., 1., 2., np.nan]
        frame['as03bq08'] = [1., 2., 9., np.nan]
        frame['as03bq09'] = [1., 3., 9., np.nan]
        frame['as03bq10'] = [1., 4., 9., np.nan]
        frame['as03bq11'] = [1., 6., np.nan, np.nan]
        for letter in 'abcdefgh':
            frame['as03bq12' + letter] = [1., 2., 9., np.nan]
        frame['ms00q05'] = np.array([1] * 4, dtype='int8')
        frame['ms00q06'] = np.array([1] * 4, dtype='int8')
    else:
        frame['ms00q05'] = [1., 1., np.nan, np.nan]
        frame['ms00q06'] = [1., 1., np.nan, np.nan]
        frame['as03dq01'] = pd.array(['Water', 'Water', 'unknown', 'unknown'], dtype='str')
        frame['as03dq02'] = [3., 3., np.nan, np.nan]
        frame['as03dq03'] = [1., 2., np.nan, np.nan]
        frame['as03dq04'] = [99., 0., 2., np.nan]
        frame['as03dq05'] = [1.25, 0., np.nan, np.nan]
        frame['as03dq06'] = [1., 5., np.nan, np.nan]
    frame.attrs = {'native_witness': {'codes': [9, None]}, 'note': 'invented'}
    return frame


def assert_native(out, original):
    restored = out.copy()
    restored.index = original.index.copy()
    assert_frame_equal(restored, original, check_exact=True)
    assert restored.attrs == original.attrs
    assert list(out.columns) == list(original.columns)
    assert len(out) == len(original) and out.index.is_unique
    assert not {'Quantity', 'Cost', 'Expenditure', 'plot_id', 'season'} & set(out.columns)
    assert not out.attrs.get('id_converted')


@pytest.mark.parametrize('table,obs,role', TABLES)
def test_full_native_conservation_and_input_immutability(drivers, table, obs, role):
    raw = source(role)
    before = deepcopy(raw)
    out = drivers[table].build_native(raw)
    assert_native(out, raw)
    assert out.index.names == ['t', 'i', obs]
    assert out.index.get_level_values('i').tolist() == ['811', '811', '812', '813']
    assert out.index.get_level_values('t').tolist() == ['2011-12'] * 4
    tokens = out.index.get_level_values(obs).tolist()
    if role == '3b':
        assert [t.rsplit('=', 1)[1] for t in tokens] == ['1', '2', '1', '1']
        assert all('ecvmaas3b_p1.dta@dvc:' in t and ':as03bqa=' in t for t in tokens)
        assert out['as03bq04'].tolist() == [1, 2, 1, 2]
        assert out['as03bq12a'].iloc[:3].tolist() == [1., 2., 9.]
    else:
        assert [t.rsplit(':', 1)[1] for t in tokens] == ['0', '1', '2', '3']
        assert all(t.startswith('row:dvc:') for t in tokens)
        assert out['as03dq04'].iloc[:3].tolist() == [99., 0., 2.]
        assert out['as03dq05'].iloc[:2].tolist() == [1.25, 0.]
        assert out['as03dq02'].isna().tolist() == [False, False, True, True]
    assert_frame_equal(raw, before, check_exact=True)
    assert raw.attrs == before.attrs


@pytest.mark.parametrize('table,obs,role', TABLES)
def test_equal_payloads_remain_distinct_observations(drivers, table, obs, role):
    raw = pd.concat([source(role).iloc[[0]], source(role).iloc[[0]]], ignore_index=True)
    if role == '3b':
        raw['as03bqa'] = np.array([1, 2], dtype='int8')
    out = drivers[table].build_native(raw)
    assert_native(out, raw)
    assert out.index.get_level_values(obs).nunique() == 2


@pytest.mark.parametrize('table,obs,role', TABLES)
def test_empty_native_frame_keeps_all_column_types(drivers, table, obs, role):
    raw = source(role).iloc[:0]
    out = drivers[table].build_native(raw)
    assert_native(out, raw)
    assert out.index.names == ['t', 'i', obs]


@pytest.mark.parametrize('table,obs,role', TABLES)
@pytest.mark.parametrize('coordinate', ['t', 'i', 'observation'])
def test_reserved_coordinate_refuses_before_overwrite(drivers, table, obs, role, coordinate):
    raw = source(role)
    raw[obs if coordinate == 'observation' else coordinate] = 'recorded'
    before = deepcopy(raw)
    with pytest.raises(ValueError, match='reserved coordinate'):
        drivers[table].build_native(raw)
    assert_frame_equal(raw, before, check_exact=True)
    assert raw.attrs == before.attrs


@pytest.mark.parametrize('slots', [[1., 1., 1., 1.], [1., None, 1., 1.],
                                   [1., 0., 1., 1.], [1., -2., 1., 1.],
                                   [1., 2.5, 1., 1.]])
def test_cultivation_invalid_recorded_slot_refuses(drivers, slots):
    raw = source('3b')
    raw['as03bqa'] = slots
    with pytest.raises(ValueError, match='household|slot'):
        drivers['cultivation_records'].build_native(raw)


def test_cultivation_missing_household_uses_existing_identity_refusal(drivers):
    raw = source('3b').astype({'hid': 'Float64'})
    raw.loc[raw.index[1], 'hid'] = pd.NA
    with pytest.raises(ValueError, match='household'):
        drivers['cultivation_records'].build_native(raw)


def test_cost_missing_household_is_not_replaced_by_an_invented_id(drivers):
    raw = source('3d').astype({'hid': 'Float64'})
    raw.loc[raw.index[1], 'hid'] = pd.NA
    out = drivers['farm_cost_operands'].build_native(raw)
    assert pd.isna(out.index.get_level_values('i')[1])
    assert_native(out, raw)


@pytest.mark.parametrize('table,obs,role', TABLES)
def test_main_uses_one_numeric_source_and_writes_only_success(drivers, monkeypatch, table, obs, role):
    module = drivers[table]
    raw = source(role)
    calls = []
    def reader(path, **kwargs):
        calls.append(('read', path, kwargs))
        return raw
    monkeypatch.setattr(module, 'get_dataframe', reader)
    monkeypatch.setattr(module, 'to_parquet', lambda frame, path: calls.append(('write', path, frame)))
    module.main()
    assert calls[0] == ('read', '../Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaas' + role + '_p1.dta',
                        {'convert_categoricals': False})
    assert len(calls) == 2 and calls[1][0:2] == ('write', table + '.parquet')
    assert_native(calls[1][2], raw)
    calls.clear()
    def fail(_):
        raise ValueError('identity failed')
    monkeypatch.setattr(module, 'build_native', fail)
    with pytest.raises(ValueError, match='identity failed'):
        module.main()
    assert len(calls) == 1


@pytest.mark.parametrize('table,obs,role', TABLES)
def test_registered_shape_actual_country_finalizer_and_singleton_feature(drivers, monkeypatch, table, obs, role):
    assert _canonical_index_levels(table) == ['t', 'i', obs]
    assert _required_index_levels(table) == ['t', 'i', obs]
    assert table in _no_v_join_tables()
    native = drivers[table].build_native(source(role))
    before = deepcopy(native)
    country = Country('Niger', preload_panel_ids=False)
    # Actual walk, raw mode, with an invented transitive map. The finalizer
    # must not apply the second edge after the wave-level raw walk.
    country._updated_ids_cache = {'2011-12': {'811': '900', '900': '1000'}}
    country._panel_ids_attempted = True
    country.__dict__['_categorical_mapping_cache'] = {}
    walked = local_tools.id_walk(native.copy(), country.updated_ids, state='raw')
    assert walked.index.get_level_values('i').tolist()[:2] == ['900', '900']
    def forbidden(*a, **kw):
        raise AssertionError('native transport must not request sample/market/source')
    monkeypatch.setattr(country, '_join_v_from_sample', forbidden)
    served = country._finalize_result(walked, country.resources['Data Scheme'][table], table)
    expected = before.rename(index={'811': '900'}, level='i').reorder_levels(['i', 't', obs])
    assert_frame_equal(served, expected, check_exact=True)
    assert served.attrs['id_converted'] is True
    assert 'v' not in served.index.names
    assert_frame_equal(native, before, check_exact=True)
    assert native.attrs == before.attrs
    # Actual singleton Feature assembly calls the served Country adapter;
    # source/cold/warm orchestration is a separate actual-source gate.
    setattr(country, table, lambda **kw: served.copy(deep=True))
    monkeypatch.setattr(ll, 'Country', lambda *a, **kw: country)
    feat = Feature(table)
    feat._countries = ['Niger']
    out = feat(['Niger'], currency=None)
    assert out.index.names == ['country', 't', 'i', obs]
    actual = out.xs('Niger', level='country').reorder_levels(served.index.names)
    assert_frame_equal(actual, served, check_exact=True)


@pytest.mark.parametrize('table,obs,role', TABLES)
def test_feature_missing_observation_degrades_and_all_null_column_boundary(drivers, monkeypatch, table, obs, role):
    frame = drivers[table].build_native(source(role))
    frame['all_null_invented'] = pd.array([None] * len(frame), dtype='Float64')
    class Provider:
        name = 'Invented'
    provider = Provider()
    setattr(provider, table, lambda **kw: frame.copy(deep=True))
    monkeypatch.setattr(ll, 'Country', lambda *a, **kw: provider)
    feat = Feature(table)
    feat._countries = ['Invented']
    out = feat(['Invented'], currency=None)
    assert len(out) == len(frame)
    assert 'all_null_invented' not in out
    frame.index = frame.index.droplevel(obs)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        out = feat(['Invented'], currency=None)
    assert out.empty
    assert any('required index' in str(w.message) for w in caught)


def test_reserved_plot_cultivation_is_not_registered_or_an_alias():
    # GH862 reserves this name for a future per-season plot/person design.
    assert _canonical_index_levels('plot_cultivation') == []
    assert _required_index_levels('plot_cultivation') == []
    country = Country('Niger', preload_panel_ids=False)
    assert 'cultivation_records' in country.data_scheme
    assert 'plot_cultivation' not in country.data_scheme
    with pytest.raises(AttributeError):
        getattr(country, 'plot_cultivation')
