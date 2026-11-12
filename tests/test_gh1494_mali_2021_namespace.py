"""Ruled2021 raw namespace: invented operands, actual pure helpers and walk.

No Country/Wave construction, source read, committed-map deserialization or
producer execution. The generator loader executes only its pure decision
statements against invented covers, excluding getters and JSON writers.
"""
import ast
from copy import deepcopy
import importlib.util
from pathlib import Path
from types import SimpleNamespace
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.local_tools import format_id, id_walk, RecursiveDict, IdWalkError

ROOT = Path(__file__).resolve().parents[1]
MALI = ROOT / 'lsms_library/countries/Mali'


@pytest.fixture(scope='module')
def pure():
    path = MALI / '_/mali.py'
    names = {'i', 'pid', 'eaci14_i', 'eaci17_i', 'ehcvm21_i',
             'ehcvm_household_ids', 'ehcvm_dispositions', 'plot_features_for_wave',
             '_map_codes', '_ehcvm_keyed'}
    constants = {'EACI14_PREFIX', 'EACI17_PREFIX', 'EHCVM21_PREFIX', '_DISPOSITIONS'}
    nodes = []
    for node in ast.parse(path.read_text()).body:
        if isinstance(node, ast.FunctionDef) and node.name in names:
            nodes.append(node)
        elif isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id in constants for t in node.targets):
            nodes.append(node)
    env = {'pd': pd, 'np': np, 'tools': SimpleNamespace(format_id=format_id),
           '_harmonized_codes': lambda name: {1: 'invented'},
           'ehcvm_crop_labels': lambda s: s.astype('string'),
           'ehcvm_unit_labels': lambda s: s.astype('string')}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), env)
    assert names <= env.keys()
    return env


def source(g=(12, 13), h=(3, 4)):
    out = pd.DataFrame({'grappe': pd.Series(g, dtype='int64'),
                        'menage': pd.Series(h, dtype='int64'),
                        'Quantity': [0.25] * len(g)})
    out.attrs = {'native': {'invented': True}}
    return out


def walk_frame(ids):
    out = pd.DataFrame({'t': ['2021-22'] * len(ids), 'i': ids,
                        'Quantity': np.arange(len(ids), dtype=float) + .25,
                        'raw_detail': pd.array(['invented'] * len(ids), dtype='string')})
    return out.set_index(['t', 'i'])


def maps():
    return {'2021-22': {'EHCVM21_A': 'A', 'EHCVM21_B': 'C'}}


def test_01_all_flag_classes_share_raw_namespace(pure):
    frame = source(g=(12,) * 5, h=(1, 2, 3, 4, 5))
    frame['PanelHH'] = ['Oui', 'Non', None, pd.NA, 'unrecognized']
    before = deepcopy(frame)
    assert pure['ehcvm_household_ids'](frame, wave='2021-22').tolist() == [
        f'EHCVM21_1200{i}' for i in range(1, 6)]
    pd.testing.assert_frame_equal(frame, before)
    assert frame.attrs == before.attrs


def test_02_column_scalars_protect_large_integer_from_quantity(pure):
    number = 2 ** 53 + 1
    frame = source(g=(number,), h=(3,))
    before = deepcopy(frame)
    assert pure['ehcvm_household_ids'](frame, wave='2021-22').iloc[0] == f'EHCVM21_{number}003'
    pd.testing.assert_frame_equal(frame, before)


def test_03_2018_default_and_explicit_are_unchanged(pure):
    frame = source()
    expected = pd.Series(['12003', '13004'])
    pd.testing.assert_series_equal(pure['ehcvm_household_ids'](frame), expected)
    pd.testing.assert_series_equal(pure['ehcvm_household_ids'](frame, wave='2018-19'), expected)


def test_04_existing_eaci_namespaces_unchanged(pure):
    pair = pd.Series([12, 3])
    assert pure['eaci14_i'](pair) == 'EACI14_12003'
    assert pure['eaci17_i'](pair) == 'EACI17_12003'
    assert pure['i'](pair) == '12003'


def test_05_person_id_is_unchanged(pure):
    assert pure['pid'](pd.Series([12, 3, 4])) == '12003004'


def test_06_none_raw_component_keeps_scalar_refusal(pure):
    pair = pd.Series([None, 3], dtype=object)
    with pytest.raises(TypeError):
        pure['i'](pair)
    with pytest.raises(TypeError):
        pure['ehcvm21_i'](pair)


def test_07_unknown_wave_refuses(pure):
    with pytest.raises(ValueError, match='household IDs require'):
        pure['ehcvm_household_ids'](source(), wave='not-a-Mali-EHCVM-wave')


def test_08_empty_2021_keeps_empty_native_index(pure):
    frame = source(g=(), h=())
    frame.index = pd.Index([], dtype='int64', name='native_row')
    before = deepcopy(frame)
    out = pure['ehcvm_household_ids'](frame, wave='2021-22')
    assert isinstance(out, pd.Series) and len(out) == 0
    pd.testing.assert_index_equal(out.index, frame.index)
    pd.testing.assert_frame_equal(frame, before)


def test_09_duplicate_native_rows_remain_rows(pure):
    frame = source(g=(12, 12), h=(3, 3))
    frame.index = pd.Index([7, 7], name='native_row')
    before = deepcopy(frame)
    out = pure['ehcvm_household_ids'](frame, wave='2021-22')
    assert out.tolist() == ['EHCVM21_12003'] * 2
    pd.testing.assert_index_equal(out.index, frame.index)
    pd.testing.assert_frame_equal(frame, before)


def test_10_approved_links_only_map_back():
    frame = walk_frame(['EHCVM21_A', 'EHCVM21_B', 'EHCVM21_non',
                        'EHCVM21_unknown', 'EHCVM21_orphan'])
    out = id_walk(frame.copy(), maps(), state='raw')
    assert out.index.get_level_values('i').tolist() == ['A', 'C', 'EHCVM21_non',
                                                       'EHCVM21_unknown', 'EHCVM21_orphan']
    pd.testing.assert_frame_equal(out.reset_index(drop=True), frame.reset_index(drop=True))


def test_11_second_walk_without_attrs_is_exact():
    first = id_walk(walk_frame(['EHCVM21_A', 'EHCVM21_non']), maps(), state='raw')
    again = first.copy(); again.attrs = {}
    again = id_walk(again, maps())
    pd.testing.assert_frame_equal(again, first)
    assert again.attrs == first.attrs


def test_12_mixed_raw_and_canonical_still_refuses():
    with pytest.raises(IdWalkError, match='mixed'):
        id_walk(walk_frame(['EHCVM21_A', 'C']), maps())


def test_13_empty_and_index_only_walks_keep_shape():
    empty = walk_frame([])
    out = id_walk(empty.copy(), maps(), state='raw')
    pd.testing.assert_frame_equal(out, empty)
    spine = walk_frame(['EHCVM21_A']).iloc[:, :0]
    out = id_walk(spine.copy(), maps(), state='raw')
    assert out.shape == (1, 0) and out.index.get_level_values('i').tolist() == ['A']


def test_14_real_sample_v_join_matches_canonical_keys():
    node = next(n for n in ast.parse((ROOT / 'lsms_library/country.py').read_text()).body
                if isinstance(n, ast.ClassDef) and n.name == 'Country')
    function = next(n for n in node.body if isinstance(n, ast.FunctionDef) and n.name == '_join_v_from_sample')
    function.decorator_list = []
    env = {'pd': pd, 'warnings': warnings, 'format_id': format_id}
    exec(compile(ast.Module(body=[function], type_ignores=[]), 'actual_v_join.py', 'exec'), env)
    roster = id_walk(walk_frame(['EHCVM21_A', 'EHCVM21_non']), maps(), state='raw')
    sample = id_walk(pd.DataFrame({'t': ['2021-22'] * 2, 'i': ['EHCVM21_A', 'EHCVM21_non'],
                                   'v': ['v-invented1', 'v-invented2']}).set_index(['t', 'i']), maps(), state='raw')
    obj = SimpleNamespace(_sample_v_cache=sample[['v']], name='Mali')
    out = env['_join_v_from_sample'](obj, roster)
    assert out.index.get_level_values('v').tolist() == ['v-invented1', 'v-invented2']
    assert out.Quantity.tolist() == roster.Quantity.tolist()


def test_15_plot_and_raw_derivation_helpers_agree(pure):
    env = dict(pure)
    frame = source(g=(12,), h=(3,))
    fields = ['field_no', 'parcel_no', 'area_gps', 'gps_measured', 'area_est', 'area_est_unit']
    for name, value in zip(fields, [1, 2, 1.25, 1, 1.1, 1]):
        frame[name] = value
    colmap = {name: name for name in ['grappe', 'menage', *fields,
              'tenure', 'tenure_system', 'soil_type', 'water_source', 'topography', 'soil_fertility']}
    before = deepcopy(frame)
    out = env['plot_features_for_wave']('2021-22', frame, colmap)
    assert out.index.get_level_values('i').tolist() == ['EHCVM21_12003']
    assert out.index.get_level_values('plot_id').tolist() == ['1_2']
    assert out.Area.iloc[0] == 1.25
    # Execute the real raw-input function against only this invented in-memory reader.
    env['tools'] = SimpleNamespace(format_id=format_id, get_dataframe=lambda *a, **k: frame.copy())
    env['_ehcvm_crop_map'] = lambda wave: {'Rice': 'Rice'}
    frame['crop_code'] = 'Rice'
    raw_func = env['_ehcvm_keyed']
    original = raw_func.__globals__['tools']; crop_original = raw_func.__globals__.get('_ehcvm_crop_map')
    raw_func.__globals__['tools'] = env['tools']; raw_func.__globals__['_ehcvm_crop_map'] = env['_ehcvm_crop_map']
    try:
        raw = raw_func('2021-22', 'invented.dta', 'crop_code')
    finally:
        raw_func.__globals__['tools'] = original
        if crop_original is None:
            raw_func.__globals__.pop('_ehcvm_crop_map', None)
        else:
            raw_func.__globals__['_ehcvm_crop_map'] = crop_original
    assert raw.i.tolist() == out.index.get_level_values('i').tolist()
    pd.testing.assert_frame_equal(raw[frame.columns], frame)
    assert raw.attrs == frame.attrs
    pd.testing.assert_frame_equal(frame.drop(columns='crop_code'), before)


def test_16_disposition_namespace_changes_no_operand(pure):
    frame = source()
    frame['s16dq01'] = ['Rice', 'Maize']
    for name in pure['_DISPOSITIONS']:
        frame[name + 'a'] = [2.5, np.nan]
        frame[name + 'b'] = ['Kg', 'Litre']
    before = deepcopy(frame)
    old = pure['ehcvm_dispositions'](frame)
    new = pure['ehcvm_dispositions'](frame, wave='2021-22')
    new['i'] = new['i'].str.removeprefix('EHCVM21_')
    pd.testing.assert_frame_equal(new, old)
    pd.testing.assert_frame_equal(frame, before)


def generator(cover18, cover21, pure):
    tree = ast.parse((MALI / '_/panel_ids.py').read_text())
    selected = []
    names = {'baseline_ids', 'panel_mask', 'panel_21', 'candidates', 'cur_set',
             'ehcvm_21_to_18', 'skipped_chains', 'updated_ids', 'recursive_D'}
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == '_canonical_i':
            selected.append(node)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(isinstance(t, ast.Name) and t.id in names for t in targets):
                selected.append(node)
        elif isinstance(node, ast.For):
            selected.append(node)
    env = {'pd': pd, 'mali_i': pure['i'], 'EHCVM21_PREFIX': pure['EHCVM21_PREFIX'],
           'RecursiveDict': RecursiveDict, 'cover_18': cover18, 'cover_21': cover21}
    exec(compile(ast.Module(body=selected, type_ignores=[]), 'actual_panel_decisions.py', 'exec'), env)
    return env


def test_17_generator_keeps_bare_occupancy_and_approved_membership(pure):
    baseline = pd.DataFrame({'grappe': [1, 2, 3, 5], 'menage': [1, 1, 1, 1]})
    cover = pd.DataFrame({'grappe': [1, 4, 3, 5, 6, 7], 'menage': [1] * 6,
                          'PanelHH': ['Oui', 'Oui', 'Non', 'Oui', 'Oui', None],
                          's00q07f1': [1, 3, np.nan, np.nan, np.nan, np.nan],
                          's00q07f2': [1, 1, np.nan, np.nan, np.nan, np.nan]})
    before = deepcopy(cover)
    got = generator(baseline, cover, pure)
    assert got['skipped_chains'] == 1  # moved target held by Non: old omission retained
    assert got['ehcvm_21_to_18'] == {'1001': '1001', '5001': '5001'}
    assert got['updated_ids']['2021-22'] == {'EHCVM21_1001': '1001', 'EHCVM21_5001': '5001'}
    assert len(got['recursive_D'].data) == 2
    pd.testing.assert_frame_equal(cover, before)
    # The old overconservative chain omission also survives: prefixing output
    # must not turn a bare-coordinate occupancy test into an always-false one.
    chain = pd.DataFrame({'grappe': [1, 2], 'menage': [1, 1],
                          'PanelHH': ['Oui', 'Oui'],
                          's00q07f1': [2, 3], 's00q07f2': [1, 1]})
    got = generator(baseline, chain, pure)
    assert got['skipped_chains'] == 1
    assert got['ehcvm_21_to_18'] == {'2001': '3001'}
    assert got['updated_ids']['2021-22'] == {'EHCVM21_2001': '3001'}


def test_18_every_2021_script_and_yaml_binding_is_explicit(pure):
    path = MALI / '2021-22/_/mapping.py'
    spec = importlib.util.spec_from_file_location('_mali1494_current_mapping', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.i(pd.Series([12, 3])) == 'EHCVM21_12003'
    assert Path(module._ehcvm21_i.__code__.co_filename).resolve() == MALI / '_/mali.py'
    crop = ast.parse((MALI / '2021-22/_/crop_production.py').read_text())
    calls = [n for n in ast.walk(crop) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
             and n.func.id in {'ehcvm_household_ids', 'ehcvm_dispositions'}]
    assert len(calls) == 3
    assert all(any(k.arg == 'wave' and isinstance(k.value, ast.Constant) and k.value.value == '2021-22'
                   for k in n.keywords) for n in calls)
    assert 'ehcvm_household_ids(src, wave=wave)' in (MALI / '_/mali.py').read_text()
