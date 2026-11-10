"""Frozen source-free controls for the nonactivated own Mali s16b adapter.

UNRUN by the source-only author. Coordinator selection is exactly this file,
--noconftest, -n 2 --dist=loadfile. No LSMS import, Country, provider or writer.
Expected cards/frames are stated independently of adapter constants/helpers.
Only current production household/format_id definitions are AST-extracted.
"""
import ast
from copy import deepcopy
import importlib.util
import os
from pathlib import Path
import socket
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest


ROOT = Path(__file__).resolve().parents[1]
COUNTRY = ROOT / 'lsms_library/countries/Mali'
ADAPTER = COUNTRY / '_/s16b_header_adapter.py'
COMMON = ('s16bq01', 's16bq02', 's16bq03a', 's16bq03b', 's16bq04',
          's16bq05', 's16bq06', 's16bq07a', 's16bq07b', 's16bq08',
          's16bq09a', 's16bq09b', 's16bq09c')
INVENTORIES = {
    '2018-19': ('vague', 'grappe', 'menage', 's16aq09a', 's16aq09b')
               + COMMON + ('s16bq00',),
    '2021-22': ('grappe', 'menage', 'vague') + COMMON[:7]
               + ('s16bq06_autre',) + COMMON[7:],
}
MENU18 = [
    "Engrais organiques - déchets d'animaux",
    'Engrais organiques - Ordures ménagères',
    'Engrais inorganiques - Urée', 'Engrais inorganiques - Phosphates',
    'Engrais inorganiques - NPK/Formule unique',
    'DAP et autres engrais inorganiques',
    'Produits phytosanitaires - Pesticides',
    'Produits phytosanitaires - Herbicides',
    'Produits phytosanitaires -Fongicides', 'Autres produits phytosanitaires',
    'Semences de petit mil', 'Semences de sorgho', 'Semences de maïs',
    'Semences de riz', "Semences d'autres céréales", 'Semences de coton',
    'Semences de césame', 'Semences de haricots/niébé',
    'Plants/boutures de tubercules', 'Autres semences',
]
MENU21 = MENU18[:18] + ["Semences d'arichide", 'Plants/boutures de tubercules',
                          "Plants d'hévéa, de teck", 'Autres semences']


class AttrRecord(dict):
    """Typed synthetic metadata; equality alone must not hide a type change."""


def _extract(path, names, namespace):
    """Execute selected definitions only; never execute provider-capable imports."""
    tree = ast.parse(path.read_text(encoding='utf-8'))
    selected = [node for node in tree.body
                if isinstance(node, ast.FunctionDef) and node.name in names]
    assert {node.name for node in selected} == set(names)
    module = ast.Module(body=selected, type_ignores=[])
    exec(compile(ast.fix_missing_locations(module), str(path), 'exec'), namespace)
    return namespace


@pytest.fixture
def adapter(monkeypatch):
    # Coordinator must provide exact source/config identity before collection.
    assert Path.cwd().resolve() == ROOT
    assert Path(os.environ['PYTHONPATH']).resolve() == ROOT
    assert Path(os.environ['LSMS_COUNTRIES_ROOT']).resolve() == COUNTRY.parent

    def forbidden(*args, **kwargs):
        raise AssertionError('pure Mali controls attempted provider/writer/network access')

    for name in ('read_stata', 'read_parquet', 'read_csv', 'read_excel', 'read_spss'):
        monkeypatch.setattr(pd, name, forbidden)
    for name in ('to_parquet', 'to_csv', 'to_excel', 'to_pickle'):
        monkeypatch.setattr(pd.DataFrame, name, forbidden)
    monkeypatch.setattr(socket.socket, 'connect', forbidden)
    monkeypatch.setattr(socket, 'create_connection', forbidden)
    monkeypatch.setattr(socket, 'getaddrinfo', forbidden)
    # Imports admitted by the adapter are exactly pandas and deepcopy.
    tree = ast.parse(ADAPTER.read_text(encoding='utf-8'))
    imports = [node for node in ast.walk(tree)
               if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert len(imports) == 2
    assert isinstance(imports[0], ast.ImportFrom)
    assert imports[0].module == 'copy'
    assert [alias.name for alias in imports[0].names] == ['deepcopy']
    assert isinstance(imports[1], ast.Import)
    assert [(alias.name, alias.asname) for alias in imports[1].names] == [('pandas', 'pd')]
    calls = {node.func.id for node in ast.walk(tree)
             if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)}
    assert calls <= {'ValueError', 'set', 'sorted', 'any', 'isinstance',
                     'household_id', 'deepcopy', '_card_alias'}
    spec = importlib.util.spec_from_file_location('mali_1483_pure_adapter', ADAPTER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fmt = _extract(ROOT / 'lsms_library/local_tools.py', ['format_id'],
                   {'pd': pd, 'Any': Any})['format_id']
    # The real household functions receive NO get_dataframe / provider namespace.
    ns = _extract(COUNTRY / '_/mali.py', ['i', 'ehcvm21_i'],
                  {'tools': SimpleNamespace(format_id=fmt), 'EHCVM21_PREFIX': 'EHCVM21_'})
    assert vars(ns['tools']) == {'format_id': fmt}
    return module, {'2018-19': ns['i'], '2021-22': ns['ehcvm21_i']}


def native(wave):
    """Five invented lines, including No/unknown/gift-only/purchase-only."""
    data = {
        'grappe': np.array([12] * 5, dtype='int16'),
        'menage': np.array([3] * 5, dtype='int8'),
        'vague': np.array([1] * 5, dtype='int8'),
        's16bq01': np.array([3, 5, 6, 19, 77], dtype='int8'),
        's16bq02': pd.array([1, 2, pd.NA, 9, 1], dtype='Float64'),
        's16bq03a': np.array([1.25, 0, 99, 9999, np.nan], dtype='float64'),
        's16bq03b': np.array([1, 2, 3, 4, 5], dtype='int8'),
        's16bq04': np.array([1] * 5, dtype='int8'),
        's16bq05': pd.array([2, 1, pd.NA, 9, 1], dtype='Float64'),
        's16bq06': np.array([1] * 5, dtype='int8'),
        's16bq07a': np.array([np.nan, 2.5, 0, 9999, 1.0],
                               dtype='float32' if wave == '2021-22' else 'float64'),
        's16bq07b': pd.array([5, 4, 3, pd.NA, 77], dtype='Float64'),
        's16bq08': pd.array([1, 2, 9, pd.NA, 1], dtype='Float64'),
        's16bq09a': np.array([7.0, np.nan, 99, 0, 4.5], dtype='float64'),
        's16bq09b': pd.array([2, 1, pd.NA, 5, 77], dtype='Float64'),
        's16bq09c': pd.array([350, pd.NA, 99, 0, -3], dtype='Int32'),
    }
    if wave == '2018-19':
        data.update(s16aq09a=np.array([0.25] * 5, dtype='float64'),
                    s16aq09b=np.array([1] * 5, dtype='int8'),
                    s16bq00=np.array([2] * 5, dtype='int8'))
    else:
        data['s16bq06_autre'] = pd.array(['Literal donor', pd.NA, '', 'x', 'y'],
                                       dtype='string')
    frame = pd.DataFrame(data)[list(INVENTORIES[wave])]
    frame.index = pd.Index([8, 8, 2, 9, 1], name='not_a_native_key')
    frame.attrs = {'record': AttrRecord(campaign=('native', wave), count=np.int16(2))}
    return frame


@pytest.mark.parametrize('wave', ['2018-19', '2021-22'])
def test_exact_independent_alias_frame_keys_dtypes_masks_and_typed_attrs(adapter, wave):
    module, formatters = adapter
    raw = native(wave)
    before = raw.copy(deep=True)
    attrs = deepcopy(raw.attrs)
    got = module.s16b_rows_for_wave(raw, wave, household_id=formatters[wave])
    expected = pd.DataFrame({
        'Quantity': np.array([1.25, 0, 99, 9999, np.nan], dtype='float64'),
        'Quantity_received_as_gift': np.array([np.nan, 2.5, 0, 9999, 1.0],
            dtype='float32' if wave == '2021-22' else 'float64'),
        'Quantity_purchased': np.array([7.0, np.nan, 99, 0, 4.5], dtype='float64'),
        'Input_native': pd.array(['Engrais inorganiques - Urée',
            'Engrais inorganiques - NPK/Formule unique',
            'DAP et autres engrais inorganiques',
            'Plants/boutures de tubercules' if wave == '2018-19'
            else "Semences d'arichide", pd.NA], dtype='string'),
        'Input_native_unlisted': [False, False, False, False, True],
        'Used': pd.array([True, False, pd.NA, pd.NA, True], dtype='boolean'),
        'Used_unlisted': [False, False, False, True, False],
        'u': pd.array(['Gramme', 'Kilogramme', 'Tonne', 'Litre', 'Charrette'], dtype='string'),
        'u_unlisted': [False] * 5,
        'Received_as_gift': pd.array([False, True, pd.NA, pd.NA, True], dtype='boolean'),
        'Received_as_gift_unlisted': [False, False, False, True, False],
        'Unit_received_as_gift': pd.array(['Charrette', 'Litre', 'Tonne', pd.NA, pd.NA], dtype='string'),
        'Unit_received_as_gift_unlisted': [False, False, False, False, True],
        'Purchased': pd.array([True, False, pd.NA, pd.NA, True], dtype='boolean'),
        'Purchased_unlisted': [False, False, True, False, False],
        'Unit_purchased': pd.array(['Kilogramme', 'Gramme', pd.NA, 'Charrette', pd.NA], dtype='string'),
        'Unit_purchased_unlisted': [False, False, False, False, True],
    })
    i = '12003' if wave == '2018-19' else 'EHCVM21_12003'
    index = pd.MultiIndex.from_arrays([
        pd.array([wave] * 5, dtype='string'), pd.array([i] * 5, dtype='string'),
        np.array([3, 5, 6, 19, 77], dtype='int8')], names=['t', 'i', 's16bq01'])
    expected.index = index
    pd.testing.assert_frame_equal(got[list(expected)], expected, check_exact=True)
    # All native payload, extra fields, order, masks and exact NumPy values survive.
    pd.testing.assert_frame_equal(got[list(raw)].reset_index(drop=True),
                                  raw.reset_index(drop=True), check_exact=True)
    pd.testing.assert_frame_equal(raw, before, check_exact=True)
    for field in ('s16bq03a', 's16bq07a', 's16bq09a'):
        mask = raw[field].notna().to_numpy()
        np.testing.assert_array_equal(got[field].to_numpy()[mask], raw[field].to_numpy()[mask])
    assert got.index.is_unique and got.index.equals(index)
    assert got.attrs == attrs
    assert type(got.attrs['record']) is AttrRecord
    assert type(got.attrs['record']['campaign']) is tuple
    assert type(got.attrs['record']['count']) is np.int16
    got.attrs['record']['campaign'] = ('changed',)
    assert raw.attrs == attrs


@pytest.mark.parametrize('wave,menu', [('2018-19', MENU18), ('2021-22', MENU21)])
def test_every_own_menu_slot_survives_without_crop_grade_or_dap_recode(adapter, wave, menu):
    module, formatters = adapter
    raw = pd.concat([native(wave).iloc[[0]].copy() for _ in menu], ignore_index=True)
    raw['s16bq01'] = np.arange(1, len(menu) + 1, dtype='int8')
    got = module.s16b_rows_for_wave(raw, wave, household_id=formatters[wave])
    assert got['Input_native'].tolist() == menu
    assert not got['Input_native_unlisted'].any()
    assert got.index.get_level_values('s16bq01').tolist() == list(range(1, len(menu) + 1))
    assert not {'plot_id', 'crop', 'input', 'nitrogen_kg', 'Quantity_kg', 'Applied'} & set(got)


def test_use_classes_partition_does_not_select_rows_or_treat_unknown_as_no(adapter):
    module, formatters = adapter
    raw = native('2021-22')
    got = module.s16b_use_classes(raw)
    expected = pd.DataFrame({'yes': [True, False, False, False, True],
        'no': [False, True, False, False, False],
        'unanswered': [False, False, True, False, False],
        'unlisted': [False, False, False, True, False]}, index=raw.index)
    pd.testing.assert_frame_equal(got, expected, check_exact=True)
    assert got.sum(axis=1).tolist() == [1] * 5
    out = module.s16b_rows_for_wave(raw, '2021-22', household_id=formatters['2021-22'])
    assert len(out) == 5  # No, unanswered, gift/purchase-only all retained for REVIEW.
    assert out['s16bq09c'].tolist() == [350, pd.NA, 99, 0, -3]


@pytest.mark.parametrize('wave', ['2018-19', '2021-22'])
def test_all_missing_amounts_known_yes_and_empty_frame_are_not_filled_or_dropped(adapter, wave):
    module, formatters = adapter
    raw = native(wave)
    for name in ('s16bq03a', 's16bq07a', 's16bq09a'):
        raw[name] = np.nan
    got = module.s16b_rows_for_wave(raw, wave, household_id=formatters[wave])
    assert len(got) == 5
    assert got[['Quantity', 'Quantity_purchased', 'Quantity_received_as_gift']].isna().all().all()
    empty = module.s16b_rows_for_wave(raw.iloc[:0], wave, household_id=formatters[wave])
    pd.testing.assert_frame_equal(empty.reset_index(drop=True),
                                  got.iloc[:0].reset_index(drop=True), check_exact=True)
    assert empty.index.names == ['t', 'i', 's16bq01']
    assert [level.dtype for level in empty.index.levels] == [
        level.dtype for level in got.index.levels]
    assert empty.attrs == got.attrs


@pytest.mark.parametrize('column', ['grappe', 'menage', 's16bq01'])
@pytest.mark.parametrize('bad', [pd.NA, 0, -999999999, 1.5, np.inf])
def test_malformed_native_key_refuses_without_modifying_input(adapter, column, bad):
    module, formatters = adapter
    raw = native('2018-19')
    raw[column] = raw[column].astype('Float64')
    raw.iloc[0, raw.columns.get_loc(column)] = bad
    before = raw.copy(deep=True)
    with pytest.raises(ValueError, match='positive integer keys'):
        module.s16b_rows_for_wave(raw, '2018-19', household_id=formatters['2018-19'])
    pd.testing.assert_frame_equal(raw, before, check_exact=True)


def test_duplicate_slot_refuses_even_for_identical_payload_or_different_vague(adapter):
    module, formatters = adapter
    raw = pd.concat([native('2018-19').iloc[[0]]] * 2, ignore_index=True)
    for different_vague in (False, True):
        raw.loc[1, 'vague'] = 2 if different_vague else 1
        with pytest.raises(ValueError, match='repeats a native'):
            module.s16b_rows_for_wave(raw, '2018-19', household_id=formatters['2018-19'])


@pytest.mark.parametrize('column', ['s16bq03b', 's16bq07b', 's16bq09b'])
def test_unlisted_unit_remains_raw_and_missing_unit_is_distinct(adapter, column):
    module, formatters = adapter
    raw = native('2018-19')
    raw[column] = pd.array([77.5, pd.NA, 1, 2, 3], dtype='Float64')
    got = module.s16b_rows_for_wave(raw, '2018-19', household_id=formatters['2018-19'])
    alias = {'s16bq03b': 'u', 's16bq07b': 'Unit_received_as_gift',
             's16bq09b': 'Unit_purchased'}[column]
    assert got[alias].isna().tolist() == [True, True, False, False, False]
    assert got[alias + '_unlisted'].tolist() == [True, False, False, False, False]
    assert got[column].iloc[0] == 77.5 and pd.isna(got[column].iloc[1])


def test_exact_source_shape_and_namespace_refusals(adapter):
    module, formatters = adapter
    raw = native('2021-22')
    for bad, text in [(raw.drop(columns='s16bq09c'), 'missing required'),
                      (raw.assign(s16bq02='Oui'), 'numeric native'),
                      (raw.assign(s16bq02=True), 'numeric native'),
                      (raw.assign(Quantity=1), 'collides with adapter')]:
        with pytest.raises(ValueError, match=text):
            module.s16b_rows_for_wave(bad, '2021-22', household_id=formatters['2021-22'])
    with pytest.raises(ValueError, match='unique source column'):
        module.s16b_rows_for_wave(pd.concat([raw, raw[['s16bq02']]], axis=1),
                                  '2021-22', household_id=formatters['2021-22'])
    with pytest.raises(ValueError, match='only 2018-19 and 2021-22'):
        module.s16b_rows_for_wave(raw, '2017-18', household_id=formatters['2018-19'])
    with pytest.raises(ValueError, match='EHCVM21_ namespace'):
        module.s16b_rows_for_wave(raw, '2021-22', household_id=formatters['2018-19'])
    with pytest.raises(ValueError, match='bare numeric IDs'):
        module.s16b_rows_for_wave(native('2018-19'), '2018-19', household_id=formatters['2021-22'])
    with pytest.raises(ValueError, match='nonempty strings'):
        module.s16b_rows_for_wave(raw, '2021-22', household_id=lambda value: None)


def test_formatted_identity_collision_refuses_and_source_sentinels_are_not_masked(adapter):
    module, formatters = adapter
    raw = native('2021-22').iloc[[0, 0]].copy()
    raw['menage'] = np.array([3, 4], dtype='int8')
    with pytest.raises(ValueError, match='formatted household/input-slot keys collide'):
        module.s16b_rows_for_wave(raw, '2021-22', household_id=lambda value: 'EHCVM21_1')
    raw = native('2018-19')
    raw['s16bq03a'] = np.array([99, 999, 9999, 99999, -2], dtype='float64')
    got = module.s16b_rows_for_wave(raw, '2018-19', household_id=formatters['2018-19'])
    assert got['Quantity'].tolist() == [99, 999, 9999, 99999, -2]


def test_header_provenance_is_own_and_adapter_has_no_automatic_entrypoint(adapter):
    module, _ = adapter
    assert module.SOURCE_BINDINGS['2018-19']['header_sha256'] == (
        '701c60fe2b2e988d116da23d92d8cc5e260592a4d292ecfb34e7e0c870ee5eb5')
    assert module.SOURCE_BINDINGS['2021-22']['header_sha256'] == (
        '3f467ae181bb4d3f1d537c535ce2cf94d8cf8bc8cfe1ded5067adcf6577aca35')
    for wave in INVENTORIES:
        assert module.SOURCE_BINDINGS[wave]['columns'] == INVENTORIES[wave]
        assert not (COUNTRY / wave / '_/plot_inputs.py').exists()
    # Merely adding helper source does not declare a wave or execute a producer.
    for wave in INVENTORIES:
        text = (COUNTRY / wave / '_/data_info.yml').read_text(encoding='utf-8')
        assert '\nplot_inputs:' not in text
