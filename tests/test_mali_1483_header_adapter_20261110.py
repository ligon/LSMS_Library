"""Frozen source-free controls for own Mali s16b adapters and driver wiring.

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
import re
import socket
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml


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
    assert Path(os.environ['PYTHONPATH'].split(os.pathsep)[0]).resolve() == ROOT
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
                     'household_id', 'deepcopy', '_card_alias', 's16b_rows_for_wave',
                     's16b_applied_candidate'}
    spec = importlib.util.spec_from_file_location('mali_1483_pure_adapter', ADAPTER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fmt = _extract(ROOT / 'lsms_library/local_tools.py', ['format_id'],
                   {'pd': pd, 'Any': Any})['format_id']
    # The real household functions receive NO get_dataframe / provider namespace.
    country_tree = ast.parse((COUNTRY / '_/mali.py').read_text(encoding='utf-8'))
    prefix = next(ast.literal_eval(node.value) for node in country_tree.body
                  if isinstance(node, ast.Assign)
                  and any(isinstance(target, ast.Name) and target.id == 'EHCVM21_PREFIX'
                          for target in node.targets))
    assert prefix == 'EHCVM21_'
    ns = _extract(COUNTRY / '_/mali.py', ['i', 'ehcvm21_i'],
                  {'tools': SimpleNamespace(format_id=fmt), 'EHCVM21_PREFIX': prefix})
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
def test_applied_candidate_uses_only_native_yes_and_has_no_amount_or_identity_drop(adapter, wave):
    module, formatters = adapter
    raw = native(wave)
    got = module.s16b_applied_candidate(raw, wave, household_id=formatters[wave])
    i = '12003' if wave == '2018-19' else 'EHCVM21_12003'
    assert got.index.tolist() == [(wave, i, 3), (wave, i, 77)]
    # Yes with no used quantity and unlisted input slot survives this projection.
    assert got['Quantity'].isna().tolist() == [False, True]
    assert got['Quantity_purchased'].tolist() == [7., 4.5]
    assert got['Used'].tolist() == [True, True]
    pd.testing.assert_frame_equal(got[list(raw)].reset_index(drop=True),
                                  raw.iloc[[0, 4]].reset_index(drop=True), check_exact=True)
    assert got.attrs == raw.attrs and type(got.attrs['record']) is AttrRecord
    raw['s16bq02'] = pd.array([2, 2, pd.NA, 9, 2], dtype='Float64')
    assert module.s16b_applied_candidate(raw, wave, household_id=formatters[wave]).empty
    # The unfiltered inverse/raw route still carries all five answers and amounts.
    assert len(module.s16b_rows_for_wave(raw, wave, household_id=formatters[wave])) == 5


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


def test_header_provenance_and_exact_country_wiring(adapter):
    module, _ = adapter
    assert module.SOURCE_BINDINGS['2018-19']['header_sha256'] == (
        '701c60fe2b2e988d116da23d92d8cc5e260592a4d292ecfb34e7e0c870ee5eb5')
    assert module.SOURCE_BINDINGS['2021-22']['header_sha256'] == (
        '3f467ae181bb4d3f1d537c535ce2cf94d8cf8bc8cfe1ded5067adcf6577aca35')
    for wave in INVENTORIES:
        assert module.SOURCE_BINDINGS[wave]['columns'] == INVENTORIES[wave]
        assert (COUNTRY / wave / '_/plot_inputs.py').is_file()
        text = (COUNTRY / wave / '_/data_info.yml').read_text(encoding='utf-8')
        assert text.count('\nplot_inputs: !make\n') == 1
    scheme = yaml.safe_load((COUNTRY / '_/data_scheme.yml').read_text())['Data Scheme']['plot_inputs']
    assert scheme['index'] == '(t, i, plot_id, input, crop)'
    for name, kind in [('Quantity', 'float'), ('u', 'str'), ('Purchased', 'bool'),
                       ('Quantity_purchased', 'float'), ('Improved', 'bool'), ('Applied', 'bool')]:
        assert scheme[name] == kind
    for name, kind in [('Unit_purchased', 'str'), ('Received_as_gift', 'bool'),
                       ('Quantity_received_as_gift', 'float'), ('Unit_received_as_gift', 'str'),
                       ('Input_native', 'str'), ('input_code_native', 'int')]:
        assert scheme[name] == {'type': kind, 'optional': True}
    make = (COUNTRY / '_/Makefile').read_text()
    rule = next(line for line in make.splitlines()
                if line.startswith('$(VAR_DIR)/plot_inputs.parquet:'))
    for wave in ['2014-15', '2017-18', '2018-19', '2021-22']:
        assert '../' + wave + '/_/plot_inputs.parquet' in rule


def public_native(wave):
    raw = native(wave)
    raw['s16bq01'] = np.array([3, 5, 6, 19, 11], dtype='int8')
    raw['s16bq07b'] = pd.array([5, 4, 3, pd.NA, 5], dtype='Float64')
    raw['s16bq09b'] = pd.array([2, 1, pd.NA, 5, 5], dtype='Float64')
    return raw


@pytest.mark.parametrize('wave', ['2018-19', '2021-22'])
def test_exact_public_frame_preserves_yes_missing_quantity_and_own_acquisition_units(adapter, wave):
    module, formatters = adapter
    raw = public_native(wave)
    got = module.s16b_plot_inputs_for_wave(raw, wave, household_id=formatters[wave])
    i = '12003' if wave == '2018-19' else 'EHCVM21_12003'
    index = pd.MultiIndex.from_arrays([
        pd.array([wave, wave], dtype='string'), pd.array([i, i], dtype='string'),
        pd.array(['Farm-level', 'Farm-level'], dtype='string'),
        pd.array(['Urea', 'Seed'], dtype='string'),
        pd.array(['(not crop-specific)', 'Mil'], dtype='string')],
        names=['t', 'i', 'plot_id', 'input', 'crop'])
    expected = pd.DataFrame({
        'Quantity': [1.25, np.nan],
        'u': pd.array(['Gramme', 'Charrette'], dtype='string'),
        'Purchased': pd.array([True, True], dtype='boolean'),
        'Quantity_purchased': [7., 4.5],
        'Improved': pd.array([pd.NA, pd.NA], dtype='boolean'),
        'Applied': pd.array([True, True], dtype='boolean'),
        'Unit_purchased': pd.array(['Kilogramme', 'Charrette'], dtype='string'),
        'Received_as_gift': pd.array([False, True], dtype='boolean'),
        'Quantity_received_as_gift': [np.nan, 1.],
        'Unit_received_as_gift': pd.array(['Charrette', 'Charrette'], dtype='string'),
        'Input_native': pd.array(['Engrais inorganiques - Urée', 'Semences de petit mil'], dtype='string'),
        'input_code_native': pd.array([3, 11], dtype='Int64'),
    }, index=index)
    pd.testing.assert_frame_equal(got, expected, check_exact=True)
    assert got.attrs == raw.attrs and type(got.attrs['record']) is AttrRecord
    assert len(got) == 2 and got.index.is_unique
    assert not {'s16bq09c', 'Quantity_kg', 'nitrogen_kg', 'v', 'season'} & set(got)
    # Native source is invertible independently through the unfiltered adapter.
    review = module.s16b_rows_for_wave(raw, wave, household_id=formatters[wave])
    pd.testing.assert_frame_equal(review[list(raw)].reset_index(drop=True),
                                  raw.reset_index(drop=True), check_exact=True)


@pytest.mark.parametrize('wave,menu', [('2018-19', MENU18), ('2021-22', MENU21)])
def test_all_own_public_slots_are_injective_with_literal_catchalls(adapter, wave, menu):
    module, formatters = adapter
    raw = pd.concat([native(wave).iloc[[0]]] * len(menu), ignore_index=True)
    raw['s16bq01'] = np.arange(1, len(menu) + 1, dtype='int8')
    got = module.s16b_plot_inputs_for_wave(raw, wave, household_id=formatters[wave])
    inputs = ['Manure', 'Other organic', 'Urea', 'Engrais inorganiques - Phosphates',
              'NPK', 'DAP et autres engrais inorganiques', 'Pesticide', 'Herbicide',
              'Fungicide', 'Other phytosanitary'] + ['Seed'] * (len(menu) - 10)
    crops = ['(not crop-specific)'] * 10 + ['Mil', 'Sorgho', 'Maïs', 'Riz',
              'Autre céréale', 'Coton', 'Sésame', 'Haricots secs']
    crops += (['Autre tubercule', 'Autres semences'] if wave == '2018-19' else
              ["Semences d'arichide", 'Autre tubercule', "Plants d'hévéa, de teck", 'Autres semences'])
    assert got.index.get_level_values('input').tolist() == inputs
    assert got.index.get_level_values('crop').tolist() == crops
    assert got['Input_native'].tolist() == menu
    assert got['input_code_native'].tolist() == list(range(1, len(menu) + 1))
    assert got.index.is_unique and len(got) == len(menu)
    assert got['Quantity'].tolist() == [1.25] * len(menu)
    assert got.index.get_level_values('plot_id').tolist() == ['Farm-level'] * len(menu)


@pytest.mark.parametrize('field', ['s16bq01', 's16bq03b', 's16bq05', 's16bq07b',
                                  's16bq08', 's16bq09b'])
def test_public_unknown_refuses_but_the_raw_answer_survives(adapter, field):
    module, formatters = adapter
    raw = public_native('2018-19')
    raw[field] = raw[field].astype('Float64')
    raw.iloc[0, raw.columns.get_loc(field)] = 77
    with pytest.raises(ValueError, match='unlisted native answers'):
        module.s16b_plot_inputs_for_wave(raw, '2018-19', household_id=formatters['2018-19'])
    review = module.s16b_rows_for_wave(raw, '2018-19', household_id=formatters['2018-19'])
    assert review[field].iloc[0] == 77 and len(review) == len(raw)


def test_public_missing_unit_empty_and_collision_controls(adapter):
    module, formatters = adapter
    raw = public_native('2021-22')
    raw['s16bq03b'] = raw['s16bq03b'].astype('Float64')
    raw.iloc[0, raw.columns.get_loc('s16bq03b')] = pd.NA
    got = module.s16b_plot_inputs_for_wave(raw, '2021-22', household_id=formatters['2021-22'])
    assert got['u'].tolist() == ['Unknown', 'Charrette']
    assert got['Unit_purchased'].tolist() == ['Kilogramme', 'Charrette']
    raw['s16bq02'] = pd.array([2] * len(raw), dtype='Float64')
    empty = module.s16b_plot_inputs_for_wave(raw, '2021-22', household_id=formatters['2021-22'])
    pd.testing.assert_frame_equal(empty.reset_index(drop=True),
                                  got.iloc[:0].reset_index(drop=True), check_exact=True)
    assert empty.index.names == ['t', 'i', 'plot_id', 'input', 'crop']
    raw['s16bq02'] = pd.array([1] * len(raw), dtype='Float64')
    raw['s16bq05'] = pd.array([2] * len(raw), dtype='Float64')
    raw['s16bq08'] = pd.array([2] * len(raw), dtype='Float64')
    saved = module.PUBLIC_LABELS['Engrais inorganiques - NPK/Formule unique']
    module.PUBLIC_LABELS['Engrais inorganiques - NPK/Formule unique'] = ('Urea', '(not crop-specific)')
    try:
        with pytest.raises(ValueError, match='collide on the public key'):
            module.s16b_plot_inputs_for_wave(raw, '2021-22', household_id=formatters['2021-22'])
    finally:
        module.PUBLIC_LABELS['Engrais inorganiques - NPK/Formule unique'] = saved


@pytest.mark.parametrize('wave,year', [('2018-19', '2018'), ('2021-22', '2021')])
@pytest.mark.parametrize('duplicate', [False, True])
def test_actual_driver_extraction_uses_one_numeric_own_source_and_refuses_before_write(adapter, wave, year, duplicate):
    module, formatters = adapter
    raw = public_native(wave)
    if duplicate:
        raw = pd.concat([raw.iloc[[0]], raw.iloc[[0]]], ignore_index=True)
    reads, writes = [], []

    def getter(path, *, convert_categoricals):
        assert path == '../Data/s16b_me_mli' + year + '.dta'
        assert convert_categoricals is False
        reads.append(path)
        return raw.copy(deep=True)

    def writer(frame, path):
        assert path == 'plot_inputs.parquet'
        writes.append(frame.copy(deep=True))

    path = COUNTRY / wave / '_/plot_inputs.py'
    ns = _extract(path, ['build'], {'get_dataframe': getter, 'to_parquet': writer,
        'household_id': formatters[wave], 's16b_plot_inputs_for_wave': module.s16b_plot_inputs_for_wave})
    if duplicate:
        with pytest.raises(ValueError, match='repeats a native'):
            ns['build']()
        assert not writes
    else:
        ns['build']()
        assert len(writes) == 1
        expected = module.s16b_plot_inputs_for_wave(raw, wave, household_id=formatters[wave])
        pd.testing.assert_frame_equal(writes[0], expected, check_exact=True)
        assert writes[0].attrs == expected.attrs
    assert reads == ['../Data/s16b_me_mli' + year + '.dta']


def test_full_dap_catchall_does_not_match_current_production_nutrient_share(adapter):
    module, _ = adapter
    path = ROOT / 'lsms_library/transformations.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    content = next(ast.literal_eval(node.value) for node in tree.body
                   if isinstance(node, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id == '_NITROGEN_CONTENT' for t in node.targets))
    organic = next(node for node in tree.body if isinstance(node, ast.Assign)
                   and any(isinstance(t, ast.Name) and t.id == '_ORGANIC_INPUT_PATTERN' for t in node.targets))
    ns = {'pd': pd, 're': re}
    exec(compile(ast.Module(body=[organic], type_ignores=[]), str(path), 'exec'), ns)
    ns = _extract(path, ['_normalise_input_label', '_nitrogen_shares'], ns)
    label = module.PUBLIC_LABELS['DAP et autres engrais inorganiques'][0]
    shares, tally = ns['_nitrogen_shares'](pd.Series([label, 'DAP', 'NPK']), content)
    assert pd.isna(shares.iloc[0])
    np.testing.assert_array_equal(shares.iloc[1:].to_numpy(), np.array([0.18, 0.17]))
    assert tally['unmatched_labels'] == {label: 1}


@pytest.mark.parametrize('wave', ['2018-19', '2021-22'])
@pytest.mark.parametrize('answer', [2, pd.NA, 9])
def test_public_membership_precedes_hollow_nonapplication_key_validation(adapter, wave, answer):
    module, formatters = adapter
    yes = native(wave).iloc[[0]].copy()
    hollow = pd.concat([yes, yes], ignore_index=True)
    hollow['s16bq02'] = pd.array([answer, answer], dtype='Float64')
    for name in ['grappe', 'menage', 's16bq01']:
        hollow[name] = pd.array([pd.NA, pd.NA], dtype='Float64')
    raw = pd.concat([yes, hollow], ignore_index=True)
    raw.attrs = deepcopy(yes.attrs)
    before = raw.copy(deep=True)
    expected = module.s16b_plot_inputs_for_wave(yes, wave, household_id=formatters[wave])
    got = module.s16b_plot_inputs_for_wave(raw, wave, household_id=formatters[wave])
    pd.testing.assert_frame_equal(got, expected, check_exact=True)
    pd.testing.assert_frame_equal(raw, before, check_exact=True)
    assert type(got.attrs['record']) is AttrRecord
    with pytest.raises(ValueError, match='nonmissing positive integer keys'):
        module.s16b_rows_for_wave(raw, wave, household_id=formatters[wave])
    selected_bad = raw.copy(deep=True)
    selected_bad.loc[1, 's16bq02'] = 1
    with pytest.raises(ValueError, match='nonmissing positive integer keys'):
        module.s16b_plot_inputs_for_wave(selected_bad, wave, household_id=formatters[wave])
