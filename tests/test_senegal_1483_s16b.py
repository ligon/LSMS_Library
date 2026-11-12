"""Invented-only own Senegal1483 controls; no native/provider/data API calls.

Literal oracles are independent of adapter constants. Production definitions
are AST-extracted only for own i/format_id, script discovery and writer path.
Run this file alone, --noconftest, -n2 --dist=loadfile, explicit --rootdir.
Native correspondence, real serving and scientific acceptance remain UNRUN.
"""
import ast
from copy import deepcopy
import importlib.util
import inspect
from pathlib import Path
import re
import socket
import warnings
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
COUNTRY = ROOT / 'lsms_library/countries/Senegal'
ADAPTER = COUNTRY / '_/s16b_2021.py'
DRIVER = COUNTRY / '2021-22/_/plot_inputs.py'
FIELDS = ('grappe', 'menage', 'vague', 's16bq01', 's16bq02', 's16bq03a',
          's16bq03b', 's16bq03b_autre', 's16bq04', 's16bq04_autre', 's16bq05',
          's16bq06', 's16bq06_autre', 's16bq07a', 's16bq07b', 's16bq08',
          's16bq09a', 's16bq09b', 's16bq09c')
MENU = ["Engrais organiques - déchets d'animaux",
    'Engrais organiques - Ordures ménagères', 'Engrais inorganiques - Urée',
    'Engrais inorganiques - Phosphates', 'Engrais inorganiques - NPK/Formule unique',
    'DAP et autres engrais inorganiques', 'Produits phytosanitaires - Pesticides',
    'Produits phytosanitaires - Herbicides', 'Produits phytosanitaires - Fongicides',
    'Autres produits phytosanitaires', 'Semences de petit mil', 'Semences de sorgho',
    'Semences de maïs', 'Semences de riz', "Semences d'autres céréales",
    'Semences de coton', 'Semences de césame', 'Semences de haricots/niébé',
    "Semences d'arachide", 'Plants/boutures de tubercules', "Plants d'hévéa, de teck",
    "Semences d'ognon", 'Semences de Carotte', "Semences d'aubergine",
    'Semences de Combo', 'Semences de Piment', 'Semences de Pastéque',
    'Semences de poivron', 'Semences de courge/courgette', 'Semences de fonio',
    'Semences de Oseille/bissap', 'Semences de Tomate', 'Semences de chou',
    'Semences de Conconbre', 'Semences de Laitue/Salade', 'Autre semences']


class AttrRecord(dict):
    """Invented typed metadata: equality alone cannot detect a type loss."""


def extract(path, names, namespace, *, owner=None):
    tree = ast.parse(path.read_text(encoding='utf-8'))
    body = tree.body
    if owner:
        body = next(node.body for node in body
                    if isinstance(node, ast.ClassDef) and node.name == owner)
    selected = [deepcopy(node) for node in body
                if isinstance(node, ast.FunctionDef) and node.name in names]
    assert {node.name for node in selected} == set(names)
    for node in selected:
        node.decorator_list = []
    exec(compile(ast.fix_missing_locations(ast.Module(body=selected, type_ignores=[])),
                 str(path), 'exec'), namespace)
    return namespace


def org_map(name):
    """Read only the committed config table, never a provider or data file."""
    text = (COUNTRY / '_/categorical_mapping.org').read_text(encoding='utf-8')
    lines = re.split(r'(?im)^#\+name:\s*' + re.escape(name) + r'\s*$', text)[1].splitlines()
    result = {}
    for line in lines:
        if line.startswith('|') and '+' not in line:
            cells = [cell.strip() for cell in line.strip('|').split('|')]
            if len(cells) >= 2 and cells[0] != 'Original Label':
                result[cells[0]] = cells[1]
        elif result and not line.startswith('|'):
            break
    assert result
    return result


@pytest.fixture(autouse=True)
def no_native_or_network(monkeypatch):
    def deny(*args, **kwargs):
        raise AssertionError('invented Senegal controls attempted native/provider IO')
    for name in ('read_stata', 'read_parquet', 'read_csv', 'read_excel', 'read_spss', 'read_pickle'):
        monkeypatch.setattr(pd, name, deny)
    for name in ('to_parquet', 'to_csv', 'to_excel', 'to_pickle'):
        monkeypatch.setattr(pd.DataFrame, name, deny)
    for name in ('connect', 'connect_ex'):
        monkeypatch.setattr(socket.socket, name, deny)
    monkeypatch.setattr(socket, 'create_connection', deny)
    monkeypatch.setattr(socket, 'getaddrinfo', deny)


@pytest.fixture
def adapter():
    tree = ast.parse(ADAPTER.read_text())
    imports = [node for node in ast.walk(tree) if isinstance(node, (ast.Import, ast.ImportFrom))]
    assert [(node.module, [x.name for x in node.names]) if isinstance(node, ast.ImportFrom)
            else (None, [x.name for x in node.names]) for node in imports] == [
                ('copy', ['deepcopy']), (None, ['pandas'])]
    spec = importlib.util.spec_from_file_location('senegal1483_pure', ADAPTER)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    fmt = extract(ROOT / 'lsms_library/local_tools.py', ['format_id'],
                  {'pd': pd, 'Any': Any})['format_id']
    own = extract(COUNTRY / '_/senegal.py', ['i'],
                  {'tools': SimpleNamespace(format_id=fmt)})['i']
    return module, dict(household_id=own, input_map=org_map('harmonize_input'),
                        seed_crop_map=org_map('harmonize_seed_crop'), unit_map=org_map('u'))


def invented(n=6):
    data = {
        'grappe': np.array([12] * n, dtype='int16'),
        'menage': np.array([3] * n, dtype='int8'),
        'vague': np.array([2] * n, dtype='int8'),
        's16bq01': np.arange(1, n + 1, dtype='int8'),
        's16bq02': pd.array(([1, 2, pd.NA, 9, 1, 1] * n)[:n], dtype='Float64'),
        's16bq03a': pd.array(([1.25, 90, 91, 92, 0, pd.NA] * n)[:n], dtype='Float64'),
        's16bq03b': pd.array([1] * n, dtype='Int8'),
        's16bq03b_autre': pd.array(['autre kg?'] * n, dtype='string'),
        's16bq04': pd.array([1] * n, dtype='Int8'),
        's16bq04_autre': pd.array(['site'] * n, dtype='string'),
        's16bq05': pd.array(([2, 1, 2, pd.NA, 1, pd.NA] * n)[:n], dtype='Float64'),
        's16bq06': pd.array([2] * n, dtype='Int8'),
        's16bq06_autre': pd.array(['personne'] * n, dtype='string'),
        's16bq07a': pd.array(([8, 7, 6, 5, -2, pd.NA] * n)[:n], dtype='Float64'),
        's16bq07b': pd.array([1] * n, dtype='Int8'),
        's16bq08': pd.array(([1, 2, 1, 2, 2, pd.NA] * n)[:n], dtype='Float64'),
        's16bq09a': np.array(([4.5, 3, 2, 1, -3, np.nan] * n)[:n], dtype='float64'),
        's16bq09b': pd.array([1] * n, dtype='Int8'),
        's16bq09c': pd.array(([350, 99, 0, -3, pd.NA, 999] * n)[:n], dtype='Int32'),
    }
    raw = pd.DataFrame(data)
    raw.index = pd.MultiIndex.from_arrays([['x'] * n, [8] * n], names=['review', 'row'])
    raw.attrs = {'record': AttrRecord(wave='invented', nested=[None, {'a': 1}])}
    return raw


def all_yes(n=20):
    raw = invented(n)
    raw['s16bq02'] = pd.array([1] * n, dtype='Int8')
    return raw


def test_allrow_exact_inverse_including_extra_column_duplicate_index_and_typed_attrs(adapter):
    module, _ = adapter
    raw = invented()
    raw['extra_native'] = pd.Categorical(['a', None, 'b', 'a', 'b', None])
    before = deepcopy(raw)
    got = module.s16b_rows(raw)
    pd.testing.assert_frame_equal(got[list(raw)], raw, check_exact=True)
    pd.testing.assert_frame_equal(got[list(raw)].isna(), raw.isna())
    for alias, source in [('Quantity', 's16bq03a'), ('Quantity_received_as_gift', 's16bq07a'),
                          ('Quantity_purchased', 's16bq09a')]:
        pd.testing.assert_series_equal(got[alias], raw[source], check_names=False, check_exact=True)
    assert type(got.attrs['record']) is AttrRecord and got.attrs == before.attrs
    got.attrs['record']['nested'].append('independent')
    assert raw.attrs == before.attrs
    pd.testing.assert_frame_equal(raw, before, check_exact=True)


def test_36_own_slots_literal_tail_and_full_catchall(adapter):
    module, _ = adapter
    raw = all_yes(36)
    got = module.s16b_rows(raw)
    assert got['Input_native'].tolist() == MENU
    assert not got['Input_native_unlisted'].any()
    assert got['s16bq01'].tolist() == list(range(1, 37))
    assert got['Input_native'].iloc[5] == 'DAP et autres engrais inorganiques'
    assert not {'plot_id', 'input', 'crop', 'Quantity_kg', 'nitrogen_kg'} & set(got)
    assert module.RAW_FIELDS == FIELDS
    assert module.SOURCE_MD5 == 'cc408db6055a1a78506e30260c73f9c3'
    assert module.HEADER_SHA256 == '883e269e3ee2a47c473ad347efc10ff648fb2599650277d634019e36b48c80f7'


def test_all_three_unit_namespaces_have_independent_literal_oracles(adapter):
    module, _ = adapter
    raw = all_yes(10)
    raw['s16bq03b'] = pd.array(range(1, 11), dtype='Int8')
    raw['s16bq07b'] = pd.array([1, 2, 3, 4, 5] * 2, dtype='Int8')
    raw['s16bq09b'] = pd.array([5, 4, 3, 2, 1] * 2, dtype='Int8')
    got = module.s16b_rows(raw)
    assert got['u'].tolist() == ['Kg', 'Sac Gros', 'Sac moyen', 'Sac Petit',
        'Charrette asine', 'Charrette bovine', 'Seau', 'Panier', 'Camion benne', 'Autre (à spécifier)']
    assert got['Unit_received_as_gift'].tolist() == ['Gramme', 'Kilogramme', 'Tonne', 'Litre', 'Charrettée'] * 2
    assert got['Unit_purchased'].tolist() == ['Charrettée', 'Litre', 'Tonne', 'Kilogramme', 'Gramme'] * 2
    raw['s16bq03b'] = raw['s16bq07b'] = raw['s16bq09b'] = 1
    got = module.s16b_rows(raw)
    assert got['u'].tolist() == ['Kg'] * 10
    assert got['Unit_received_as_gift'].tolist() == got['Unit_purchased'].tolist() == ['Gramme'] * 10


def test_gift_purchase_use_value_and_flags_never_substitute(adapter):
    module, _ = adapter
    raw = invented()
    got = module.s16b_rows(raw)
    assert got['Received_as_gift'].tolist() == [False, True, False, pd.NA, True, pd.NA]
    assert got['Purchased'].tolist() == [True, False, True, False, False, pd.NA]
    assert got['Quantity'].tolist() == [1.25, 90, 91, 92, 0, pd.NA]
    assert got['Quantity_received_as_gift'].tolist() == [8, 7, 6, 5, -2, pd.NA]
    assert got['Quantity_purchased'].iloc[:5].tolist() == [4.5, 3, 2, 1, -3]
    pd.testing.assert_series_equal(got['s16bq09c'], raw['s16bq09c'], check_exact=True)
    assert not {'Expenditure', 'Value', 'Price'} & set(got)


def test_applied_classes_partition_and_public_membership_before_amounts(adapter):
    module, kwargs = adapter
    raw = invented()
    got = module.s16b_use_classes(raw)
    expected = pd.DataFrame({'yes': [True, False, False, False, True, True],
        'no': [False, True, False, False, False, False],
        'unanswered': [False, False, True, False, False, False],
        'unlisted': [False, False, False, True, False, False]}, index=raw.index)
    pd.testing.assert_frame_equal(got, expected, check_exact=True)
    assert got.sum(axis=1).tolist() == [1] * 6
    out = module.s16b_plot_inputs(raw, **kwargs)
    assert out['s16bq01'].tolist() == [1, 5, 6]
    assert out['Quantity'].tolist() == [1.25, 0, pd.NA]
    assert out['Purchased'].tolist() == [True, False, pd.NA]
    assert out['Quantity_received_as_gift'].tolist() == [8, -2, pd.NA]
    pd.testing.assert_frame_equal(out[list(raw)].reset_index(drop=True),
                                  raw.iloc[[0, 4, 5]].reset_index(drop=True), check_exact=True)


def test_public_first20_existing_maps_native_key_and_no_parcel_allocation(adapter):
    module, kwargs = adapter
    raw = all_yes()
    out = module.s16b_plot_inputs(raw, **kwargs)
    assert out.index.names == ['t', 'i', 'input', 'crop', 'u']
    assert out.index.get_level_values('i').tolist() == ['12003'] * 20
    assert out.index.get_level_values('input').tolist() == ['Organic manure', 'Organic compost',
        'Urea', 'Phosphate', 'NPK', 'DAP et autres engrais inorganiques', 'Pesticide',
        'Herbicide', 'Fungicide', 'Other phytosanitary'] + ['Seed'] * 10
    assert out.index.get_level_values('crop').tolist() == ['(not crop-specific)'] * 10 + [
        'Mil', 'Sorgho', 'Maïs en grain', 'Riz Paddy', 'Autre céréale', 'Coton',
        'Sésame', 'Niébé/Haricots secs', 'Arachide', 'Autre tubercule']
    assert out.index.is_unique and len(out) == 20
    assert out['Input_native'].tolist() == MENU[:20]
    assert not {'plot_id', 'v', 'Quantity_kg', 'KgFactor', 'Applied'} & set(out)
    assert out.attrs == raw.attrs and type(out.attrs['record']) is AttrRecord


@pytest.mark.parametrize('code', range(21, 37))
def test_unresolved_public_tail_retains_exact_native_category(adapter, code):
    module, kwargs = adapter
    raw = all_yes(1)
    raw['s16bq01'] = code
    assert module.s16b_rows(raw)['Input_native'].tolist() == [MENU[code - 1]]
    got = module.s16b_plot_inputs(raw, **kwargs)
    assert got.index.tolist() == [('2021-22', '12003', 'Seed', MENU[code - 1], 'Kg')]
    assert got['input_code_native'].tolist() == [code]
    pd.testing.assert_frame_equal(got[list(raw)].reset_index(drop=True),
                                  raw.reset_index(drop=True), check_exact=True)


def test_full36_public_projection_preserves_each_slot_and_all_role_payloads(adapter):
    module, kwargs = adapter
    raw = all_yes(36)
    raw['extra_native'] = pd.Categorical(['extra', None] * 18)
    # Independent role cards and quantities, with deliberately contradictory
    # flag/amount answers: neither flags nor amount signs may edit the source.
    raw['s16bq03b'] = pd.array([1, pd.NA, 10, 2] * 9, dtype='Int8')
    raw['s16bq07b'] = pd.array([1, 2, pd.NA, 5] * 9, dtype='Int8')
    raw['s16bq09b'] = pd.array([5, 1, 4, pd.NA] * 9, dtype='Int8')
    raw['s16bq03a'] = np.array([0, -7, np.nan, np.nextafter(1., 2.)] * 9)
    before = deepcopy(raw)
    got = module.s16b_plot_inputs(raw, **kwargs)
    assert len(got) == 36 and got.index.is_unique
    assert got.index.names == ['t', 'i', 'input', 'crop', 'u']
    assert got.index.get_level_values('input').tolist() == [
        'Organic manure', 'Organic compost', 'Urea', 'Phosphate', 'NPK',
        'DAP et autres engrais inorganiques', 'Pesticide', 'Herbicide',
        'Fungicide', 'Other phytosanitary'] + ['Seed'] * 26
    assert got.index.get_level_values('crop').tolist() == ['(not crop-specific)'] * 10 + [
        'Mil', 'Sorgho', 'Maïs en grain', 'Riz Paddy', 'Autre céréale', 'Coton',
        'Sésame', 'Niébé/Haricots secs', 'Arachide', 'Autre tubercule',
        "Plants d'hévéa, de teck", "Semences d'ognon", 'Semences de Carotte',
        "Semences d'aubergine", 'Semences de Combo', 'Semences de Piment',
        'Semences de Pastéque', 'Semences de poivron', 'Semences de courge/courgette',
        'Semences de fonio', 'Semences de Oseille/bissap', 'Semences de Tomate',
        'Semences de chou', 'Semences de Conconbre', 'Semences de Laitue/Salade',
        'Autre semences']
    assert got.index.get_level_values('u').tolist() == ['Kg', 'Unknown', 'Autre (à spécifier)', 'Sac Gros'] * 9
    assert got['Unit_received_as_gift'].tolist() == ['Gramme', 'Kilogramme', pd.NA, 'Charrettée'] * 9
    assert got['Unit_purchased'].tolist() == ['Charrettée', 'Gramme', 'Litre', pd.NA] * 9
    pd.testing.assert_series_equal(got['input_code_native'].reset_index(drop=True),
        pd.Series(range(1, 37), name='input_code_native', dtype='Int64'))
    pd.testing.assert_frame_equal(got[list(raw)].reset_index(drop=True),
                                  raw.reset_index(drop=True), check_exact=True)
    for alias, source in [('Quantity', 's16bq03a'), ('Quantity_received_as_gift', 's16bq07a'),
                          ('Quantity_purchased', 's16bq09a')]:
        pd.testing.assert_series_equal(got[alias].reset_index(drop=True),
            raw[source].reset_index(drop=True), check_names=False, check_exact=True)
    assert got.attrs == raw.attrs and type(got.attrs['record']) is AttrRecord
    pd.testing.assert_frame_equal(raw, before, check_exact=True)


@pytest.mark.parametrize('answer', [2, pd.NA, 9])
def test_hollow_nonapplications_do_not_fail_selected_key_gate(adapter, answer):
    module, kwargs = adapter
    yes = all_yes(1)
    hollow = pd.concat([yes, yes], ignore_index=True)
    hollow['s16bq02'] = pd.array([answer] * 2, dtype='Float64')
    for name in ('grappe', 'menage', 's16bq01', 's16bq03b'):
        hollow[name] = pd.array([pd.NA] * 2, dtype='Float64')
    raw = pd.concat([yes, hollow], ignore_index=True)
    raw.attrs = deepcopy(yes.attrs)
    before = deepcopy(raw)
    out = module.s16b_plot_inputs(raw, **kwargs)
    expected = module.s16b_plot_inputs(raw.iloc[:1], **kwargs)
    pd.testing.assert_frame_equal(out, expected, check_exact=True)
    assert len(module.s16b_rows(raw)) == 3
    pd.testing.assert_frame_equal(raw, before, check_exact=True)
    raw.loc[1, 's16bq02'] = 1
    with pytest.raises(module.S16b2021Error, match='selected native keys'):
        module.s16b_plot_inputs(raw, **kwargs)


@pytest.mark.parametrize('amount', [0., -7., np.nan])
def test_selected_amounts_are_not_an_eligibility_filter(adapter, amount):
    module, kwargs = adapter
    raw = all_yes(1)
    for name in ('s16bq03a', 's16bq07a', 's16bq09a'):
        raw[name] = np.array([amount], dtype='float64')
    got = module.s16b_plot_inputs(raw, **kwargs)
    assert len(got) == 1
    for name in ('Quantity', 'Quantity_received_as_gift', 'Quantity_purchased'):
        assert pd.isna(got[name].iloc[0]) if pd.isna(amount) else got[name].iloc[0] == amount


@pytest.mark.parametrize('name', ['grappe', 'menage', 's16bq01'])
@pytest.mark.parametrize('value', [pd.NA, 0, -1, 1.5, np.inf])
def test_selected_native_key_gate_without_source_mutation(adapter, name, value):
    module, kwargs = adapter
    raw = all_yes(1)
    raw[name] = pd.array([value], dtype='Float64')
    before = deepcopy(raw)
    assert len(module.s16b_rows(raw)) == 1
    with pytest.raises(module.S16b2021Error, match='selected native keys'):
        module.s16b_plot_inputs(raw, **kwargs)
    pd.testing.assert_frame_equal(raw, before, check_exact=True)


@pytest.mark.parametrize('name', ['s16bq01', 's16bq03b', 's16bq05', 's16bq07b', 's16bq08', 's16bq09b'])
def test_unknown_selected_codes_keep_raw_unlisted_mask_then_refuse(adapter, name):
    module, kwargs = adapter
    raw = all_yes(1)
    raw[name] = pd.array([77.5], dtype='Float64')
    transport = module.s16b_rows(raw)
    alias = {'s16bq01': 'Input_native', 's16bq03b': 'u', 's16bq05': 'Received_as_gift',
             's16bq07b': 'Unit_received_as_gift', 's16bq08': 'Purchased', 's16bq09b': 'Unit_purchased'}[name]
    assert transport[name].iloc[0] == 77.5
    assert bool(transport[alias + '_unlisted'].iloc[0]) and pd.isna(transport[alias].iloc[0])
    text = 'selected native keys' if name == 's16bq01' else 'unlisted selected'
    with pytest.raises(module.S16b2021Error, match=text):
        module.s16b_plot_inputs(raw, **kwargs)


def test_missing_units_and_flags_keep_distinct_masks_and_selected_key_semantics(adapter):
    module, kwargs = adapter
    raw = all_yes(1)
    for name in ('s16bq05', 's16bq07b', 's16bq08', 's16bq09b'):
        raw[name] = pd.array([pd.NA], dtype='Float64')
    got = module.s16b_plot_inputs(raw, **kwargs)
    assert got[['Purchased', 'Received_as_gift', 'Unit_purchased', 'Unit_received_as_gift']].isna().all(axis=None)
    assert got['Quantity_purchased'].iloc[0] == 4.5 and got['Quantity_received_as_gift'].iloc[0] == 8
    raw['s16bq03b'] = pd.array([pd.NA], dtype='Float64')
    transport = module.s16b_rows(raw)
    assert pd.isna(transport['u'].iloc[0]) and not transport['u_unlisted'].iloc[0]
    got = module.s16b_plot_inputs(raw, **kwargs)
    assert got.index.get_level_values('u').tolist() == ['Unknown']
    assert got['s16bq03b'].isna().all() and not got['u_unlisted'].any()
    assert got[['Purchased', 'Received_as_gift', 'Unit_purchased', 'Unit_received_as_gift']].isna().all(axis=None)
    assert got['Quantity'].iloc[0] == 1.25


@pytest.mark.parametrize('equal', [True, False])
def test_native_and_canonical_collisions_refuse_even_equal_payloads(adapter, equal):
    module, kwargs = adapter
    raw = pd.concat([all_yes(1)] * 2, ignore_index=True)
    if not equal:
        raw.loc[1, 's16bq03a'] = 999
    assert len(module.s16b_rows(raw)) == 2
    with pytest.raises(module.S16b2021Error, match='duplicate selected native'):
        module.s16b_plot_inputs(raw, **kwargs)
    raw.loc[1, 's16bq01'] = 2
    mutant = dict(kwargs, input_map=dict(kwargs['input_map']))
    mutant['input_map']['Engrais organiques - Ordures ménagères'] = 'Organic manure'
    with pytest.raises(module.S16b2021Error, match='collide on declared keys'):
        module.s16b_plot_inputs(raw, **mutant)


def test_own_id_examples_nonforeign_prefix_and_household_collisions_across_slots(adapter):
    module, kwargs = adapter
    own = kwargs['household_id']
    assert own(pd.Series([1, 23])) == '1023'
    assert own(pd.Series([12, 3])) == '12003'
    raw = all_yes(2)
    raw['menage'] = [3, 4]
    for foreign in ('E_12003', 'EHCVM21_12003', None, ''):
        with pytest.raises(module.S16b2021Error, match='bare decimal IDs'):
            module.s16b_plot_inputs(raw, **dict(kwargs, household_id=lambda pair: foreign))
    with pytest.raises(module.S16b2021Error, match='distinct native households collide'):
        module.s16b_plot_inputs(raw, **dict(kwargs, household_id=lambda pair: '12003'))
    raw.attrs['id_converted'] = True
    assert module.s16b_rows(raw).attrs['id_converted'] is True
    with pytest.raises(module.S16b2021Error, match='before household ID walk'):
        module.s16b_plot_inputs(raw, **kwargs)


def test_empty_exact_schema_and_raw_column_collision_controls(adapter):
    module, kwargs = adapter
    raw = all_yes(1)
    got = module.s16b_plot_inputs(raw.iloc[:0], **kwargs)
    expected = module.s16b_plot_inputs(raw, **kwargs).iloc[:0]
    pd.testing.assert_frame_equal(got.reset_index(drop=True), expected.reset_index(drop=True), check_exact=True)
    assert got.index.names == ['t', 'i', 'input', 'crop', 'u']
    for bad, text in [(raw.drop(columns='s16bq09c'), 'missing own source'),
                      (raw.assign(Quantity=1), 'collide with adapter aliases'),
                      (raw.assign(input_code_native=1), 'collide with adapter aliases'),
                      (raw.assign(s16bq05=True), 'numeric native field'),
                      (raw.assign(s16bq02='Oui'), 'numeric native field'),
                      (pd.concat([raw, raw[['s16bq05']]], axis=1), 'column names must be unique')]:
        with pytest.raises(module.S16b2021Error, match=text):
            module.s16b_rows(bad)


@pytest.mark.parametrize('defect', ['none', 'native_collision', 'canonical_collision', 'tail', 'missing_unit', 'full_menu'])
def test_actual_driver_only_one_numeric_source_and_no_write_before_gate(adapter, defect):
    module, kwargs = adapter
    raw = all_yes(36 if defect == 'full_menu' else 2)
    if defect == 'native_collision':
        raw['s16bq01'] = [1, 1]
    elif defect == 'canonical_collision':
        kwargs['input_map']['Engrais organiques - Ordures ménagères'] = 'Organic manure'
    elif defect == 'tail':
        raw['s16bq01'] = [1, 36]
    elif defect == 'missing_unit':
        raw['s16bq03b'] = pd.array([1, pd.NA], dtype='Int8')
    reads, writes, maps = [], [], []
    def read(path, *, convert_categoricals):
        assert path == '../Data/s16b_me_sen2021.dta' and convert_categoricals is False
        reads.append(path)
        return deepcopy(raw)
    def get_map(*, tablename, idxvars, **values):
        assert idxvars == 'Original Label' and values == {'Preferred Label': 'Preferred Label'}
        maps.append(tablename)
        return kwargs[{'harmonize_input': 'input_map', 'harmonize_seed_crop': 'seed_crop_map', 'u': 'unit_map'}[tablename]]
    def write(frame, path):
        assert path == 'plot_inputs.parquet'
        writes.append(deepcopy(frame))
    build = extract(DRIVER, ['build'], {'get_dataframe': read,
        'get_categorical_mapping': get_map, 'to_parquet': write,
        'i': kwargs['household_id'], 's16b_plot_inputs': module.s16b_plot_inputs})['build']
    if defect in ('none', 'tail', 'missing_unit', 'full_menu'):
        build()
        assert len(writes) == 1
        pd.testing.assert_frame_equal(writes[0], module.s16b_plot_inputs(raw, **kwargs), check_exact=True)
        if defect == 'tail':
            assert writes[0].index.tolist()[-1] == ('2021-22', '12003', 'Seed', 'Autre semences', 'Kg')
            assert writes[0]['input_code_native'].tolist() == [1, 36]
        elif defect == 'missing_unit':
            assert writes[0].index.get_level_values('u').tolist() == ['Kg', 'Unknown']
            assert writes[0]['s16bq03b'].isna().tolist() == [False, True]
        elif defect == 'full_menu':
            assert len(writes[0]) == 36 and writes[0].index.is_unique
            assert writes[0].index.get_level_values('crop')[20:].tolist() == MENU[20:]
            assert writes[0]['input_code_native'].tolist() == list(range(1, 37))
    else:
        with pytest.raises(module.S16b2021Error):
            build()
        assert writes == []
    assert reads == ['../Data/s16b_me_sen2021.dta']
    assert maps == ['harmonize_input', 'harmonize_seed_crop', 'u']


def test_fullmenu_survives_actual_country_normalization_and_feature_alignment(adapter):
    """Execute only extracted pure source definitions, not Country/Feature APIs.

    The sample/v boundary is an explicit invented stub. This tests the real
    index-normalization and canonical-alignment code, not runtime serving.
    """
    module, kwargs = adapter
    raw = all_yes(36)
    raw['s16bq03b'] = pd.array([pd.NA] * 36, dtype='Int8')
    frame = module.s16b_plot_inputs(raw, **kwargs)
    before = deepcopy(frame)
    core = extract(ROOT / 'lsms_library/country.py',
        ['_declared_index_levels', '_normalize_dataframe_index'], {'pd': pd, 'Any': Any})
    normalized = core['_normalize_dataframe_index'](frame,
        {'index': '(t, i, input, crop, u)'}, '2021-22', 'plot_inputs', 'Senegal')
    pd.testing.assert_frame_equal(normalized, before, check_exact=True)
    # Invented old2018-grain companion. No native2018 data or producer runs.
    old = pd.DataFrame({'Quantity': pd.array([4.], dtype='Float64'),
                       'Purchased': pd.array([True], dtype='boolean'),
                       'Quantity_purchased': np.array([2.], dtype='float64')},
        index=pd.MultiIndex.from_tuples([('2018-19', '12003', 'Seed', 'Arachide', 'Sac')],
                                       names=['t', 'i', 'input', 'crop', 'u']))
    combined = pd.concat([old, normalized], sort=False)
    assert len(combined) == 37 and combined.index.is_unique
    assert not combined.index.to_frame(index=False).isna().any(axis=None)
    # v is supplied here, not fetched; actual sample/IDwalk/cache routes UNRUN.
    combined['v'] = 'invented-cluster'
    combined = combined.set_index('v', append=True)
    def no_collapse(*args, **kwargs):
        raise AssertionError('full native labels unexpectedly collided at assembly')
    ns = extract(ROOT / 'lsms_library/feature.py',
        ['_rename_index_levels', '_align_to_canonical_levels', '_harmonize_country_frame'],
        {'pd': pd, 'Any': Any, 'warnings': warnings, '_collapse_duplicate_index': no_collapse})
    schema = yaml.safe_load((ROOT / 'lsms_library/data_info.yml').read_text())['Index Info']
    canonical = ['t', 'v', 'i', 'plot_id', 'input', 'j', 'u', 'season', 'input_obs']
    report = {}
    aligned = ns['_harmonize_country_frame'](combined, canonical, 'Senegal', 'plot_inputs',
        aliases=schema['level_aliases']['plot_inputs'],
        sentinels=schema['missing_level_sentinels']['plot_inputs'], alignment_report=report)
    assert aligned.index.names == canonical and len(aligned) == 37 and aligned.index.is_unique
    assert aligned.index.get_level_values('plot_id').tolist() == ['Farm-level'] * 37
    assert aligned.index.get_level_values('input_obs').tolist() == ['unresolved_observation'] * 37
    assert aligned.index.get_level_values('j')[21:].tolist() == MENU[20:]
    assert aligned.index.get_level_values('u')[1:].tolist() == ['Unknown'] * 36
    # Cross-wave union can widen raw numpy integers to hold old-wave NA.
    # Require exact preservation across alignment, not fictitious dtype parity
    # between the unfiltered native frame and the multi-wave union.
    # Existing Feature assembly elides wholly-NA payload columns. All36 raw
    # used-unit answers are absent in this case; this removes no populated
    # value and does not remove a row. Country normalization above retains it.
    elided = [name for name in raw if combined[name].isna().all()]
    assert elided == ['s16bq03b'] and not set(elided) & set(aligned)
    carried = [name for name in raw if name not in elided]
    pd.testing.assert_frame_equal(aligned.iloc[1:][carried].reset_index(drop=True),
        combined.iloc[1:][carried].reset_index(drop=True), check_exact=True)
    assert aligned['input_code_native'].iloc[1:].tolist() == list(range(1, 37))
    pd.testing.assert_frame_equal(aligned.iloc[:1][list(old)].reset_index(drop=True),
                                  old.reset_index(drop=True), check_exact=True)


def test_actual_wave_writer_resolver_retains_wave_context_without_serializing(adapter, tmp_path):
    module, kwargs = adapter
    ns = extract(ROOT / 'lsms_library/local_tools.py', ['_resolve_data_path', 'to_parquet'],
        {'pd': pd, 'Path': Path, 'inspect': inspect, 're': re,
         'countries_root': lambda: COUNTRY.parent,
         'data_root': lambda country: tmp_path / country})
    paths = []
    def stop_before_serialization(path):
        paths.append(Path(path))
        raise RuntimeError('invented writer stopped before serialization')
    ns['_tmp_sibling'] = stop_before_serialization
    frame = module.s16b_plot_inputs(all_yes(1), **kwargs)
    build = extract(DRIVER, ['build'], {'get_dataframe': lambda *a, **k: all_yes(1),
        'get_categorical_mapping': lambda **k: {}, 'i': kwargs['household_id'],
        's16b_plot_inputs': lambda *a, **k: frame, 'to_parquet': ns['to_parquet']})['build']
    with pytest.raises(RuntimeError, match='stopped before serialization'):
        build()
    assert paths == [tmp_path / 'Senegal/2021-22/_/plot_inputs.parquet']
    assert not paths[0].is_relative_to(COUNTRY) and not paths[0].exists()
    # Country-helper context would not redirect a bare filename. The actual
    # driver call above proves it does not suffer that write-side trap.
    country_call = compile('def wrong():\n    return writer(frame, "plot_inputs.parquet")',
                           str(COUNTRY / '_/s16b_2021.py'), 'exec')
    wrong = {'writer': ns['to_parquet'], 'frame': frame}
    exec(country_call, wrong)
    with pytest.raises(RuntimeError, match='stopped before serialization'):
        wrong['wrong']()
    assert paths[-1] == Path('plot_inputs.parquet')


def test_actual_source_discovery_and_optional_registry_preserve_old_contract():
    discover = extract(ROOT / 'lsms_library/country.py', ['data_scheme'],
                       {'logger': SimpleNamespace(warning=lambda *a: None)}, owner='Wave')['data_scheme']
    assert 'plot_inputs' in discover(SimpleNamespace(file_path=COUNTRY / '2021-22',
                                                    wave_folder='2021-22', resources={}))
    # Source discovery uses the driver, not a truthy wave !make extraction block.
    tree = yaml.compose((COUNTRY / '2021-22/_/data_info.yml').read_text())
    assert all(key.value != 'plot_inputs' for key, value in tree.value)
    tree = yaml.compose((COUNTRY / '_/data_scheme.yml').read_text())
    scheme = next(value for key, value in tree.value if key.value == 'Data Scheme')
    node = next(value for key, value in scheme.value if key.value == 'plot_inputs')
    got = yaml.safe_load(yaml.serialize(node))
    additions = {'Unit_purchased': 'str', 'Received_as_gift': 'bool',
                 'Quantity_received_as_gift': 'float', 'Unit_received_as_gift': 'str',
                 'Input_native': 'str', 'input_code_native': 'int'}
    for name, kind in additions.items():
        assert got.pop(name) == {'type': kind, 'optional': True}
    assert got == {'index': '(t, i, input, crop, u)', 'Quantity': 'float',
                   'Purchased': 'bool', 'Quantity_purchased': 'float', 'materialize': 'make'}
    schema = yaml.safe_load((ROOT / 'lsms_library/data_info.yml').read_text())
    # The existing structural marker is a global assembly rule, not an adapter
    # allocation. Read its finite source subtree without running Feature.
    assert schema['Index Info']['missing_level_sentinels']['plot_inputs']['plot_id'] == 'Farm-level'


def test_full_catchall_cannot_normalize_to_pure_dap(adapter):
    module, kwargs = adapter
    raw = all_yes(6)
    got = module.s16b_plot_inputs(raw, **kwargs)
    label = got.index.get_level_values('input')[-1]
    normalise = extract(ROOT / 'lsms_library/transformations.py', ['_normalise_input_label'],
                        {'pd': pd, 're': re})['_normalise_input_label']
    assert label == 'DAP et autres engrais inorganiques'
    assert normalise(label) != normalise('DAP')
    assert got.index.get_level_values('input')[-2] == 'NPK'  # existing identity only; no grade chosen


def test_2018_production_functions_still_bind_gift_under_old_purchase_names(adapter):
    _, kwargs = adapter
    old = COUNTRY / '2018-19/_/plot_inputs.py'
    fmt = extract(ROOT / 'lsms_library/local_tools.py', ['format_id'], {'pd': pd, 'Any': Any})['format_id']
    helpers = extract(old, ['i', '_input_labels', '_unit_labels', '_seed_crop_labels', '_finish_plot_inputs'],
        {'pd': pd, 'tools': SimpleNamespace(format_id=fmt), 'CROP_NA': '(not crop-specific)'})
    labels = pd.Series(["Semences d'arachide", 'Plants/boutures de tubercules'])
    frame = pd.DataFrame({'i': ['1023', '12003'],
        'input': helpers['_input_labels'](labels, kwargs['input_map']),
        'crop': helpers['_seed_crop_labels'](labels, kwargs['seed_crop_map']),
        'u': helpers['_unit_labels'](pd.Series(['Gramme', 'Sac']), kwargs['unit_map']),
        'Quantity': [0, -7], 'Purchased': [True, False], 'Quantity_purchased': [5, np.nan]})
    got = helpers['_finish_plot_inputs'](frame, '2018-19')
    assert got.index.tolist() == [('2018-19', '1023', 'Seed', 'Arachide', kwargs['unit_map'].get('Gramme', 'Gramme')),
        ('2018-19', '12003', 'Seed', 'Autre tubercule', kwargs['unit_map'].get('Sac', 'Sac'))]
    assert got['Quantity'].tolist() == [0, -7] and list(got) == ['Quantity', 'Purchased', 'Quantity_purchased']
    assert got['Quantity'].dtype == pd.Float64Dtype() and got['Purchased'].dtype == pd.BooleanDtype()
    assert helpers['i'](pd.Series([1, 23])) == '1023'
    text = old.read_text()
    assert "src['s16bq05']" in text and "srcn['s16bq07a']" in text
    assert "srcn['s16bq09a']" not in text
