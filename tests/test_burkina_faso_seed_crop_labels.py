"""GH1119: seed crop names join the native crop vocabulary without pooling slots.

Exact accented strings are source labels, not prose curation. The map's
unlabelled-code identity and non-crop marker remain separate contracts.
"""
import ast
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root


ROOT = countries_root() / 'Burkina_Faso'
UNKNOWN_21 = 'Unknown [Burkina_Faso 2018-19 s16bq01=21]'
TARGETS = [('Semences de maïs', 'Maïs'),
           ('Semences de haricots/niébé', 'Niébé')]


def _table(name):
    return df_from_orgfile(ROOT / '_' / 'categorical_mapping.org', name=name)


def _seed_map():
    return _table('harmonize_seed_crop').set_index(
        'Original Label')['Preferred Label'].to_dict()


def _helper(filename, name):
    """Execute the production pure helper; do not run a data build in this test."""
    tree = ast.parse((ROOT / '2018-19' / '_' / filename).read_text())
    fn, = [node for node in tree.body
           if isinstance(node, ast.FunctionDef) and node.name == name]
    env = {'pd': pd}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), filename, 'exec'), env)
    return env[name]


@pytest.mark.parametrize('source,target', TARGETS)
def test_actual_org_targets_use_crop_production_vocabulary(source, target):
    assert _seed_map()[source] == target


@pytest.mark.parametrize('source,target', TARGETS)
def test_real_resolvers_agree_on_seed_and_native_crop(source, target):
    seed = _helper('plot_inputs.py', '_seed_crop_labels')
    crop = _helper('crop_production.py', '_crop_labels')
    food = _table('harmonize_food').set_index(
        'Original Label')['Preferred Label'].to_dict()
    got_seed = seed(pd.Series([source], dtype='string'), _seed_map())
    got_crop = crop(pd.Series([target], dtype='string'), food)
    pd.testing.assert_series_equal(got_seed, got_crop, check_exact=True)
    assert got_seed.tolist() == [target]


def test_all_other_seed_cells_and_original_keys_are_preserved():
    expected = {
        'Semences de petit mil': 'Mil', 'Semences de sorgho': 'Sorgho',
        'Semences de riz': 'Riz Paddy', 'Semences de coton': 'Coton',
        'Semences de sésame': 'Sésame', 'Semences de césame': 'Sésame',
        "Semences d'autres céréales": 'Autre céréale',
        'Plants/boutures de tubercules': 'Autre tubercule',
        'Autres semences': 'Autre crop', '20': 'Autre crop', '21': UNKNOWN_21,
    }
    expected.update(TARGETS)
    assert _seed_map() == expected


def test_distinct_present_native_slots_remain_injective():
    # Present 2018 source slots 11..21; cesame and bare 20 are unused aliases.
    source = ['Semences de petit mil', 'Semences de sorgho',
              'Semences de maïs', 'Semences de riz',
              "Semences d'autres céréales", 'Semences de coton',
              'Semences de sésame', 'Semences de haricots/niébé',
              'Plants/boutures de tubercules', 'Autres semences', '21']
    targets = [_seed_map()[label] for label in source]
    assert len(targets) == len(set(targets)) == 11
    assert targets[-1] == UNKNOWN_21


def test_nonseed_and_unknown_labels_do_not_gain_crop_classifications():
    resolve = _helper('plot_inputs.py', '_seed_crop_labels')
    source = pd.Series(['Engrais inorganiques - Urée', 'not a seed label',
                        pd.NA], dtype='string')
    assert resolve(source, _seed_map()).isna().all()


def test_harmonize_food_keeps_its_distinct_food_categories():
    food = _table('harmonize_food').set_index(
        'Original Label')['Preferred Label'].to_dict()
    assert food['Maïs en grain'] == 'Maïs en grain'
    assert food['Maïs en épi'] == 'Maïs en épi'
    assert food['Niébé/Haricots secs'] == 'Haricots secs'


def test_unlabelled_slot_and_input_classification_stay_visible():
    inputs = _table('harmonize_input').set_index(
        'Original Label')['Preferred Label'].to_dict()
    assert inputs['21'] == 'Seed'  # Unverified legacy classification, GH1187.
    assert _seed_map()['21'] == UNKNOWN_21
    assert _seed_map()['Autres semences'] != UNKNOWN_21
