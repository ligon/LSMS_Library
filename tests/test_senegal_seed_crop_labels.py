"""GH #1119 (Senegal): maize and cowpea seed rows name the crop as crop_production does.

The Burkina Faso #1203 pattern.  ``harmonize_seed_crop`` used to send
``Semences de maïs`` / ``Semences de haricots/niébé`` to the FOOD labels
``Maïs en grain`` / ``Niébé/Haricots secs``, which Senegal's crop_production
never serves (its s16cq04 answers ``Maïs`` / ``Niébé`` pass through unmapped).
Only those two Preferred Label cells change; the map stays injective over the
slots the source uses.  The accented strings are source labels.
"""
import ast
import warnings
from pathlib import Path

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import df_from_orgfile
from lsms_library.paths import countries_root

ROOT = countries_root() / 'Senegal'
TARGETS = [('Semences de maïs', 'Maïs'), ('Semences de haricots/niébé', 'Niébé')]


def _table(name):
    return df_from_orgfile(ROOT / '_' / 'categorical_mapping.org', name=name)


def _seed_map():
    return _table('harmonize_seed_crop').set_index('Original Label')['Preferred Label'].to_dict()


def _helper(path, name):
    """Execute a production pure helper; no data build."""
    tree = ast.parse(Path(path).read_text())
    fn, = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name]
    env = {'pd': pd}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), str(path), 'exec'), env)
    return env[name]


@pytest.mark.parametrize('source,target', TARGETS)
def test_org_targets_use_the_crop_production_vocabulary(source, target):
    assert _seed_map()[source] == target


@pytest.mark.parametrize('source,target', TARGETS)
def test_seed_and_harvest_resolvers_agree(source, target):
    seed = _helper(ROOT / '2018-19/_/plot_inputs.py', '_seed_crop_labels')
    crop = _helper(ROOT / '2018-19/_/crop_production.py', '_crop_labels')
    food = _table('harmonize_food').set_index('Original Label')['Preferred Label'].to_dict()
    got_seed = seed(pd.Series([source], dtype='string'), _seed_map())
    # crop_production maps s16cq04 through harmonize_food and passes the
    # unmapped crop names through: the harvest label is the target itself.
    got_crop = crop(pd.Series([target], dtype='string'), food)
    pd.testing.assert_series_equal(got_seed, got_crop, check_exact=True)
    assert got_seed.tolist() == [target]


def test_every_other_cell_and_key_is_unchanged():
    expected = {
        "Semences d'arachide": 'Arachide', 'Semences de petit mil': 'Mil',
        'Semences de sorgho': 'Sorgho', 'Semences de riz': 'Riz Paddy',
        'Semences de coton': 'Coton', 'Semences de sésame': 'Sésame',
        'Semences de césame': 'Sésame', "Semences d'autres céréales": 'Autre céréale',
        'Plants/boutures de tubercules': 'Autre tubercule',
        'Autres semences': 'Autre crop', '20': 'Autre crop',
    }
    expected.update(TARGETS)
    assert _seed_map() == expected


def test_present_slots_stay_injective():
    present = ["Semences d'arachide", 'Semences de petit mil', 'Semences de sorgho',
               'Semences de maïs', 'Semences de riz', "Semences d'autres céréales",
               'Semences de coton', 'Semences de sésame', 'Semences de haricots/niébé',
               'Plants/boutures de tubercules', 'Autres semences']
    targets = [_seed_map()[label] for label in present]
    assert len(targets) == len(set(targets)) == len(present)


def test_harmonize_food_keeps_its_food_categories():
    food = _table('harmonize_food').set_index('Original Label')['Preferred Label'].to_dict()
    assert food['6. Maïs en grain'] == 'Maïs en grain'
    assert food['112. Niébé/Haricots secs'] == 'Niébé/Haricots secs'


@pytest.mark.requires_s3
def test_served_seed_rows_join_the_harvest_of_the_same_household():
    """(t, i, crop) existence join, 2018-19 (2021-22 has no crop_production).
    Measured 2026-11-13: maize 0 -> 723 of 735 seed rows, cowpea 0 -> 448 of 460."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        c = ll.Country('Senegal')
        pi = c.plot_inputs().reset_index()
        cp = c.crop_production().reset_index()
    seeds = pi[(pi['input'] == 'Seed') & (pi['t'] == '2018-19')]
    harvest = set(zip(cp['t'], cp['i'], cp['crop']))
    for label, rows, joined in [('Maïs', 735, 723), ('Niébé', 460, 448)]:
        sub = seeds[seeds['crop'] == label]
        assert len(sub) == rows
        assert sum(k in harvest for k in zip(sub['t'], sub['i'], sub['crop'])) == joined
    for old in ('Maïs en grain', 'Niébé/Haricots secs'):
        assert not pi['crop'].eq(old).any()
    # 2021-22 reads the same table.
    later = pi[(pi['input'] == 'Seed') & (pi['t'] == '2021-22')]
    assert later['crop'].eq('Maïs').sum() == 555 and later['crop'].eq('Niébé').sum() == 386
