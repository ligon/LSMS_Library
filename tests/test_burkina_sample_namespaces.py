"""Independent EMC/CS samples must not acquire EHCVM panel identities.

Exercises the shipped YAML through Wave.grab_data, including per-file weight
and ID overrides. Raw inputs here are invented; cached-source verification and
its independent key/weight oracle are recorded separately in the task report.
"""
import importlib.util
import runpy
import sys
import types

import pandas as pd
import pytest

from lsms_library import Country
from lsms_library import local_tools
from lsms_library.paths import countries_root

ROOT = countries_root() / 'Burkina_Faso'


def _module(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def raw_reader(monkeypatch):
    monkeypatch.setenv('LSMS_NO_CACHE', '1')
    frames = {
        's00_me_bfa2021.dta': pd.DataFrame({
            'grappe': [2, 2], 'menage': [1, 2], 's00q04': ['Rural', 'Rural']}),
        'ehcvm_panel_ponderations_bfa2021.dta': pd.DataFrame({
            'grappe': [2], 'hhweight2021': [100.], 'hhweight_panel': [150.]}),
        'Cross_Section/s00_me_bfa2021.dta': pd.DataFrame({
            'grappe': [20], 'menage': [126], 's00q04': ['Urbain'],
            # The cover's incidental weight must not shadow ponderations.
            'hhweight': [999.]}),
        'Cross_Section/ehcvm_ponderations_bfa2021.dta': pd.DataFrame({
            'grappe': [20], 'hhweight': [7.]}),
        'emc2014_welfare.dta': pd.DataFrame({
            'grappe': [2], 'menage': [1], 'hhweight': [9.],
            'strate': [1], 'milieu': ['Rural']}),
    }
    calls = []

    def read(path, **kwargs):
        key = str(path).split('/Data/')[-1]
        assert key in frames, key
        calls.append(key)
        return frames[key].copy()

    monkeypatch.setattr(local_tools, 'get_dataframe', read)
    return frames, calls


def test_sample_components_keep_own_weights_and_disjoint_ids(raw_reader):
    _, calls = raw_reader
    wave = Country('Burkina_Faso')['2021-22']
    df = wave.grab_data('sample').reset_index().set_index('i')
    assert set(df.index) == {'2001', '2002', 'CS_20126'}
    assert df.index.is_unique
    assert df.loc['2001', 'weight'] == df.loc['2002', 'weight'] == 100
    assert df.loc['2001', 'panel_weight'] == df.loc['2002', 'panel_weight'] == 150
    assert df.loc['CS_20126', 'weight'] == 7
    assert pd.isna(df.loc['CS_20126', 'panel_weight'])
    assert df.loc['CS_20126', 'v'] == '20'
    assert len(calls) == 4
    mapping18 = _module(ROOT / '2018-19/_/mapping.py', '_bf_namespace18')
    assert mapping18.i(pd.Series([2, 126])) == '20126'
    assert '20126' not in df.index


def test_emc_sample_cannot_link_to_ehcvm_draw(raw_reader):
    df = Country('Burkina_Faso')['2014'].grab_data('sample').reset_index()
    assert df.i.tolist() == ['EMC_2001']
    assert df.weight.tolist() == [9.]
    assert df.panel_weight.tolist() == [9.]
    mapping18 = _module(ROOT / '2018-19/_/mapping.py', '_bf_namespace18_other')
    assert mapping18.i(pd.Series([2, 1])) == '2001'


@pytest.mark.parametrize('script', ['food_coping', 'months_food_inadequate'])
def test_emc_script_consumers_share_sample_namespace(monkeypatch, script):
    """Changing only YAML i would orphan the two live script-path tables."""
    raw = pd.DataFrame({'zd': [2, 3], 'menage': [1, 2], 'SA4': ['Oui', 'Non']})
    for letter in 'ABCDE':
        raw['SA2' + letter] = [2, 0]
    for letter in 'ABCDEFGHIJKL':
        raw['SA5' + letter] = ['Oui', None]
    saved = {}
    monkeypatch.setattr(local_tools, 'get_dataframe', lambda *a, **k: raw.copy())
    monkeypatch.setattr(local_tools, 'to_parquet', lambda df, *a, **k: saved.update(frame=df))
    monkeypatch.syspath_prepend(str(ROOT / '_'))
    monkeypatch.chdir(ROOT / '2014/_')
    runpy.run_path(str(ROOT / '2014/_' / (script + '.py')))
    df = saved['frame']
    assert set(df.index.get_level_values('i')) == {'EMC_2001', 'EMC_3002'}
    assert df.index.is_unique
    if script == 'food_coping':
        assert len(df) == 10
        assert df['Days'].sum() == 10
    else:
        assert df['MonthsInadequate'].tolist() == [12, 0]
        assert df['AnyInadequate'].tolist() == [True, False]


def test_population_record_discloses_the_two_component_sample():
    """Do not describe the newly included cross-section as panel households."""
    rec = Country('Burkina_Faso').population['2021-22']
    assert rec.universe_tag == 'mixed-national+panel'
    assert 'sample and household_roster' in rec['survey']
    assert 'other tables still cover the panel only' in rec['survey']


@pytest.mark.parametrize('consumer', ['sample', 'food_coping', 'months_food_inadequate'])
def test_emc_helpers_bind_selected_config_not_cached_bare_module(monkeypatch, raw_reader, consumer):
    """A previously loaded country from another config root is not this source."""
    stale = types.ModuleType('burkina_faso')
    stale.__file__ = '/another-config-root/Burkina_Faso/_/burkina_faso.py'

    def wrong_helper(*args):
        raise AssertionError('used another config root country module')

    stale.emc_i = wrong_helper
    monkeypatch.setitem(sys.modules, 'burkina_faso', stale)
    if consumer == 'sample':
        frame = Country('Burkina_Faso')['2014'].grab_data('sample').reset_index()
        assert frame.i.tolist() == ['EMC_2001']
    else:
        test_emc_script_consumers_share_sample_namespace(monkeypatch, consumer)
