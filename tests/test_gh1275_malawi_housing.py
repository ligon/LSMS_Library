"""Malawi 2024-25 housing keeps one row per native interview."""

from pathlib import Path

import pandas as pd
import pytest

from lsms_library import Country, local_tools
from lsms_library.paths import data_root

WAVE = '2024-25'
TABLE = 'housing'
IHS_F = 'hh_mod_f.dta'
IHPS_F = 'hh_mod_f_24.dta'
COVER = 'hh_mod_a_filt_24.dta'


@pytest.fixture
def modules(tmp_path, monkeypatch):
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path))
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    data_root.cache_clear()
    inputs = {
        IHS_F: pd.DataFrame({
            'case_id': ['ihs-1', 'ihs-2'],
            'hh_f08': ['IRON SHEETS', None],
            'hh_f09': ['SMOOTHED MUD', 'WOOD'],
        }),
        IHPS_F: pd.DataFrame({
            # The retained ID can look like an unprefixed IHS6 case.
            'y5_hhid': ['ihps-copy', 'ihs-2'],
            'hh_f08': ['IRON SHEETS', 'GRASS'],
            'hh_f09': ['SMOOTHED MUD', 'SMOOTH CEMENT'],
        }),
        COVER: pd.DataFrame({
            'y5_hhid': ['ihps-copy', 'ihs-2'],
            'y4_hhid': ['', 'prior-round'],
            'case_id': ['ihs-1', 'baseline-only'],
        }),
    }
    reads = []

    def read(fn, **kwargs):
        name = Path(fn).name
        if name not in inputs:
            raise FileNotFoundError(name)
        reads.append(name)
        return inputs[name].copy()

    monkeypatch.setattr(local_tools, 'get_dataframe', read)
    yield Country('Malawi')[WAVE], inputs, reads
    data_root.cache_clear()


def test_uses_both_modules_and_keeps_the_ihs6_refresh_copy(modules):
    wave, _, reads = modules
    result = wave.grab_data(TABLE)
    assert list(result.index.names) == ['t', 'i']
    assert result.index.is_unique
    assert list(result.columns) == ['Roof', 'Floor']
    values = result.droplevel('t').sort_index()
    assert list(values.index) == ['cs-24-ihs-1', 'cs-24-ihs-2', 'ihs-2']
    assert values.loc['cs-24-ihs-1', 'Roof'] == 'IRON SHEETS'
    assert pd.isna(values.loc['cs-24-ihs-2', 'Roof'])
    assert values.loc['ihs-2', 'Roof'] == 'GRASS'
    assert values['Floor'].tolist() == ['SMOOTHED MUD', 'WOOD', 'SMOOTH CEMENT']
    assert reads == [IHS_F, IHPS_F, COVER]
    pd.testing.assert_frame_equal(wave.grab_data(TABLE), result)
    assert reads == [IHS_F, IHPS_F, COVER]


def test_matching_missing_material_is_not_filled(modules):
    wave, inputs, _ = modules
    inputs[IHS_F].loc[0, 'hh_f08'] = None
    inputs[IHPS_F].loc[0, 'hh_f08'] = None
    result = wave.grab_data(TABLE)
    assert pd.isna(result.loc[(WAVE, 'cs-24-ihs-1'), 'Roof'])
    assert result.loc[(WAVE, 'cs-24-ihs-1'), 'Floor'] == 'SMOOTHED MUD'


@pytest.mark.parametrize('column,value', [('hh_f08', None), ('hh_f09', 'SAND')])
def test_refuses_different_refresh_answers(modules, column, value):
    wave, inputs, _ = modules
    inputs[IHPS_F].loc[0, column] = value
    with pytest.raises(ValueError, match='refresh housing answers disagree'):
        wave.grab_data(TABLE)


@pytest.mark.parametrize('file', [COVER, IHPS_F])
def test_refuses_unmatched_panel_households(modules, file):
    wave, inputs, _ = modules
    inputs[file] = inputs[file].iloc[:1]
    with pytest.raises(ValueError, match='disagree on household membership'):
        wave.grab_data(TABLE)


@pytest.mark.parametrize('target', ['ghost', None])
def test_requires_a_real_ihs6_refresh_counterpart(modules, target):
    wave, inputs, _ = modules
    inputs[COVER].loc[0, 'case_id'] = target
    with pytest.raises(ValueError, match='missing or duplicate IHS6 refresh counterparts'):
        wave.grab_data(TABLE)


def test_refuses_two_refresh_copies_for_one_ihs6_interview(modules):
    wave, inputs, _ = modules
    inputs[COVER].loc[1, ['y4_hhid', 'case_id']] = ['', 'ihs-1']
    with pytest.raises(ValueError, match='missing or duplicate IHS6 refresh counterparts'):
        wave.grab_data(TABLE)


@pytest.mark.parametrize('file', [COVER, IHS_F, IHPS_F])
def test_refuses_duplicate_module_or_cover_keys(modules, file):
    wave, inputs, _ = modules
    inputs[file] = pd.concat([inputs[file], inputs[file].iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match='missing or duplicate household keys'):
        wave.grab_data(TABLE)


def test_refuses_build_without_panel_cover(modules):
    wave, inputs, _ = modules
    del inputs[COVER]
    with pytest.warns(UserWarning, match='could not load sub-df'):
        with pytest.raises(ValueError, match='requires Module F and the IHPS panel cover'):
            wave.grab_data(TABLE)


def test_panel_cover_is_a_housing_cache_input(modules, monkeypatch):
    from lsms_library import country as country_module

    wave, _, _ = modules
    original = country_module.source_fingerprint
    wave_before = wave._input_hash(TABLE)
    country_before = wave.country._table_cache_hash(TABLE, wave.country.waves)

    def revised(path):
        result = original(path)
        return result + '-revised' if Path(path).name == COVER else result

    monkeypatch.setattr(country_module, 'source_fingerprint', revised)
    assert wave._input_hash(TABLE) != wave_before
    assert wave.country._table_cache_hash(TABLE, wave.country.waves) != country_before
