"""Malawi 2024-25 Module B: refresh interviews are not a second roster."""

from pathlib import Path

import pandas as pd
import pytest

from lsms_library import Country, local_tools
from lsms_library.paths import data_root

WAVE = '2024-25'
TABLE = 'household_roster'
COVER = 'hh_mod_a_filt_24.dta'


@pytest.fixture
def fake_roster(tmp_path, monkeypatch):
    # Exercise the real YAML mappings and wave loader; no raw data is read.
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path))
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    data_root.cache_clear()
    answers = {
        'hh_b03': ['Male','Female','Female'],
        'hh_b04': ['Head','Child','Head'],
        'hh_b05a': [43,9,37],
        'hh_b07': [0,3,0],
    }
    sources = {
        'hh_mod_b.dta': pd.DataFrame({
            'case_id': ['ihs-1','ihs-1','ihs-2'],
            'PID': [1,2,1],
            **answers,
        }),
        'hh_mod_b_24.dta': pd.DataFrame({
            'y5_hhid': ['ihps-copy','ihps-copy','ihps-kept'],
            'id_code': [1,2,1],
            # The IHPS longitudinal PID is not the interview-local person ID.
            'PID': ['serial-1','serial-2','serial-3'],
            **answers,
        }),
        COVER: pd.DataFrame({
            'y5_hhid': ['ihps-copy','ihps-kept'],
            'y4_hhid': ['','earlier-round'],
            'case_id': ['ihs-1','baseline-case'],
        }),
    }
    reads = []

    def read(fn, **kwargs):
        name = Path(fn).name
        reads.append(name)
        if name not in sources:
            raise FileNotFoundError(name)
        return sources[name].copy()

    monkeypatch.setattr(local_tools,'get_dataframe',read)
    yield Country('Malawi')[WAVE], sources, reads
    data_root.cache_clear()


def test_source_roster_keeps_the_ihs6_copy_and_retained_panel(fake_roster):
    wave, _, reads = fake_roster
    result = wave.grab_data(TABLE)
    assert list(result.index.names) == ['t','i','pid']
    assert result.index.is_unique
    assert list(result.index) == [
        (WAVE,'cs-24-ihs-1','1'), (WAVE,'cs-24-ihs-1','2'),
        (WAVE,'cs-24-ihs-2','1'), (WAVE,'ihps-kept','1'),
    ]
    assert list(result['MonthsAway']) == [0,3,0,0]
    assert list(result['Age']) == [43,9,37,37]
    assert list(result.columns) == ['Sex', 'Age', 'Relationship', 'MonthsAway']
    assert reads == ['hh_mod_b.dta','hh_mod_b_24.dta',COVER]
    pd.testing.assert_frame_equal(result, wave.grab_data(TABLE))
    assert reads == ['hh_mod_b.dta','hh_mod_b_24.dta',COVER]


@pytest.mark.parametrize('bad_column,bad_value,message', [
    ('id_code', 3, 'disagree on person keys'),
    ('hh_b05a', 44, 'disagree on roster answers'),
])
def test_refuses_inconsistent_refresh_interview(fake_roster,bad_column,bad_value,message):
    wave,sources,_ = fake_roster
    sources['hh_mod_b_24.dta'].loc[0,bad_column] = bad_value
    with pytest.raises(ValueError,match=message):
        wave.grab_data(TABLE)


def test_refuses_a_roster_household_missing_from_cover(fake_roster):
    wave,sources,_ = fake_roster
    sources[COVER] = sources[COVER].iloc[:1]
    with pytest.raises(ValueError,match='disagree on household identity'):
        wave.grab_data(TABLE)


def test_refuses_to_build_without_the_panel_cover(fake_roster):
    wave,sources,_ = fake_roster
    del sources[COVER]
    with pytest.warns(UserWarning,match='could not load sub-df'):
        with pytest.raises(ValueError,match='requires the IHPS panel cover'):
            wave.grab_data(TABLE)


def test_panel_cover_fingerprint_is_a_wave_roster_input(fake_roster,monkeypatch):
    from lsms_library import country as country_module
    wave,_,_ = fake_roster
    original = country_module.source_fingerprint
    before = wave._input_hash(TABLE)
    country_before = wave.country._table_cache_hash(TABLE, wave.country.waves)

    def revised(path):
        out = original(path)
        return out + '-revised' if Path(path).name == COVER else out

    monkeypatch.setattr(country_module,'source_fingerprint',revised)
    assert wave._input_hash(TABLE) != before
    assert wave.country._table_cache_hash(TABLE, wave.country.waves) != country_before


def test_independent_cross_sectional_and_panel_ids_cannot_collide(fake_roster):
    wave, sources, _ = fake_roster
    sources[COVER].loc[1, 'y5_hhid'] = 'ihs-2'
    sources['hh_mod_b_24.dta'].loc[2, 'y5_hhid'] = 'ihs-2'
    result = wave.grab_data(TABLE)
    assert result.index.is_unique
    assert set(result.index.get_level_values('i')) == {
        'cs-24-ihs-1', 'cs-24-ihs-2', 'ihs-2',
    }


def test_duplicate_person_key_is_not_silently_collapsed(fake_roster):
    wave, sources, _ = fake_roster
    xs = sources['hh_mod_b.dta']
    sources['hh_mod_b.dta'] = pd.concat([xs, xs.iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match='missing or duplicate person keys'):
        wave.grab_data(TABLE)
