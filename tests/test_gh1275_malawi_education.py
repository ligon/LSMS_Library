"""Use local Module C person IDs and keep only one shared interview."""

from pathlib import Path

import pandas as pd
import pytest

from lsms_library import Country, local_tools
from lsms_library.paths import data_root


WAVE = '2024-25'
TABLE = 'individual_education'
VALUE = 'Educational Attainment'
IHS_C = 'hh_mod_c.dta'
IHPS_C = 'hh_mod_c_24.dta'
COVER = 'hh_mod_a_filt_24.dta'


@pytest.fixture
def modules(tmp_path, monkeypatch):
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path))
    monkeypatch.delenv('LSMS_NO_CACHE', raising=False)
    data_root.cache_clear()
    inputs = {
        IHS_C: pd.DataFrame({
            'case_id': ['ihs-a', 'ihs-a', 'ihs-b'],
            'PID': [1, 2, 7],
            'hh_c09': ['NONE', None, 'PSLC'],
        }),
        IHPS_C: pd.DataFrame({
            'y5_hhid': ['ref', 'ref', 'ihs-b'],
            'id_code': [1, 2, 7],
            'PID': ['long-1', 'long-2', 'long-3'],
            'hh_c09': ['NONE', None, 'PhD'],
        }),
        COVER: pd.DataFrame({
            'y5_hhid': ['ref', 'ihs-b'],
            'y4_hhid': ['', 'prior-round'],
            'case_id': ['ihs-a', 'baseline-only'],
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


def test_keeps_local_person_keys_and_the_two_kinds_of_null(modules):
    wave, _, reads = modules
    df = wave.grab_data(TABLE)
    assert list(df.index.names) == ['t', 'i', 'pid']
    assert df.index.is_unique
    assert list(df.columns) == [VALUE]
    assert reads == [IHS_C, IHPS_C, COVER]
    assert df.loc[(WAVE, 'cs-24-ihs-a', '1'), VALUE] == 'No education'
    assert pd.isna(df.loc[(WAVE, 'cs-24-ihs-a', '2'), VALUE])
    assert df.loc[(WAVE, 'cs-24-ihs-b', '7'), VALUE] == 'Primary complete'
    assert df.loc[(WAVE, 'ihs-b', '7'), VALUE] == 'Doctorate'
    assert len(df) == 4

    # The existing Country finalizer drops the hollow row, not explicit NONE.
    c = wave.country
    c._sample_v_cache = pd.DataFrame(
        {'v': ['1', '2', '3']},
        index=pd.MultiIndex.from_tuples(
            [(WAVE, 'cs-24-ihs-a'), (WAVE, 'cs-24-ihs-b'), (WAVE, 'ihs-b')],
            names=['t', 'i']),
    )
    scheme = c.resources['Data Scheme'][TABLE]
    public = c._finalize_result(df.copy(), scheme, TABLE)
    assert len(public) == 3
    assert set(public[VALUE]) == {'No education', 'Primary complete', 'Doctorate'}
    assert 'v' in public.index.names
    pd.testing.assert_frame_equal(wave.grab_data(TABLE), df)
    assert reads == [IHS_C, IHPS_C, COVER]


@pytest.mark.parametrize('row,changed', [(0, 'PSLC'), (0, 'none'), (1, 'NONE')])
def test_refuses_original_c09_disagreement_even_before_harmonization(modules, row, changed):
    wave, inputs, _ = modules
    inputs[IHPS_C].loc[row, 'hh_c09'] = changed
    with pytest.raises(ValueError, match='qualifications disagree'):
        wave.grab_data(TABLE)


def test_missing_person_on_either_side_is_not_ignored_even_if_the_answer_was_blank(modules):
    wave, inputs, _ = modules
    inputs[IHPS_C] = inputs[IHPS_C].drop(index=1)
    with pytest.raises(ValueError, match='disagree on person keys'):
        wave.grab_data(TABLE)


def test_wrong_panel_local_id_is_not_replaced_with_a_longitudinal_guess(modules):
    wave, inputs, _ = modules
    inputs[IHPS_C].loc[1, 'id_code'] = 12
    with pytest.raises(ValueError, match='disagree on person keys'):
        wave.grab_data(TABLE)


@pytest.mark.parametrize('file', [IHS_C, IHPS_C])
def test_refuses_duplicate_person_keys(modules, file):
    wave, inputs, _ = modules
    inputs[file] = pd.concat([inputs[file], inputs[file].iloc[[0]]], ignore_index=True)
    with pytest.raises(ValueError, match='missing or duplicate person keys'):
        wave.grab_data(TABLE)


def test_refuses_a_panel_roster_without_its_cover_identity(modules):
    wave, inputs, _ = modules
    inputs[COVER] = inputs[COVER].iloc[:1]
    with pytest.raises(ValueError, match='disagree on household identity'):
        wave.grab_data(TABLE)


def test_cover_only_household_does_not_become_a_person(modules):
    wave, inputs, _ = modules
    inputs[IHPS_C] = inputs[IHPS_C].iloc[2:]
    with pytest.raises(ValueError, match='missing or duplicate person keys'):
        wave.grab_data(TABLE)


def test_refuses_two_panel_households_splitting_one_ihs6_roster(modules):
    wave, inputs, _ = modules
    inputs[IHPS_C].loc[1, 'y5_hhid'] = 'ref-second'
    inputs[COVER] = pd.concat([
        inputs[COVER],
        pd.DataFrame({'y5_hhid': ['ref-second'], 'y4_hhid': [''], 'case_id': ['ihs-a']}),
    ], ignore_index=True)
    with pytest.raises(ValueError, match='missing or duplicate IHS6 refresh counterparts'):
        wave.grab_data(TABLE)


def test_refuses_build_without_panel_cover(modules):
    wave, inputs, _ = modules
    del inputs[COVER]
    with pytest.warns(UserWarning, match='could not load sub-df'):
        with pytest.raises(ValueError, match='requires Module C and the IHPS panel cover'):
            wave.grab_data(TABLE)


def test_panel_cover_sidecar_is_an_education_cache_input(modules, monkeypatch):
    from lsms_library import country as country_module

    wave, _, _ = modules
    original = country_module.source_fingerprint
    wave_before = wave._input_hash(TABLE)
    country_before = wave.country._table_cache_hash(TABLE, wave.country.waves)

    def revised(path):
        fingerprint = original(path)
        return fingerprint + '-revised' if Path(path).name == COVER else fingerprint

    monkeypatch.setattr(country_module, 'source_fingerprint', revised)
    assert wave._input_hash(TABLE) != wave_before
    assert wave.country._table_cache_hash(TABLE, wave.country.waves) != country_before
