"""New retirement controls, actual APIs/walk with private invented leaves only."""
import pandas as pd
import pytest

from tests.ethiopia_retirement_concat import CURRENT_WAVES, metadata_actor


def frames(wave):
    index = pd.MultiIndex.from_arrays(
        [[wave] * 3, [f'invented-{wave}-b', f'invented-{wave}-a', f'invented-{wave}-b'],
         ['02', '01', '02']], names=['t', 'i', 'pid'])
    native = pd.DataFrame({
        'Weight': pd.array([12.25 + CURRENT_WAVES.index(wave), pd.NA, 13.5 + CURRENT_WAVES.index(wave)], dtype='Float64'),
        'Height': pd.array([pd.NA, 85.5 + CURRENT_WAVES.index(wave), 86.0 + CURRENT_WAVES.index(wave)], dtype='Float64'),
        'MUAC': pd.array([14.0, 15.0, pd.NA], dtype='Float64'),
        'Age_months': pd.array([24, 36, 24], dtype='Int64'),
        'Sex': pd.array(['F', 'M', 'F'], dtype='string'),
    }, index=index)
    native.attrs = {'synthetic_source': 'invented-child-measures'}
    expected = native.copy(deep=True)
    if wave in ('2013-14', '2015-16'):
        expected.index = pd.MultiIndex.from_arrays(
            [[wave] * 3, ['invented-baseline-b', 'invented-baseline-a', 'invented-baseline-b'],
             index.get_level_values('pid')], names=index.names)
    return native, expected


@pytest.fixture(params=['script', 'yaml', 'union'])
def actor(request, monkeypatch, tmp_path, record_property):
    yield from metadata_actor(request, monkeypatch, tmp_path, record_property, 'anthropometry', frames)


@pytest.mark.parametrize('empty', ['none', 'one', 'all'])
def test_declared_populated_and_typed_empty_frames_keep_actual_walk(actor, empty):
    for position, folder in enumerate(actor.expected_reads()):
        if empty == 'all' or (empty == 'one' and position == 2):
            actor.frames[folder] = actor.frames[folder].iloc[:0]
            actor.expected[folder] = actor.expected[folder].iloc[:0]
    actor.run()
    actor.assert_output()


@pytest.mark.parametrize('position,error_type', [
    (0, FileNotFoundError), (2, FileNotFoundError), (4, FileNotFoundError),
    (2, ValueError), (2, PermissionError), (2, RuntimeError)])
def test_declared_read_failure_preserves_cause_before_map_or_write(actor, position, error_type):
    folders = actor.expected_reads()
    cause = OSError('invented underlying cause')
    failure = error_type('invented declared anthropometry output failure')
    failure.__cause__ = cause
    actor.frames[folders[position]] = failure
    with pytest.raises(error_type) as caught:
        actor.run()
    assert caught.value is failure
    assert caught.value.__cause__ is cause
    assert actor.reads == folders[:position + 1]
    assert actor.map_reads == actor.writes == actor.walks == []


@pytest.mark.parametrize('retired', CURRENT_WAVES)
def test_each_retired_wave_skips_before_reader(actor, retired):
    actor.retire(retired)
    # Stale output presence cannot rewire a retired declaration. Empty marker only.
    (actor.home / retired / '_/anthropometry.parquet').touch()
    actor.run()
    actor.assert_output()


def test_all_retired_preserves_no_input_refusal_without_reader_map_or_writer(actor):
    for folder in list(actor.declared):
        actor.retire(folder)
    with pytest.raises(AssertionError, match='anthropometry: no wave-level parquets found'):
        actor.run()
    assert actor.reads == actor.map_reads == actor.writes == actor.walks == []


def test_actual_folder_map_reads_physical_source_once(actor):
    actor.configure({CURRENT_WAVES[0]: 'invented-shared', CURRENT_WAVES[1]: 'invented-shared',
                     CURRENT_WAVES[2]: 'invented-third'})
    actor.run()
    actor.assert_output()
    assert len(actor.reads) == 4


@pytest.mark.parametrize('actor', ['union'], indirect=True)
def test_native_yaml_union_keeps_other_declaration_when_one_is_retired(actor):
    # Actual Wave.data_scheme must still see YAML after native retirement.
    (actor.home / CURRENT_WAVES[0] / '_/anthropometry.py').unlink()
    # And native after YAML retirement; no invented null-precedence semantics.
    (actor.home / CURRENT_WAVES[1] / '_/data_info.yml').write_text(
        'Country: Ethiopia\nWave: ' + CURRENT_WAVES[1] + '\n')
    actor.run()
    actor.assert_output()
