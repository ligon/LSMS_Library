"""Selected plot-features compiler: real metadata and walk, invented leaves only."""
import pandas as pd
import pytest

from tests.ethiopia_declared_concat import CURRENT_WAVES, metadata_actor


def frames(wave):
    ids = [f'invented-{wave}-b', f'invented-{wave}-a', f'invented-{wave}-b']
    index = pd.MultiIndex.from_arrays(
        [[wave] * 3, ids, ['holder_2_field', 'holder_1_field', 'holder_2_field']],
        names=['t', 'i', 'plot_id'])
    native = pd.DataFrame({
        'Area': pd.array([0.0, pd.NA, 1.25], dtype='Float64'),
        'AreaUnit': pd.array(['Hectare', 'Unknown', 'Timad'], dtype='string'),
        'Tenure': pd.array(['owned', pd.NA, 'rented_in'], dtype='string'),
        'TenureSystem': pd.array([pd.NA] * 3, dtype='string'),
        'SoilType': pd.array([pd.NA] * 3 if wave == '2011-12' else ['Vertisols', pd.NA, 'Nitisols'], dtype='string'),
        'Irrigated': pd.array([False, pd.NA, True], dtype='boolean'),
        'Fallow': pd.array([True, False, pd.NA], dtype='boolean'),
        'ErosionProtection': pd.array([pd.NA, True, False], dtype='boolean'),
        'Certificate': pd.array([False, pd.NA, True], dtype='boolean'),
    }, index=index)
    native.attrs = {'invented': 'reported-plot-attributes-unknown-soil-and-tenure-system'}
    expected = native.copy(deep=True)
    if wave in ('2013-14', '2015-16'):
        expected.index = pd.MultiIndex.from_arrays(
            [[wave] * 3, ['invented-baseline-b', 'invented-baseline-a', 'invented-baseline-b'],
             index.get_level_values('plot_id')], names=index.names)
    return native, expected


@pytest.fixture(params=['script', 'yaml'])
def actor(request, monkeypatch, tmp_path, record_property):
    yield from metadata_actor(request, monkeypatch, tmp_path, record_property, 'plot_features', frames)


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
    failure = error_type('invented declared plot-features output failure')
    actor.frames[folders[position]] = failure
    with pytest.raises(error_type) as caught:
        actor.run()
    assert caught.value is failure
    assert actor.reads == folders[:position + 1]
    assert actor.map_reads == actor.writes == []


@pytest.mark.parametrize('retired', CURRENT_WAVES)
def test_each_retired_wave_skips_before_reader(actor, retired):
    actor.retire(retired)
    actor.run()
    actor.assert_output()


def test_all_retired_preserves_no_input_refusal_without_reader_map_or_writer(actor):
    for folder in list(actor.declared):
        actor.retire(folder)
    with pytest.raises(AssertionError, match='plot_features: no wave-level parquets found'):
        actor.run()
    assert actor.reads == actor.map_reads == actor.writes == []


def test_actual_folder_map_reads_physical_source_once(actor):
    actor.configure({CURRENT_WAVES[0]: 'invented-shared', CURRENT_WAVES[1]: 'invented-shared',
                     CURRENT_WAVES[2]: 'invented-third'})
    actor.run()
    actor.assert_output()
    assert len(actor.reads) == 4
