"""Consumed per-file overrides participate in dependency hashes, without IO."""
from types import SimpleNamespace

import pytest
import yaml

import lsms_library.country as cm
from lsms_library import local_tools as lt


@pytest.mark.parametrize('block, expected', [
    ({'file': 'plain.dta'}, ['plain.dta']),
    ({'file': ['plain.dta', {'panel.dta': {'i': 'panel_id'}},
               {'later.dta': {'t': '2020PH'}}, 'plain.dta']},
     ['plain.dta', 'panel.dta', 'later.dta', 'plain.dta']),
    ({'dfs': {'main': {'file': [{'panel.dta': {'i': 'id'}}]},
              'geo': {'file': 'geo.dta'}}}, ['panel.dta', 'geo.dta']),
    ({'file': [{'used.dta': {'file': 'variable-not-a-path.dta'},
               'ignored.dta': {'i': 'unused'}}]}, ['used.dta']),
    ({'myvars': {'Result': {'mapping': {'not-a-path.dta': 'value'}}}}, []),
    ({'myvars': {'file': 'column-not-a-path.dta'}}, []),
    ({'idxvars': {'file': 'id-column-not-a-path.dta'}}, []),
    ({'file': [None, 42, {}, {42: {}}, 'plain.dta']}, ['plain.dta']),
    ({'file': None}, []), ({}, []), (None, []),
    # A bare mapping is not the supported list-member override grammar.
    ({'file': {'unused.dta': {'i': 'id'}}}, []),
])
def test_declarations_match_consumed_paths(block, expected):
    assert list(lt._collect_file_paths_from_block(block)) == expected


@pytest.fixture
def metadata(tmp_path, monkeypatch):
    root = tmp_path / 'countries'
    package = tmp_path / 'package'
    (package / 'categorical_mapping').mkdir(parents=True)
    monkeypatch.setattr(cm, 'countries_root', lambda: root)
    monkeypatch.setattr(cm, 'files', lambda _: package)
    country_dir = root / 'Control'
    (country_dir / '_').mkdir(parents=True)
    (country_dir / '_/data_scheme.yml').write_text(
        "Country: Control\nWaves: ['2020']\nData Scheme:\n"
        "  facts: {index: '(t, i)', Result: float}\n")
    wave_dir = country_dir / '2020'
    (wave_dir / '_').mkdir(parents=True)
    (wave_dir / 'Data').mkdir()
    block = {'file': ['../Data/plain.dta',
                      {'../Data/panel.dta': {'i': 'panel_id',
                                             'file': 'column_not_path'}}],
             'idxvars': {'i': 'id'}, 'myvars': {'Result': 'result'}}
    (wave_dir / '_/data_info.yml').write_text(yaml.safe_dump({'facts': block}))

    def sidecar(name, digest):
        path = wave_dir / 'Data' / (name + '.dvc')
        path.write_text(yaml.safe_dump({'outs': [
            {'md5': digest, 'path': name, 'size': 1}]}))
        return path

    sidecar('plain.dta', '1' * 32)
    sidecar('panel.dta', '2' * 32)
    sidecar('unrelated.dta', '3' * 32)
    country = cm.Country('Control', preload_panel_ids=False)

    def denied(*args, **kwargs):
        raise AssertionError('metadata controls must not read/build/acquire data')

    monkeypatch.setattr(lt, 'get_dataframe', denied)
    monkeypatch.setattr(cm, 'get_dataframe', denied)
    monkeypatch.setattr(cm.Wave, 'grab_data', denied)
    monkeypatch.setattr(cm.Country, '_compute_panel_ids', denied)
    return country, country['2020'], sidecar, block


@pytest.mark.parametrize('source', ['plain.dta', 'panel.dta'])
def test_actual_wave_and_country_hash_track_each_declared_sidecar(metadata, source):
    country, wave, sidecar, _ = metadata
    before = wave._input_hash('facts'), country._table_cache_hash('facts', country.waves)
    assert all(isinstance(value, str) and len(value) == 64 for value in before)
    assert before == (wave._input_hash('facts'),
                      country._table_cache_hash('facts', country.waves))
    sidecar(source, '4' * 32)
    after = wave._input_hash('facts'), country._table_cache_hash('facts', country.waves)
    assert all(old != new for old, new in zip(before, after))
    assert after == (wave._input_hash('facts'),
                     country._table_cache_hash('facts', country.waves))
    sidecar('unrelated.dta', '5' * 32)
    assert after == (wave._input_hash('facts'),
                     country._table_cache_hash('facts', country.waves))
    assert not list(wave.file_path.rglob('*.dta'))


def test_actual_mapping_first_entry_and_override_payload_are_not_sources(metadata):
    country, wave, sidecar, block = metadata
    block['file'][1]['../Data/ignored.dta'] = {'i': 'never_used'}
    info = wave.file_path / '_/data_info.yml'
    info.write_text(yaml.safe_dump({'facts': block}, sort_keys=False))
    mapping = wave.column_mapping('facts', wave.resources['facts'])
    paths = set(mapping) - {'df_edit'}
    assert paths == {'../Data/plain.dta', '../Data/panel.dta'}
    assert mapping['../Data/panel.dta']['myvars']['file'] == 'column_not_path'
    assert set(lt._collect_file_paths_from_block(wave.resources['facts'])) == paths
    before = wave._input_hash('facts')
    sidecar('ignored.dta', '6' * 32)
    sidecar('column_not_path', '7' * 32)
    assert wave._input_hash('facts') == before


def test_missing_declared_source_becoming_tracked_changes_hash(metadata):
    _, wave, sidecar, _ = metadata
    (wave.file_path / 'Data/panel.dta.dvc').unlink()
    before = wave._input_hash('facts')
    sidecar('panel.dta', '2' * 32)
    assert wave._input_hash('facts') != before


def test_variable_maps_do_not_add_source_sidecar_dependencies(metadata):
    _, wave, sidecar, block = metadata
    original = wave._input_hash('facts')
    block['myvars']['file'] = '../Data/value_column.dta'
    block['idxvars']['file'] = '../Data/id_column.dta'
    info = wave.file_path / '_/data_info.yml'
    info.write_text(yaml.safe_dump({'facts': block}, sort_keys=False))
    expected = {'../Data/plain.dta', '../Data/panel.dta'}
    assert set(wave.column_mapping('facts', wave.resources['facts'])) - {'df_edit'} == expected
    assert set(lt._collect_file_paths_from_block(wave.resources['facts'])) == expected
    configured = wave._input_hash('facts')
    assert configured != original, 'the whole declaration YAML remains hashed'
    sidecar('value_column.dta', '8' * 32)
    sidecar('id_column.dta', '9' * 32)
    assert wave._input_hash('facts') == configured


@pytest.mark.parametrize('name', ['main', 'myvars', 'idxvars', 'file'])
def test_named_dfs_blocks_keep_sources_even_with_variable_field_names(metadata, name):
    country, wave, sidecar, block = metadata
    # grab_data resolves each dfs name in its parent block. The same spelling
    # can name a subframe here and a variable map inside that subframe.
    outer = {'dfs': [name], 'final_index': ['t', 'i'], 'merge_on': ['i'], name: block}
    info = wave.file_path / '_/data_info.yml'
    info.write_text(yaml.safe_dump({'facts': outer}, sort_keys=False))
    consumed = set(wave.column_mapping(name, wave.resources['facts'][name])) - {'df_edit'}
    assert consumed == {'../Data/plain.dta', '../Data/panel.dta'}
    assert set(lt._collect_file_paths_from_block(wave.resources['facts'])) == consumed
    before = wave._input_hash('facts'), country._table_cache_hash('facts', country.waves)
    sidecar('panel.dta', '8' * 32)
    after = wave._input_hash('facts'), country._table_cache_hash('facts', country.waves)
    assert all(old != new for old, new in zip(before, after))


def test_secondary_prewarm_caller_uses_overrides_with_fake_remote(metadata, tmp_path, monkeypatch):
    country, _, _, _ = metadata
    calls = []

    def fake_get(source, target):
        calls.append(source)
        # Invented cache bytes only; never a survey blob or real FS client.
        from pathlib import Path
        Path(target).write_bytes(b'invented test cache')

    remote = SimpleNamespace(path='/invented-remote', fs=SimpleNamespace(get_file=fake_get))
    fake_dvc = SimpleNamespace(repo=SimpleNamespace(
        config={'core': {'remote': 'fake'}},
        cloud=SimpleNamespace(get_remote=lambda **kwargs: remote)))
    monkeypatch.setattr(lt, 'DVCFS', fake_dvc)
    monkeypatch.setattr(lt, '_DVC_CACHE_DIR', tmp_path / 'invented-cache')
    lt._warm_dvc_cache_for_feature(country, 'facts', wave='2020')
    assert calls == ['/invented-remote/' + '1' * 2 + '/' + '1' * 30,
                     '/invented-remote/' + '2' * 2 + '/' + '2' * 30]
    lt._warm_dvc_cache_for_feature(country, 'facts', wave='2020')
    assert len(calls) == 2, 'existing fake blobs must not be fetched twice'
