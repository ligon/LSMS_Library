"""Prewarm follows actual YAML Wave source paths; all IO/providers are invented."""
import os
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest
import yaml

import lsms_library.country as cm
from lsms_library import local_tools as lt


@pytest.fixture
def metadata(tmp_path, monkeypatch):
    root = tmp_path / 'countries'
    cdir = root / 'Control'
    (cdir / '_').mkdir(parents=True)
    (cdir / '_/data_scheme.yml').write_text(yaml.safe_dump({
        'Country': 'Control', 'Waves': ['2020'],
        'Data Scheme': {'facts': {'index': '(t, i)', 'Result': 'float'}},
    }))
    wave_dir = cdir / '2020'
    (wave_dir / '_').mkdir(parents=True)
    (wave_dir / 'Data').mkdir()
    monkeypatch.setattr(cm, 'countries_root', lambda: root)
    monkeypatch.setattr(lt, '_DVC_CACHE_DIR', tmp_path / 'fake-cache')

    def denied(*args, **kwargs):
        raise AssertionError('No real source reader, acquisition or panel build')

    monkeypatch.setattr(lt, 'get_dataframe', denied)
    monkeypatch.setattr(cm, 'get_dataframe', denied)
    monkeypatch.setattr(lt, '_ensure_dvc_pulled', denied)
    monkeypatch.setattr(cm.Country, '_compute_panel_ids', denied)
    country = cm.Country('Control', preload_panel_ids=False)

    def configure(files):
        block = {'file': files, 'idxvars': {'i': 'id'},
                 'myvars': {'Result': 'result'}}
        (wave_dir / '_/data_info.yml').write_text(
            yaml.safe_dump({'facts': block}, sort_keys=False))
        return block

    def sidecar(path, digest):
        source = Path(os.path.normpath(wave_dir / 'Data' / path))
        side = source.with_name(source.name + '.dvc')
        side.parent.mkdir(parents=True, exist_ok=True)
        side.write_text(yaml.safe_dump({'outs': [{'md5': digest,
                                                 'path': source.name}]}))
        return source

    return SimpleNamespace(country=country, wave=country['2020'],
                           directory=wave_dir, configure=configure,
                           sidecar=sidecar)


@pytest.fixture
def remote(tmp_path, monkeypatch):
    downloads, clients = [], []

    def get_file(source, target):
        downloads.append(source)
        Path(target).write_bytes(b'invented blob, never parsed as survey data')

    fs = SimpleNamespace(get_file=get_file)
    provider = SimpleNamespace(path='/fake-remote', fs=fs)

    def get_remote(**kwargs):
        clients.append(kwargs)
        return provider

    monkeypatch.setattr(lt, 'DVCFS', SimpleNamespace(repo=SimpleNamespace(
        config={'core': {'remote': 'fake'}},
        cloud=SimpleNamespace(get_remote=get_remote))))
    return SimpleNamespace(downloads=downloads, clients=clients, fs=fs)


@pytest.mark.parametrize('style', ['bare', 'relative', 'absolute', 'parent'])
def test_prewarm_and_actual_wave_provider_consume_same_paths(metadata, remote,
                                                           monkeypatch, tmp_path,
                                                           style):
    m = metadata
    prefix = {'bare': '', 'relative': '../Data/',
              'absolute': str(tmp_path / 'absolute') + '/',
              'parent': '../../shared/'}[style]
    plain, panel = prefix + 'plain.dta', prefix + 'panel.dta'
    m.configure([plain, {panel: {'i': 'panel_id'}}])
    wanted = [m.sidecar(plain, '1' * 32), m.sidecar(panel, '2' * 32)]
    m.sidecar(prefix + 'unrelated.dta', '3' * 32)
    before = m.wave._input_hash('facts')
    consumed = []

    def fake_grabber(path, idxvars, **kwargs):
        actual = Path(os.path.abspath(path))
        assert actual in wanted, 'unrelated source reached actual Wave provider'
        consumed.append((actual, idxvars['i'][0]))
        ordinal = wanted.index(actual)
        return pd.DataFrame({'Result': [10. + ordinal]},
                            index=pd.Index([str(ordinal)], name='i'))

    monkeypatch.setattr(cm, 'df_data_grabber', fake_grabber)
    # Actual loader and cache writer; only the source provider is fake.
    result = m.wave.grab_data('facts')
    assert consumed == [(wanted[0], 'id'), (wanted[1], 'panel_id')]
    assert result.index.names == ['t', 'i']
    assert result['Result'].tolist() == [10., 11.]
    lt._warm_dvc_cache_for_feature(m.country, 'facts', wave='2020')
    assert remote.clients == [{'name': 'fake'}]
    assert remote.downloads == ['/fake-remote/11/' + '1' * 30,
                                '/fake-remote/22/' + '2' * 30]
    assert m.wave._input_hash('facts') == before
    lt._warm_dvc_cache_for_feature(m.country, 'facts', wave='2020')
    assert len(remote.clients) == 1 and len(remote.downloads) == 2


@pytest.mark.parametrize('name', ['main', 'file', 'idxvars', 'myvars'])
def test_dfs_siblings_and_first_override_key_keep_source_scope(metadata, remote,
                                                             name):
    m = metadata
    block = m.configure(['plain.dta', {'panel.dta': {
        'i': 'panel_id', 'file': 'variable-value.dta'},
        'ignored.dta': {'i': 'ignored'}}])
    outer = {'dfs': [name], name: block,
             'merge_on': ['i'], 'final_index': ['t', 'i']}
    (m.directory / '_/data_info.yml').write_text(
        yaml.safe_dump({'facts': outer}, sort_keys=False))
    for path, digest in [('plain.dta', '1'), ('panel.dta', '2'),
                         ('ignored.dta', '3'), ('variable-value.dta', '4')]:
        m.sidecar(path, digest * 32)
    consumed = m.wave.column_mapping(name, m.wave.resources['facts'][name])
    assert list(consumed)[1:] == ['plain.dta', 'panel.dta']
    lt._warm_dvc_cache_for_feature(m.country, 'facts', wave='2020')
    assert remote.downloads == ['/fake-remote/11/' + '1' * 30,
                                '/fake-remote/22/' + '2' * 30]


def test_logical_wave_maps_use_physical_paths_and_deduplicate(metadata, remote):
    m = metadata
    resource = m.country.file_path / '_/data_scheme.yml'
    doc = yaml.safe_load(resource.read_text())
    doc['Waves'] = ['2020PP', '2020PH', '2021']
    doc['wave_folder_map'] = {'2020PP': '2020', '2020PH': '2020'}
    resource.write_text(yaml.safe_dump(doc))
    m.country.__dict__.pop('_resources_cache', None)
    m.configure(['plain.dta', {'panel.dta': {'i': 'panel_id'}}])
    m.sidecar('plain.dta', '1' * 32)
    m.sidecar('panel.dta', '1' * 32)
    other = m.country.file_path / '2021/_'
    other.mkdir(parents=True)
    (other / 'data_info.yml').write_text(
        yaml.safe_dump({'other_feature': {'file': 'unrelated.dta'}}))
    assert m.country['2020PH'].file_path == m.directory
    lt._warm_dvc_cache_for_feature(m.country, 'facts')
    assert remote.downloads == ['/fake-remote/11/' + '1' * 30]
    assert remote.clients == [{'name': 'fake'}]


@pytest.mark.parametrize('layout', ['legacy', 'modern'])
def test_existing_blob_never_constructs_remote_client(metadata, remote, layout):
    m = metadata
    m.configure([{'../Data/panel.dta': {'i': 'id'}}])
    m.sidecar('panel.dta', '2' * 32)
    base = lt._DVC_CACHE_DIR / ('files/md5' if layout == 'modern' else '')
    cached = base / '22' / ('2' * 30)
    cached.parent.mkdir(parents=True)
    cached.write_bytes(b'already present invented bytes')
    lt._warm_dvc_cache_for_feature(m.country, 'facts')
    assert remote.clients == [] and remote.downloads == []
    assert cached.read_bytes() == b'already present invented bytes'


def test_missing_and_malformed_sidecars_do_not_create_remote(metadata, remote):
    m = metadata
    m.configure(['../Data/missing.dta', '../Data/malformed.dta'])
    (m.directory / 'Data/malformed.dta.dvc').write_text('outs: [\n')
    lt._warm_dvc_cache_for_feature(m.country, 'facts')
    assert remote.clients == [] and remote.downloads == []


@pytest.mark.parametrize('failure', [FileNotFoundError, OSError])
def test_unavailable_blob_tries_both_layouts_and_continues(metadata, remote,
                                                        failure):
    m = metadata
    m.configure(['absent.dta', 'available.dta'])
    m.sidecar('absent.dta', '1' * 32)
    m.sidecar('available.dta', '2' * 32)

    def get_file(source, target):
        remote.downloads.append(source)
        if '/11/' in source:
            Path(target).write_bytes(b'invented partial download')
            raise failure('invented unavailable blob')
        Path(target).write_bytes(b'available invented bytes')

    remote.fs.get_file = get_file
    lt._warm_dvc_cache_for_feature(m.country, 'facts')
    assert remote.downloads == ['/fake-remote/11/' + '1' * 30,
                                '/fake-remote/files/md5/11/' + '1' * 30,
                                '/fake-remote/22/' + '2' * 30]
    assert (lt._DVC_CACHE_DIR / '22' / ('2' * 30)).is_file()
    assert not list(lt._DVC_CACHE_DIR.rglob('*.tmp.*'))


def test_competing_underscore_sidecar_is_not_the_yaml_source(metadata, remote):
    m = metadata
    m.configure('plain.dta')
    m.sidecar('plain.dta', '1' * 32)
    (m.directory / '_/plain.dta.dvc').write_text(
        yaml.safe_dump({'outs': [{'md5': '9' * 32}]}))
    lt._warm_dvc_cache_for_feature(m.country, 'facts')
    assert remote.downloads == ['/fake-remote/11/' + '1' * 30]


def test_normalization_matches_loader_and_hash_before_symlinks(metadata, remote,
                                                              tmp_path):
    m = metadata
    m.configure('alias/../plain.dta')
    m.sidecar('plain.dta', '1' * 32)
    outside = tmp_path / 'outside'
    (outside / 'child').mkdir(parents=True)
    try:
        (m.directory / 'Data/alias').symlink_to(outside / 'child',
                                               target_is_directory=True)
    except OSError:
        pytest.skip('This platform does not permit creating directory symlinks')
    (outside / 'plain.dta.dvc').write_text(
        yaml.safe_dump({'outs': [{'md5': '9' * 32}]}))
    loader_path = os.path.relpath(m.directory / 'Data/alias/../plain.dta')
    assert Path(os.path.abspath(loader_path)) == m.directory / 'Data/plain.dta'
    lt._warm_dvc_cache_for_feature(m.country, 'facts')
    assert remote.downloads == ['/fake-remote/11/' + '1' * 30]


def test_existing_relative_source_can_use_modern_remote_layout(metadata, remote):
    metadata.configure('../Data/plain.dta')
    metadata.sidecar('plain.dta', '1' * 32)

    def get_file(source, target):
        remote.downloads.append(source)
        if '/files/md5/' not in source:
            raise FileNotFoundError('invented old layout missing')
        Path(target).write_bytes(b'invented modern blob')

    remote.fs.get_file = get_file
    lt._warm_dvc_cache_for_feature(metadata.country, 'facts')
    assert remote.downloads == ['/fake-remote/11/' + '1' * 30,
                                '/fake-remote/files/md5/11/' + '1' * 30]
    assert (lt._DVC_CACHE_DIR / '11' / ('1' * 30)).is_file()


def test_remote_unavailable_is_best_effort(metadata, remote):
    metadata.configure('plain.dta')
    metadata.sidecar('plain.dta', '1' * 32)

    def unavailable(*args, **kwargs):
        raise RuntimeError('invented unavailable remote')

    remote.fs.get_file = unavailable
    assert lt._warm_dvc_cache_for_feature(metadata.country, 'facts') is None
    assert remote.clients == [{'name': 'fake'}]
