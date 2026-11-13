"""An explicitly declared secondary source must not consume a primary ZIP member."""
from types import SimpleNamespace
from pathlib import Path

import pytest
from lsms_library import data_access as access, local_tools


@pytest.fixture
def routes(tmp_path, monkeypatch):
    countries = tmp_path / 'countries'
    doc = countries / 'Burkina_Faso/2021-22/Documentation/SOURCE.org'
    doc.parent.mkdir(parents=True)
    doc.write_text("https://microdata.worldbank.org/index.php/catalog/6224\n"
                   "#+CATALOG_ID: 6224, 6277\n"
                   "#+CATALOG_SUBDIR: 6277 Cross_Section\n"
                   "#+PROVENANCE_SOURCE: worldbank\n")
    monkeypatch.setattr(access, '_COUNTRIES_DIR', countries)
    monkeypatch.setattr(access, '_fetch_destination',
                        lambda path, populate_cache=False: ((countries if populate_cache else tmp_path / 'output') / path))
    monkeypatch.setattr(access, 'permissions', lambda *a: {'dvc': 'read', 'wb_api': 'read'})
    monkeypatch.setattr(local_tools, '_ensure_dvc_pulled', lambda *a: None)
    monkeypatch.setattr(local_tools, '_dvc_cache_path', lambda *a: None)
    monkeypatch.setattr(local_tools, 'DVCFS', SimpleNamespace(exists=lambda *a: False))
    monkeypatch.setattr(access, '_ACQUIRED_RELEASES', {})
    calls = []

    def api_key():
        calls.append(('key',))
        return 'invented-test-key'

    def idno(url):
        calls.append(('idno', url))
        return 'invented-release-' + url.rsplit('/', 1)[-1]

    def find(base, release, key):
        calls.append(('find', release))
        return ('https://invalid.example/invented.zip', 'invented.zip')

    def download(url, archive, filename, destination, key):
        calls.append(('download', filename))
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text('invented primary panel cover')
        return True

    monkeypatch.setattr(access.config, 'microdata_api_key', api_key)
    monkeypatch.setattr(access, '_get_catalog_idno', idno)
    monkeypatch.setattr(access, '_find_stata_zip_url', find)
    monkeypatch.setattr(access, '_download_and_extract', download)
    # All provider boundaries above are mocked; any unplanned network call fails.
    import socket
    monkeypatch.setattr(socket, 'create_connection', lambda *a, **k: pytest.fail('network'))
    monkeypatch.setattr(socket.socket, 'connect', lambda *a, **k: pytest.fail('network'))
    return tmp_path, countries, calls


@pytest.mark.parametrize('filename', ['s00_me_bfa2021.dta', 's01_me_bfa2021.dta',
                                     'ehcvm_ponderations_bfa2021.dta'])
def test_secondary_miss_refuses_primary_basename_before_credentials(routes, caplog, filename):
    root, countries, calls = routes
    relative = Path('Burkina_Faso/2021-22/Data/Cross_Section') / filename
    assert access.get_data_file(relative) is None
    assert calls == []
    assert not (root / 'output' / relative).exists()
    assert '6277' in caplog.text and '6224' in caplog.text and 'Cross_Section' in caplog.text


def test_primary_miss_keeps_normal_fallback(routes):
    root, countries, calls = routes
    relative = Path('Burkina_Faso/2021-22/Data/s00_me_bfa2021.dta')
    result = access.get_data_file(relative)
    assert result == root / 'output' / relative
    assert result.read_text() == 'invented primary panel cover'
    assert ('idno', 'https://microdata.worldbank.org/index.php/catalog/6224') in calls
    assert ('download', 's00_me_bfa2021.dta') in calls


def test_secondary_local_hit_needs_no_provenance_or_provider(routes, monkeypatch):
    root, countries, calls = routes
    relative = Path('Burkina_Faso/2021-22/Data/Cross_Section/s00_me_bfa2021.dta')
    existing = root / 'output' / relative
    existing.parent.mkdir(parents=True)
    existing.write_text('invented correct cross-section cover')
    monkeypatch.setattr(access, 'read_provenance', lambda *a: pytest.fail('local hit reached provenance'))
    assert access.get_data_file(relative) == existing
    assert calls == []


def test_secondary_dvc_hit_needs_no_provenance_or_provider(routes, monkeypatch):
    root, countries, calls = routes
    relative = Path('Burkina_Faso/2021-22/Data/Cross_Section/s00_me_bfa2021.dta')
    blob = root / 'invented-dvc-blob'
    blob.write_text('invented correct cross-section cover')
    monkeypatch.setattr(local_tools, '_dvc_cache_path', lambda *a: blob)
    monkeypatch.setattr(access, 'read_provenance', lambda *a: pytest.fail('DVC hit reached provenance'))
    result = access.get_data_file(relative)
    assert result == root / 'output' / relative
    assert result.read_text() == blob.read_text()
    assert calls == []


def test_explicit_matching_confined_acquisition_keeps_secondary_override(routes, monkeypatch):
    root, countries, calls = routes
    relative = Path('Burkina_Faso/2021-22/Data/Cross_Section/s00_me_bfa2021.dta')

    def extract(url, archive, destination, key, report, **kwargs):
        assert destination == countries / 'Burkina_Faso/2021-22/Data/Cross_Section'
        result = destination / relative.name
        result.parent.mkdir(parents=True, exist_ok=True)
        result.write_text('invented explicitly acquired cross-section')
        report.update(skipped=[], error=None, members=[result], not_placed=[])
        return [result]

    monkeypatch.setattr(access, '_extract_all_from_zip', extract)
    with local_tools.allow_intree_sources():
        result = access.get_data_file(relative, populate_cache=True,
                                      _source_url='https://microdata.worldbank.org/index.php/catalog/6277',
                                      _subdir='Cross_Section')
    assert result.read_text() == 'invented explicitly acquired cross-section'
    assert ('idno', 'https://microdata.worldbank.org/index.php/catalog/6277') in calls
    assert not any(c[0] == 'download' for c in calls)
