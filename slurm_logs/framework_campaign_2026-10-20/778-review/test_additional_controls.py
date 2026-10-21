"""Independent #778 boundary and physical-fixture checks; synthetic data only."""
from pathlib import Path
import subprocess

import pytest
import lsms_library as ll
from lsms_library import country as C
from lsms_library.paths import countries_root, data_root
from tests.test_gh1040_wave_read_gates import acf, root, _plant, _FellThrough, CF_TABLE
from tests import test_guyana_index_uniqueness as guyana_tests
from tests import test_india_employment_activity_index as india_tests
from tests import test_gh866_stale_country_parquet_not_laundered as stale_tests


SOURCE = Path('/local/job40255479/worktrees/778-cache-race').resolve()
assert Path.cwd().resolve() == SOURCE
assert Path(ll.__file__).resolve().is_relative_to(SOURCE), ll.__file__
assert countries_root().resolve() == SOURCE / 'lsms_library/countries'
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == '3c97f20db8ca30c18f80a6aed681c76fc0a82aa3'


@pytest.mark.parametrize('coverage', ['full', 'subset', 'legacy'])
def test_real_reader_after_metadata_disappearance(acf, monkeypatch, coverage):
    make, path, frame, finalized = acf
    country = make()
    meta = {} if coverage == 'legacy' else {'cache_waves': list(country.waves)}
    _plant(path, frame, cache_hash='private', **meta)
    footer = C.read_parquet_cache_waves

    def read_footer_then_remove(candidate):
        covered = footer(candidate)
        candidate.unlink()
        return covered

    monkeypatch.setattr(C, 'read_parquet_cache_waves', read_footer_then_remove)
    waves = [country.waves[-1]] if coverage == 'subset' else list(country.waves)
    # Unlike a stubbed get_dataframe exception, this exercises the actual
    # sanctioned reader on the disappeared private file.
    with pytest.raises(_FellThrough):
        country._aggregate_wave_data(waves, CF_TABLE)
    assert finalized == [] and not path.exists()


@pytest.mark.parametrize('boundary', ['restriction', 'datetime_replay', 'finalization'])
def test_missing_dependency_outside_the_data_read_is_not_recovered(acf, monkeypatch, boundary):
    make, path, frame, finalized = acf
    country = make()
    _plant(path, frame, cache_hash='private', cache_waves=list(country.waves))
    error = FileNotFoundError('independent synthetic dependency at ' + boundary)

    def fail(*args, **kwargs):
        raise error

    if boundary == 'restriction':
        monkeypatch.setattr(country, '_restrict_to_waves', fail)
    elif boundary == 'datetime_replay':
        monkeypatch.setattr(C, 'replay_datetime_audit', fail)
    else:
        monkeypatch.setattr(C.Country, '_finalize_result', fail)
    with pytest.raises(FileNotFoundError) as caught:
        country._aggregate_wave_data(list(country.waves), CF_TABLE)
    assert caught.value is error and path.exists()
    assert finalized == []


@pytest.mark.parametrize('which', ['Guyana', 'India', 'Togo'])
@pytest.mark.parametrize('failure', [False, True])
def test_private_fixtures_dereference_l2_and_restore_after_failure(tmp_path, monkeypatch, which, failure):
    outer = tmp_path / 'outer'
    table = {'Guyana': 'household_roster', 'India': 'employment', 'Togo': 'plot_features'}[which]
    source_path = outer / which / 'var' / (table + '.parquet')
    source_path.parent.mkdir(parents=True)
    payload = tmp_path / 'synthetic-payload'
    payload.write_bytes(b'original synthetic cache bytes')
    source_path.symlink_to(payload)
    blobs = outer / 'dvc-cache'
    blobs.mkdir()
    monkeypatch.setenv('LSMS_DATA_DIR', str(outer))
    data_root.cache_clear()
    assert data_root() == outer
    temp = tmp_path / 'fixture'
    temp.mkdir()
    fixture = {'Guyana': guyana_tests.private_cold_root,
               'India': india_tests.private_cold_root,
               'Togo': stale_tests.private_country_root}[which]
    generator = fixture.__wrapped__(temp)
    isolated = next(generator)
    if which == 'Togo':
        copied = isolated(which, table)
        private = data_root()
    else:
        private = isolated
        copied = private / which / 'var' / (table + '.parquet')
    assert private != outer and data_root() == private
    assert not copied.is_symlink()
    assert copied.stat().st_ino != payload.stat().st_ino
    assert copied.read_bytes() == payload.read_bytes()
    assert (private / 'dvc-cache').is_symlink()
    assert (private / 'dvc-cache').resolve() == blobs.resolve()
    copied.write_bytes(b'changed private synthetic bytes')
    assert payload.read_bytes() == b'original synthetic cache bytes'
    if failure:
        with pytest.raises(RuntimeError, match='synthetic test failure'):
            generator.throw(RuntimeError('synthetic test failure'))
    else:
        generator.close()
    assert data_root() == outer
    assert source_path.is_symlink()
    assert payload.read_bytes() == b'original synthetic cache bytes'
    data_root.cache_clear()


@pytest.mark.parametrize('which', ['Guyana', 'India'])
def test_cold_fixture_has_no_warm_source_requirement(tmp_path, monkeypatch, which):
    outer = tmp_path / 'empty-outer'
    outer.mkdir()
    monkeypatch.setenv('LSMS_DATA_DIR', str(outer))
    data_root.cache_clear()
    temp = tmp_path / 'fixture'
    temp.mkdir()
    fixture = {'Guyana': guyana_tests.private_cold_root,
               'India': india_tests.private_cold_root}[which]
    generator = fixture.__wrapped__(temp)
    private = next(generator)
    assert data_root() == private and not (private / which).exists()
    generator.close()
    assert data_root() == outer
    data_root.cache_clear()
