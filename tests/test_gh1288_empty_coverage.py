"""Explicit no-completed-wave evidence is not pre891 missing metadata."""
import hashlib
import inspect
import json
from pathlib import Path
import shlex
import subprocess
import sys
import warnings
from types import SimpleNamespace

import pandas as pd
import pyarrow.parquet as pq
import pytest

from lsms_library import country as C, local_tools as L, _build_registry as R
from lsms_library.paths import data_root
from tests.test_gh1590_wave_failure_manifest import survey, BAD, NAME

REAL_RUN = subprocess.run


@pytest.mark.parametrize('coverage', [None, [], ['2001']])
@pytest.mark.parametrize('cache_hash', [None, 'current'])
def test_writer_preserves_absent_empty_and_nonempty_coverage(tmp_path, coverage, cache_hash):
    frame = pd.DataFrame({'x': pd.array([1.0, None], dtype='Float64')},
                         index=pd.Index([11, 12], name='i'))
    frame.attrs = {'provenance': {'invented': True}}
    before = frame.copy(deep=True)
    path = tmp_path / 'coverage.parquet'
    L.to_parquet(frame, path, absolute_path=True, cache_hash=cache_hash,
                 cache_waves=coverage)
    metadata = pq.read_schema(path).metadata or {}
    assert (b'lsms_cache_waves' in metadata) == (coverage is not None)
    if coverage is not None:
        assert metadata[b'lsms_cache_waves'] == json.dumps(coverage).encode()
    assert L.read_parquet_cache_waves(path) == coverage
    assert L.read_parquet_cache_hash(path) == cache_hash
    loaded = pd.read_parquet(path)
    pd.testing.assert_frame_equal(loaded, before, check_exact=True)
    assert loaded.attrs == before.attrs
    pd.testing.assert_frame_equal(frame, before, check_exact=True)
    assert frame.attrs == before.attrs
    c = SimpleNamespace(waves=['2001', '2002'])
    assert C.Country._cache_covers_waves(c, path, ['2001'], 'fresh') == (
        c.waves if coverage is None else coverage)


def test_failed_legacy_make_empty_coverage_rebuilds_and_recovers(
        survey, monkeypatch, tmp_path, capfd):
    """Keep available old data, but never certify it as a completed warm build."""
    survey.script()
    survey.fault = 'missing'
    country = survey.country()
    # Same registry source-simulation mechanism as test_build_transform_hash:
    # reverse precisely the two writer guards to version the real parent logic.
    real_source = inspect.getsource
    target = inspect.unwrap(L.to_parquet)
    source = real_source(target)
    assert source.count('cache_waves is not None') == 2
    previous_source = source.replace('cache_waves is not None', 'cache_waves')
    try:
        with monkeypatch.context() as m:
            m.setattr(inspect, 'getsource',
                      lambda fn: previous_source if fn is target else real_source(fn))
            R.clear_caches()
            old_hash = country._table_cache_hash('facts', [BAD])
        R.clear_caches()
        current_hash = country._table_cache_hash('facts', [BAD])
        assert old_hash is not None and current_hash is not None
        assert old_hash != current_hash

        cache = survey.cache
        old = pd.DataFrame({'t': [BAD], 'i': ['synthetic'], 'x': [99.0]}).set_index(['t', 'i'])
        cache.parent.mkdir(parents=True, exist_ok=True)
        # The old [] writer emitted exactly this no-manifest representation.
        L.to_parquet(old, cache, absolute_path=True, cache_hash=old_hash)
        assert L.cache_freshness(cache, current_hash) == 'stale'
        wave = data_root() / NAME / BAD / '_' / 'facts.parquet'
        makefile = survey.country_path / BAD / '_' / 'Makefile'
        makefile.write_text(f'{wave}: FORCE\n\t@echo EMPTY_COVERAGE_EXIT7 >&2\n\t@exit 7\n.PHONY: FORCE\nFORCE:\n')
        calls = []

        def actual(argv, *args, **kwargs):
            assert argv[0] == 'make'
            assert Path(argv[-1]) in [wave, cache]
            record = {'target': str(argv[-1])}
            calls.append(record)
            try:
                result = REAL_RUN(argv, *args, **kwargs)
                record['exit'] = result.returncode
                return result
            except subprocess.CalledProcessError as error:
                record['exit'] = error.returncode
                raise

        monkeypatch.setattr(C.subprocess, 'run', actual)
        expected = old.reorder_levels(['i', 't'])
        expected['x'] = expected['x'].astype('Float64')
        for expected_calls in (2, 4):
            with warnings.catch_warnings(record=True) as seen:
                warnings.simplefilter('always')
                out = survey.load([BAD], country=country)
            pd.testing.assert_frame_equal(out, expected, check_exact=True)
            assert len(calls) == expected_calls
            assert [x['exit'] for x in calls[-2:]] == [2, 0]
            assert any('Makefile execution failed' in str(x.message) for x in seen)
            assert L.read_parquet_cache_hash(cache) == current_hash
            assert L.read_parquet_cache_waves(cache) == []
            assert pq.read_schema(cache).metadata[b'lsms_cache_waves'] == b'[]'
        assert 'EMPTY_COVERAGE_EXIT7' in capfd.readouterr().err
        # Repair only the invented producer; the next miss recovers genuine
        # coverage and the following warm call must execute no further Make.
        writer = tmp_path / 'healthy-writer.py'
        writer.write_text("import pandas as pd\nfrom pathlib import Path\np=Path(" + repr(str(wave)) + ")\np.parent.mkdir(parents=True,exist_ok=True)\npd.DataFrame({'t':['" + BAD + "'],'i':['synthetic'],'x':[7.0]}).set_index(['t','i']).to_parquet(p)\n")
        makefile.write_text(f'{wave}: FORCE\n\t{shlex.quote(sys.executable)} {shlex.quote(str(writer))}\n.PHONY: FORCE\nFORCE:\n')
        recovered = survey.load([BAD], country=country)
        pd.testing.assert_frame_equal(recovered, expected.assign(x=pd.array([7.0], dtype='Float64')), check_exact=True)
        assert len(calls) == 5 and calls[-1]['exit'] == 0
        assert L.read_parquet_cache_waves(cache) == [BAD]
        before = hashlib.sha256(cache.read_bytes()).hexdigest()
        warm = survey.load([BAD], country=country)
        pd.testing.assert_frame_equal(warm, recovered, check_exact=True)
        assert warm.attrs == recovered.attrs
        assert len(calls) == 5
        assert hashlib.sha256(cache.read_bytes()).hexdigest() == before
    finally:
        R.clear_caches()
