"""GH1196: both actual Wave read routes share parquet-only policy restoration."""
import os
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from lsms_library import country as C
from lsms_library import local_tools as lt
from lsms_library.paths import data_root
from tests.test_gh1196_parquet_index_policy import _exact, _frame


class _Rebuilt(Exception):
    pass


@pytest.fixture
def private_root(tmp_path, monkeypatch):
    monkeypatch.setenv('LSMS_DATA_DIR', str(tmp_path / 'data'))
    monkeypatch.setenv('LSMS_ISSUES_LOG', str(tmp_path / 'issues.log'))
    for name in ('LSMS_NO_CACHE', 'LSMS_GRAIN_STRICT', 'LSMS_BUILD_BACKEND'):
        monkeypatch.delenv(name, raising=False)
    data_root.cache_clear()
    for ledger in (C._DEGRADED_ATTEMPTED, C._SOURCE_PROBES,
                   C._BUILD_DROP_LEDGER, C._GRAIN_LEDGER):
        ledger.clear()
    yield
    data_root.cache_clear()


def _wave(route, monkeypatch):
    if route == 'yaml':
        country, year, table = 'Albania', '2002', 'household_roster'
    else:
        country, year, table = 'Uganda', '2019-20', 'food_acquired'
    c = C.Country(country, preload_panel_ids=False)
    w = c[year]
    assert bool(w.resources.get(table)) == (route == 'yaml')
    expected = w._input_hash(table)
    assert expected

    def rebuild(*args, **kwargs):
        raise _Rebuilt
    monkeypatch.setattr(C, 'df_data_grabber', rebuild)
    monkeypatch.setattr(C, '_build_wave_script_target', rebuild)
    assert w._input_hash(table) == expected
    path = data_root(country) / w.wave_folder / '_' / (table + '.parquet')
    return w, table, path, expected


def _native(wave, shape='multi', missing=True, policy=pd.NA):
    f = _frame(shape, missing, policy).rename_axis(index={'u': 'v'})
    if shape == 'multi':
        f.index = pd.MultiIndex.from_arrays(
            [pd.array([wave.year] * len(f), dtype='str'),
             f.index.get_level_values('v')], names=['t', 'v'])
    return f


def _plant(path, frame, expected, stamp):
    path.parent.mkdir(parents=True, exist_ok=True)
    return lt.to_parquet(frame, path, absolute_path=True,
                         **({'cache_hash': expected} if stamp else {}))


@pytest.mark.parametrize('route,shape', [('yaml', 'plain'), ('yaml', 'multi'), ('script', 'multi')])
@pytest.mark.parametrize('policy', [pd.NA, np.nan])
@pytest.mark.parametrize('missing', [False, True])
@pytest.mark.parametrize('stamp', [False, True])
def test_actual_wave_recorded_policy(private_root, monkeypatch, route, shape, policy, missing, stamp):
    w, table, path, expected = _wave(route, monkeypatch)
    prepared = _plant(path, _native(w, shape, missing, policy), expected, stamp)
    got = w.grab_data(table)
    _exact(prepared, got)
    assert lt.read_parquet_cache_hash(path) == (expected if stamp or route == 'yaml' else None)


@pytest.mark.parametrize('route', ['yaml', 'script'])
def test_wave_disappearance_after_selection(private_root, monkeypatch, route):
    w, table, path, expected = _wave(route, monkeypatch)
    _plant(path, _native(w), expected, True)
    grade = C.cache_freshness
    def disappear(candidate, *args):
        result = grade(candidate, *args)
        if Path(candidate) == path:
            path.unlink()
        return result
    monkeypatch.setattr(C, 'cache_freshness', disappear)
    with pytest.raises(_Rebuilt if route == 'yaml' else FileNotFoundError):
        w.grab_data(table)


@pytest.mark.parametrize('route', ['yaml', 'script'])
def test_wave_corrupt_payload_disposition(private_root, monkeypatch, route):
    w, table, path, _ = _wave(route, monkeypatch)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b'controlled non-parquet payload')
    with pytest.raises(_Rebuilt if route == 'yaml' else pa.ArrowInvalid):
        w.grab_data(table)


@pytest.mark.parametrize('route', ['yaml', 'script'])
@pytest.mark.parametrize('error_type', [OSError, pa.ArrowInvalid])
def test_wave_payload_errors_keep_original_disposition(private_root, monkeypatch, route, error_type):
    w, table, path, expected = _wave(route, monkeypatch)
    _plant(path, _native(w), expected, True)
    error = error_type('controlled payload failure')
    def fail(*args, **kwargs):
        raise error
    monkeypatch.setattr(pd, 'read_parquet', fail)
    with pytest.raises(_Rebuilt if route == 'yaml' else error_type) as result:
        w.grab_data(table)
    if route == 'script':
        assert result.value is error


@pytest.mark.parametrize('route', ['yaml', 'script'])
@pytest.mark.parametrize('error_type', [OSError, pa.ArrowInvalid])
def test_wave_new_footer_errors_are_loud(private_root, monkeypatch, route, error_type):
    w, table, path, expected = _wave(route, monkeypatch)
    _plant(path, _native(w), expected, True)
    error = error_type('controlled same-stream footer failure')
    read_schema = pq.read_schema
    def fail(stream, *args, **kwargs):
        if not isinstance(stream, (str, Path)):
            raise error
        return read_schema(stream, *args, **kwargs)
    monkeypatch.setattr(pq, 'read_schema', fail)
    with pytest.raises(error_type) as result:
        w.grab_data(table)
    assert result.value is error


@pytest.mark.parametrize('route', ['yaml', 'script'])
def test_wave_restore_programmer_error_is_loud(private_root, monkeypatch, route):
    w, table, path, expected = _wave(route, monkeypatch)
    _plant(path, _native(w), expected, True)
    error = TypeError('controlled restoration bug')
    def fail(*args, **kwargs):
        raise error
    monkeypatch.setattr(lt, '_restore_parquet_index_string_policy', fail)
    with pytest.raises(TypeError) as result:
        w.grab_data(table)
    assert result.value is error


@pytest.mark.parametrize('route', ['generic', 'yaml', 'script'])
def test_same_stream_survives_actual_atomic_path_replacement(private_root, tmp_path, monkeypatch, route):
    if route == 'generic':
        path, expected = tmp_path / 'target.parquet', 'synthetic'
        original = _frame()
        invoke = lambda: lt.get_dataframe(path)
    else:
        w, table, path, expected = _wave(route, monkeypatch)
        original = _native(w)
        invoke = lambda: w.grab_data(table)
    prepared = _plant(path, original, expected, True)
    replacement = path.with_name('replacement.parquet')
    other = original.copy(deep=True)
    other.iloc[0, 0] = 9876.
    _plant(replacement, other, expected, True)
    reader, schema = pd.read_parquet, pq.read_schema
    seen = []
    def replace_after_payload(stream, *args, **kwargs):
        assert not isinstance(stream, (str, Path)) and not stream.closed
        seen.append(stream)
        if route == 'generic':
            assert kwargs == {'engine': 'pyarrow'}
        else:
            assert not kwargs
        result = reader(stream, *args, **kwargs)
        os.replace(replacement, path)
        return result
    def footer(stream, *args, **kwargs):
        if not isinstance(stream, (str, Path)):
            assert stream is seen[-1] and not stream.closed
        return schema(stream, *args, **kwargs)
    monkeypatch.setattr(pd, 'read_parquet', replace_after_payload)
    monkeypatch.setattr(pq, 'read_schema', footer)
    _exact(prepared, invoke())
    assert len(seen) == 1 and seen[0].closed
    assert reader(path).iloc[0, 0] == 9876.


@pytest.mark.parametrize('error_type', [OSError, pa.ArrowInvalid, TypeError])
@pytest.mark.parametrize('caught', [False, True])
def test_shared_reader_returns_only_requested_payload_error(tmp_path, monkeypatch, error_type, caught):
    path = tmp_path / 'input.parquet'
    _plant(path, _frame(), 'synthetic', True)
    error = error_type('controlled payload error object')
    def fail(*args, **kwargs):
        raise error
    monkeypatch.setattr(pd, 'read_parquet', fail)
    with path.open('rb') as stream:
        if caught:
            frame, actual = lt._read_parquet_with_index_policy(stream, read_errors=(error_type,))
            assert frame is None and actual is error and not stream.closed
        else:
            with pytest.raises(error_type) as result:
                lt._read_parquet_with_index_policy(stream)
            assert result.value is error and not stream.closed
