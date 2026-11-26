"""Country L2 admission, audits and decode refer to the same opened entry."""
import os

import pandas as pd
import pytest

from lsms_library import country as C, local_tools as L
from tests.test_gh1040_wave_read_gates import root, _plant


WAVES = ['2000', '2001']


@pytest.fixture
def cache(root, monkeypatch):
    c = C.Country.__new__(C.Country)
    c.name = 'Snapshot1748'
    c.assume_cache_fresh = True
    c.wave_folder_map = {}
    monkeypatch.setattr(C.Country, 'waves', property(lambda self: WAVES))
    monkeypatch.setattr(C.Country, '_table_cache_hash', lambda *a: 'same-hash')
    return c, root / c.name / 'var/sample.parquet'


def plant(path, waves, marker, *, manifest=True):
    # Keep a null key to pin the all-wave pass-through (GH891).
    frame = pd.DataFrame({'i': ['hh'] * len(waves), 't': waves,
                          'marker': [marker] * len(waves)}).set_index(['i', 't'])
    stamp = [{'wave': '2000', 'marker': marker}]
    return _plant(path, frame, cache_hash='same-hash',
                  cache_waves=sorted(set(w for w in waves if w is not None)) if manifest else None,
                  grain_audit=stamp, datetime_audit=stamp, build_drops=stamp)


def read(c, path, route, waves=WAVES):
    if route == 'assumed':
        return c._read_assumed_fresh_cache('sample', waves)
    return c._read_l2_country_cache(
        'sample', path, waves, _recover_unreadable=route == 'normal',
        _borrowed_input=route == 'borrowed')


@pytest.mark.skipif(os.name == 'nt', reason='Windows may disallow replacing an open file')
@pytest.mark.parametrize('route', ['normal', 'assumed', 'borrowed'])
def test_country_cache_replacement_keeps_admitted_payload_and_audits(cache, monkeypatch, route):
    c, path = cache
    plant(path, [*WAVES, None], 1)
    replacement = plant(path.with_suffix('.new'), WAVES[:1], 2)
    old = path.stat().st_ino
    new = replacement.stat().st_ino
    footer, payload = C._CountryCacheFooter._country_coverage_manifest, pd.read_parquet
    events, streams = [], []

    def replace_after_admission(source):
        verified = footer(source)
        events.append(('coverage', verified))
        os.replace(replacement, path)
        return verified

    def record_payload(source, **kwargs):
        events.append(('payload', os.fstat(source.fileno()).st_ino))
        streams.append(source)
        return payload(source, **kwargs)

    def record_stamp(kind):
        def record(reports, *args, **kwargs):
            events.append((kind, reports[0]['marker']))
        return record

    def grade(drops, key):
        record_stamp('drops')(drops)
        return 'serve', None

    monkeypatch.setattr(C._CountryCacheFooter, '_country_coverage_manifest', replace_after_admission)
    monkeypatch.setattr(pd, 'read_parquet', record_payload)
    monkeypatch.setattr(C, '_replay_grain_audit', record_stamp('grain'))
    monkeypatch.setattr(C, 'replay_datetime_audit', record_stamp('date'))
    monkeypatch.setattr(C, '_degraded_cache_verdict', grade)
    monkeypatch.setattr(C, '_warn_degraded_serve',
                        lambda country, method, drops, path: record_stamp('warn')(drops))
    got = read(c, path, route)
    if route != 'assumed':
        got, covered, reason = got
        assert covered == WAVES and reason is None
    assert got['marker'].tolist() == [1, 1, 1]
    assert pd.isna(got.index.get_level_values('t')[-1])
    assert ('payload', old) in events and ('payload', new) not in events
    assert ('grain', 1) in events and ('date', 1) in events
    if route != 'assumed':
        assert ('drops', 1) in events and ('warn', 1) in events
    else:
        assert not any(kind in ('drops', 'warn') for kind, _ in events)
    assert len(streams) == 1 and streams[0].closed
    assert path.stat().st_ino == new and L.read_parquet_cache_waves(path) == WAVES[:1]


@pytest.mark.skipif(os.name == 'nt', reason='Windows may disallow replacing an open file')
@pytest.mark.parametrize('route', ['normal', 'assumed', 'borrowed'])
def test_country_corrupt_open_inode_cannot_evict_replacement(cache, monkeypatch, route):
    c, path = cache
    plant(path, WAVES, 1)
    replacement = plant(path.with_suffix('.new'), WAVES, 2)
    expected = replacement.read_bytes()
    error = OSError('synthetic old payload decode failure')
    streams = []

    def replace_during_decode(source, **kwargs):
        streams.append(source)
        os.replace(replacement, path)
        raise error

    reject = C._reject_unreadable_cache

    def reject_after_close(candidate, observed, failure):
        assert streams[0].closed
        assert candidate == path and observed.st_ino != path.stat().st_ino
        return reject(candidate, observed, failure)

    monkeypatch.setattr(C, '_reject_unreadable_cache', reject_after_close)
    monkeypatch.setattr(pd, 'read_parquet', replace_during_decode)
    if route == 'borrowed':
        with pytest.warns(UserWarning, match='leaving borrowed live input unchanged'):
            frame, covered, reason = read(c, path, route)
        assert frame is None and covered == WAVES and 'read-error' in reason
    else:
        with pytest.raises(RuntimeError, match='changed while rejecting') as exc:
            read(c, path, route)
        assert exc.value.__cause__ is error
    assert streams[0].closed and path.read_bytes() == expected


@pytest.mark.parametrize('route', ['normal', 'assumed', 'borrowed'])
def test_short_manifest_misses_without_payload_decode(cache, monkeypatch, route):
    c, path = cache
    plant(path, WAVES[:1], 1)
    original_open, sources = open, []

    def record_open(candidate, *args, **kwargs):
        stream = original_open(candidate, *args, **kwargs)
        if candidate == path:
            sources.append(stream)
        return stream

    def deny(*args, **kwargs):
        pytest.fail('coverage miss must not decode the payload')

    monkeypatch.setattr(C, 'open', record_open, raising=False)
    monkeypatch.setattr(pd, 'read_parquet', deny)
    got = read(c, path, route)
    if route == 'assumed':
        assert got is None
    else:
        frame, covered, reason = got
        assert frame is None and covered == WAVES[:1] and reason.startswith('coverage:')
    assert len(sources) == 1 and sources[0].closed


@pytest.mark.parametrize('route', ['normal', 'assumed'])
def test_full_legacy_without_manifest_keeps_null_key(cache, route):
    c, path = cache
    plant(path, [*WAVES, None], 1, manifest=False)
    got = read(c, path, route)
    if route == 'normal':
        got, covered, reason = got
        assert covered == WAVES and reason is None
    assert got['marker'].tolist() == [1, 1, 1]
    assert pd.isna(got.index.get_level_values('t')[-1])


@pytest.mark.parametrize('case', ['absent', 'bypassed'])
def test_ordinary_miss_without_open_or_payload(cache, monkeypatch, case):
    c, path = cache
    if case == 'bypassed':
        plant(path, WAVES, 1)
        monkeypatch.setenv('LSMS_NO_CACHE', '1')

    def deny(*args, **kwargs):
        pytest.fail('a known miss must not open the country cache')

    monkeypatch.setattr(C, 'open', deny, raising=False)
    assert read(c, path, 'normal') == (
        None, None, 'no parquet' if case == 'absent' else 'LSMS_NO_CACHE')
