from pathlib import Path
import hashlib
import json
import os
import socket
import subprocess
import sys
import warnings
import pandas as pd
from pandas.testing import assert_frame_equal
import lsms_library as ll
from lsms_library.paths import countries_root, data_root
from lsms_library.local_tools import cache_freshness, read_parquet_cache_waves

ROOT = Path('/local/job40255479/worktrees/isa-integration')
CONTROL = Path('/local/job40255479/control/isa-backlog-20261021')
OUT = CONTROL / 'integration/mali-ten-increment-v4'
NINE = CONTROL / 'integration/api-nine-review'
LABOR = CONTROL / 'review-1121-Mali'
HEAD = 'bbdcf34578085fb90e981184a0cadadc9701a366'
PHASE = sys.argv[1]
assert PHASE in {'initial', 'warm'}
assert Path.cwd().resolve() == ROOT
assert Path(ll.__file__).resolve() == ROOT / 'lsms_library/__init__.py'
assert countries_root().resolve() == ROOT / 'lsms_library/countries'
assert data_root().resolve() == OUT / 'data'
assert subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip() == HEAD
assert not subprocess.check_output(['git', 'status', '--porcelain'])
assert not os.environ.get('LSMS_NO_CACHE') and not os.environ.get('LSMS_BUILD_BACKEND')
for method in ('connect', 'connect_ex'):
    assert getattr(socket.socket, method).__module__ == 'sitecustomize'
    with socket.socket() as sock:
        try:
            getattr(sock, method)(('192.0.2.1', 80))
        except OSError as error:
            assert 'forbids network' in str(error)
        else:
            raise AssertionError('inactive socket guard')

def state():
    return {str(p.relative_to(data_root())): [hashlib.sha256(p.read_bytes()).hexdigest(), p.stat().st_mtime_ns] for p in data_root().rglob('*.parquet')}

def exact(actual, expected):
    assert_frame_equal(actual, expected, check_exact=True)
    assert actual.attrs == expected.attrs

before = state()
cases = [
    ('Mali', 'plot_labor', 52057, LABOR / 'review-warm-Mali.private.pkl'),
    ('Niger', 'plot_labor', 53041, LABOR / 'review-warm-Niger.private.pkl'),
    ('Mali', 'community_prices', 155931, NINE / 'private-warm-Mali-community_prices.pkl'),
]
summary = {'head': HEAD, 'phase': PHASE, 'calls': {}, 'normal_no_bypass': True}
warnings_private = {}
for country, table, count, reference in cases:
    key = country + '-' + table
    expected = pd.read_pickle(reference)
    api = ll.Country(country)
    assert not api.assume_cache_fresh
    cache = data_root() / country / 'var' / (table + '.parquet')
    grade_before = cache_freshness(cache, api._table_cache_hash(table, api.waves)) if cache.exists() else 'absent'
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        actual = getattr(api, table)()
    warnings_private[key] = [{'class': type(w.message).__name__, 'message': str(w.message)} for w in caught]
    exact(actual, expected)
    assert len(actual) == count
    assert cache_freshness(cache, api._table_cache_hash(table, api.waves)) == 'fresh'
    assert set(read_parquet_cache_waves(cache)) == set(api.waves)
    if PHASE == 'warm':
        exact(actual, pd.read_pickle(OUT / ('initial-' + key + '-private.pkl')))
    actual.to_pickle(OUT / (PHASE + '-' + key + '-private.pkl'))
    summary['calls'][key] = {'rows': count, 'full_reference_frame_dtypes_attrs_exact': True, 'hash_grade_before': grade_before, 'hash_grade_after': 'fresh', 'all_wave_coverage': True}
    (OUT / (PHASE + '-progress.json')).write_text(json.dumps(summary, indent=2) + '\n')
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always')
    feature = ll.Feature('plot_labor')(['Mali', 'Niger'])
exact(feature, pd.read_pickle(LABOR / 'review-warm-Feature.private.pkl'))
assert len(feature) == 105098
warnings_private['Feature'] = [{'class': type(w.message).__name__, 'message': str(w.message)} for w in caught]
summary['Feature'] = {'rows': 105098, 'full_reference_frame_dtypes_attrs_exact': True}
after = state()
summary['parquets'] = len(after)
summary['changed_parquets'] = sum(before.get(name) != record for name, record in after.items())
summary['created_parquets'] = len(set(after) - set(before))
summary['deleted_parquets'] = len(set(before) - set(after))
if PHASE == 'warm':
    assert before == after
    summary['warm_zero_byte_mtime_writes'] = True
(OUT / (PHASE + '-warnings-private.json')).write_text(json.dumps(warnings_private, indent=2) + '\n')
(OUT / (PHASE + '-summary.json')).write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary), flush=True)
