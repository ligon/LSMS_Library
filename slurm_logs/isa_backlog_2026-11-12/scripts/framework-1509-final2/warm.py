"""Warm-root check: read Uganda nutrition() from a root another arm built.

usage: warm.py OUT.json
Records the var/nutrition.parquet on disk before and after (mtime, size,
embedded lsms_cache_hash), the served frame's totals and invariance hash, and
the warnings.  A stale cache served as fresh keeps its mtime and hash; a
rebuild writes a new one.
"""
import hashlib
import json
import os
import sys
import time
import warnings

sys.path.insert(0, os.path.dirname(__file__))
import guard  # noqa: F401,E402

import pandas as pd  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
import lsms_library as ll  # noqa: E402
from lsms_library.paths import data_root  # noqa: E402

COUNTRY = os.environ.get('WARM_COUNTRY', 'Uganda')
p = data_root() / COUNTRY / 'var' / 'nutrition.parquet'


def state():
    if not p.exists():
        return {'exists': False}
    md = pq.read_schema(p).metadata or {}
    return {'exists': True, 'mtime': p.stat().st_mtime, 'size': p.stat().st_size,
            'lsms_cache_hash': (md.get(b'lsms_cache_hash') or b'').decode() or None}


rec = {'country': COUNTRY, 'arm': os.environ['ARM'], 'sha': os.environ['WANT_SHA'],
       'before': state(),
       'expected_hash': ll.Country(COUNTRY)._table_cache_hash(
           'nutrition', list(ll.Country(COUNTRY).waves))}
t0 = time.time()
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    n = ll.Country(COUNTRY).nutrition()
rec['seconds'] = round(time.time() - t0, 1)
rec['warnings'] = sorted({x.category.__name__ for x in w})
rec['after'] = state()
rec['rebuilt'] = rec['before'].get('mtime') != rec['after'].get('mtime')
num = n.select_dtypes('number')
rec['totals'] = {str(k): float(v) for k, v in num.sum().items()}
rec['shape'] = list(n.shape)
rec['invariance_hash'] = hashlib.sha256(
    pd.util.hash_pandas_object(n, index=True).values.tobytes()).hexdigest()
json.dump(rec, open(sys.argv[1], 'w'), indent=1, sort_keys=True, default=str)
print(json.dumps({k: rec[k] for k in ('before', 'after', 'rebuilt', 'seconds',
                                      'invariance_hash')}, default=str))
