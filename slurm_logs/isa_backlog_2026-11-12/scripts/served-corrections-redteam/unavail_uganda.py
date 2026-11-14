"""Uganda GH #1536 sub-df outage: what is built and what is served from cache afterwards.
Usage: python unavail_uganda.py patched|unpatched   (same FRESH private root for both, in order)
Simulates FileNotFoundError for Uganda/2013-14/Data/GSEC1.dta only (the ea_2013_14 sub-df)."""
import collections
import json
import os
import sys
import warnings

import lsms_library
top = os.environ['LSMS_CLONE']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep)
from lsms_library.paths import data_root
assert os.path.realpath(str(data_root())).startswith(
    '/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-redteam/')
from lsms_library import local_tools as lt
import lsms_library as ll

mode = sys.argv[1]
hits = collections.Counter()
if mode == 'patched':
    _orig = lt.get_dataframe

    def patched(path, *a, **k):
        if '2013-14/Data/GSEC1' in str(path).replace('\\', '/'):
            hits['2013-14 GSEC1'] += 1
            raise FileNotFoundError(f'red-team simulated unavailable: {path}')
        return _orig(path, *a, **k)
    lt.get_dataframe = patched

key = 'Uganda::sample::ea-same-household'
res = {'mode': mode}
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always')
    try:
        df = ll.Country('Uganda').sample(waves=['2009-10'])
        x = df.xs('2009-10', level='t') if 't' in df.index.names else df
        st = x['Derivation'].astype(object).fillna('').astype(str).str.contains(key, regex=False)
        w = x['weight'].astype(float)
        res.update(ok=True, rows=len(x), ea_na=int(x['ea'].isna().sum()), stamped=int(st.sum()),
                   ea_na_weight_share=round(float(w[x['ea'].isna()].sum() / w.sum()), 5))
    except BaseException as e:
        res.update(ok=False, error=f'{type(e).__name__}: {str(e)[:500]}')
res['warnings'] = dict(collections.Counter(type(w.message).__name__ for w in caught))
res['warning_samples'] = [str(w.message)[:300] for w in caught
                          if any(s in str(w.message) for s in ('ea_2013_14', 'could not load', 'egraded', 'GSEC1'))][:4]
res['patched_calls'] = dict(hits)
try:
    from lsms_library.local_tools import read_parquet_cache_waves  # noqa
except Exception:
    pass
p = data_root() / 'Uganda' / 'var' / 'sample.parquet'
res['var_parquet_exists'] = p.exists()
try:
    import pyarrow.parquet as pq
    md = pq.read_schema(str(p)).metadata or {}
    res['var_parquet_meta_keys'] = sorted(k.decode() for k in md)
    drops = md.get(b'lsms_build_drops')
    res['lsms_build_drops_len'] = len(drops) if drops else 0
    res['lsms_build_drops_mentions_ea_2013_14'] = bool(drops and b'ea_2013_14' in drops)
except Exception as e:
    res['meta_error'] = f'{type(e).__name__}: {e}'
out = os.path.join(os.environ['RESULTS'], 'unavail_uganda.json')
prev = json.load(open(out)) if os.path.exists(out) else {}
prev[mode] = res
json.dump(prev, open(out, 'w'), indent=1)
print(json.dumps(res)[:3000], flush=True)
