"""GH #1261: cold country-level build of Nigeria cluster_features and its
consumers, for a development-vs-branch comparison.

    python country_build.py <checkout> <frames_dir> <summary.json>

Run in a FRESH ``LSMS_DATA_DIR``.  Row-level frames are written to
``<frames_dir>`` (keep it OUT of the repository: it holds coordinates); the
summary JSON holds aggregates only (shapes, dtypes, digests, grain reports,
warning counts).
"""
import collections
import hashlib
import json
import os
import subprocess
import sys
import warnings

checkout, frames_dir, out = sys.argv[1], sys.argv[2], sys.argv[3]
import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402
from lsms_library import country as cmod  # noqa: E402

top = subprocess.check_output(['git', '-C', checkout, 'rev-parse', '--show-toplevel'],
                              text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
assert not any(os.scandir(os.environ['LSMS_DATA_DIR'] + '/Nigeria')) if os.path.isdir(
    os.environ['LSMS_DATA_DIR'] + '/Nigeria') else True, 'LSMS_DATA_DIR is not cold'
os.makedirs(frames_dir, exist_ok=True)


def digest(df):
    h = pd.util.hash_pandas_object(df.reset_index(), index=False)
    return hashlib.sha256(h.values.tobytes()).hexdigest()[:16]


def summ(name, df):
    df.to_parquet(os.path.join(frames_dir, f'{name}.parquet'))
    s = {'shape': list(df.shape), 'index': list(df.index.names),
         'dtypes': {k: str(v) for k, v in df.dtypes.items()},
         'index_dtypes': {str(n): str(df.index.get_level_values(n).dtype)
                          for n in df.index.names},
         'digest': digest(df)}
    if 't' in df.index.names:
        s['rows_by_t'] = {str(k): int(v) for k, v in
                          df.index.get_level_values('t').value_counts().sort_index().items()}
    return s


res = {'checkout': top, 'head': subprocess.check_output(
    ['git', '-C', checkout, 'rev-parse', 'HEAD'], text=True).strip()}
caught = []
with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    c = ll.Country('Nigeria')
    res['cluster_features'] = summ('cluster_features', c.cluster_features())
    res['grain_reports_cluster_features'] = [
        {k: r.get(k) for k in ('wave', 'site', 'levels', 'rows', 'dropped', 'destroyed',
                               'conflicting_groups', 'nan_key_rows')}
        for r in cmod.grain_reports(country='Nigeria', table='cluster_features')]
    s = c.sample()
    res['sample'] = summ('sample', s)
    for col in ('Region', 'District'):
        ml = c._market_lookup(col)
        ml = ml.set_index([x for x in ('t', 'v') if x in ml.columns]) if 'v' in ml.columns else ml
        res[f'market_lookup_{col}'] = summ(f'market_lookup_{col}', ml)
    res['sample_market_Region'] = summ('sample_market_Region', c._add_market_index(s.copy(), 'Region'))
    caught = w
res['warnings'] = dict(collections.Counter(
    f'{x.category.__name__}: {str(x.message)[:90]}' for x in caught))
with open(out, 'w') as f:
    json.dump(res, f, indent=1, sort_keys=True)
print(json.dumps({k: v for k, v in res.items() if k != 'warnings'}, indent=1)[:4000])
