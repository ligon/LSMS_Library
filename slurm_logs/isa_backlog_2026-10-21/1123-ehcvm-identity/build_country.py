"""GH #1123: build Country(C).plot_inputs() in a private data root and record
aggregate facts (rows per wave, grain-collapse warnings/reports, transform
totals).  Row-level frames go to <private_dir> only.

Usage: python build_country.py <checkout> <country> <aggregate.json> <private_dir> <label>
Run once in an EMPTY country L2 tree for a cold build; run again for warm.
"""
import json
import os
import sys
import time
import warnings
from pathlib import Path

checkout, country, out_json, private, label = sys.argv[1:6]
private = Path(private)
private.mkdir(parents=True, exist_ok=True)
import lsms_library as ll  # noqa: E402
from lsms_library import country as C  # noqa: E402
from lsms_library import transformations as T  # noqa: E402
from lsms_library.local_tools import data_root  # noqa: E402

assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
var = Path(data_root()) / country / 'var' / 'plot_inputs.parquet'
pre_exists = var.exists()
t0 = time.time()
with warnings.catch_warnings(record=True) as caught:
    warnings.simplefilter('always')
    df = ll.Country(country).plot_inputs()
elapsed = time.time() - t0
gw = [w for w in caught if issubclass(w.category, C.GrainCollapseWarning)
      and 'plot_inputs' in str(w.message)]
reports = C.grain_reports(country, 'plot_inputs')
rep_agg = []
for r in reports:
    rep_agg.append({k: r.get(k) for k in ('wave', 'rows', 'dropped', 'destroyed',
                                          'conflicting_groups', 'nan_key_rows', 'kind')
                    if k in r})
flat = df.reset_index()
t = flat['t']
res = {
    'label': label, 'country': country, 'cold': not pre_exists, 'seconds': round(elapsed, 1),
    'rows': int(len(df)), 'rows_by_wave': {str(k): int(v) for k, v in t.value_counts().sort_index().items()},
    'index_names': list(df.index.names), 'columns': list(df.columns),
    'index_unique': bool(df.index.is_unique),
    'grain_collapse_warnings': len(gw),
    'grain_reports': rep_agg,
    'report_keys': sorted({k for r in reports for k in r}),
}
for fn in ('nitrogen_kg', 'seed_kg'):
    r = getattr(T, fn)(df)
    col = r.columns[0]
    res[fn] = {'rows': int(len(r)), 'total': float(r[col].sum()),
               'by_wave': {str(k): float(v) for k, v in r[col].groupby(level='t').sum().items()}}
    r.to_parquet(private / f'{label}_{country}_{fn}.parquet')
nm = T.nitrogen_kg(df).attrs.get('nitrogen_input_match', {})
res['nitrogen_input_match'] = {k: nm.get(k) for k in ('matched_rows', 'unmatched_rows')}
df.to_parquet(private / f'{label}_{country}_plot_inputs.parquet')
Path(out_json).write_text(json.dumps(res, indent=1, sort_keys=True, default=str))
print(json.dumps({k: res[k] for k in ('label', 'country', 'cold', 'rows', 'rows_by_wave',
                                      'grain_collapse_warnings', 'nitrogen_kg', 'seed_kg')}, default=str))
print('reports', json.dumps(rep_agg, default=str))
