"""Measure Mali plot_inputs on one checkout (cold, in a fresh LSMS_DATA_DIR).

Usage: python measure_mali.py <checkout> <outdir>

Writes <outdir>/mali_country.pkl, <outdir>/mali_feature.pkl (row-level; keep
them OUT of the repo) and <outdir>/summary.json (aggregate counts only).
"""
import json
import os
import subprocess
import sys
import warnings

checkout, outdir = sys.argv[1], sys.argv[2]
os.makedirs(outdir, exist_ok=True)

import pandas as pd  # noqa: E402

import lsms_library as ll  # noqa: E402
from lsms_library import transformations as tr  # noqa: E402
from lsms_library.null_index_audit import null_index_reports  # noqa: E402

top = subprocess.check_output(['git', '-C', checkout, 'rev-parse', '--show-toplevel'],
                              text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__

MARKER = '(not crop-specific)'
S = {'checkout': checkout,
     'head': subprocess.check_output(['git', '-C', checkout, 'rev-parse', 'HEAD'],
                                     text=True).strip()}


def _vc(s):
    return {str(k): int(v) for k, v in s.value_counts(dropna=False).sort_index().items()}


with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    df = ll.Country('Mali').plot_inputs()
S['country_warnings'] = sorted({f'{x.category.__name__}: {str(x.message)[:160]}' for x in w})
df.to_pickle(os.path.join(outdir, 'mali_country.pkl'))

crop = pd.Series(df.index.get_level_values('crop'), index=df.index)
inp = pd.Series(df.index.get_level_values('input'), index=df.index)
S['rows'] = int(len(df))
S['index_names'] = list(df.index.names)
S['columns'] = list(df.columns)
S['rows_by_input'] = _vc(inp)
S['crop_nan'] = int(crop.isna().sum())
S['crop_marker'] = int((crop == MARKER).sum())
S['crop_nan_by_input'] = _vc(inp[crop.isna()])
S['crop_marker_by_input'] = _vc(inp[crop == MARKER])
S['seed_rows'] = int((inp == 'Seed').sum())
S['seed_crop_nan'] = int(((inp == 'Seed') & crop.isna()).sum())
S['seed_crop_marker'] = int(((inp == 'Seed') & (crop == MARKER)).sum())
S['use_answer_rows'] = int(inp.map(tr.is_use_answer_label).sum())
S['applied'] = _vc(df['Applied'].astype('string').fillna('<NA>'))
S['u_unknown'] = int((df['u'] == 'Unknown').sum())
S['u_null'] = int(df['u'].isna().sum())
S['null_index_reports'] = null_index_reports('Mali', 'plot_inputs')
full = list(df.index.names)
S['country_groupby_size_sum'] = int(df.groupby(full).size().sum())

with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    n = tr.nitrogen_kg(df)
S['nitrogen_rows'] = int(len(n))
S['nitrogen_total'] = float(n['Nitrogen_kg'].sum())
S['nitrogen_tally'] = n.attrs.get('nitrogen_input_match')
S['nitrogen_warnings'] = sorted({f'{x.category.__name__}: {str(x.message)[:200]}' for x in w})
sk = tr.seed_kg(df)
S['seed_kg_rows'] = int(len(sk))
S['seed_kg_total'] = float(sk.select_dtypes('number').sum().sum())

with warnings.catch_warnings(record=True) as w:
    warnings.simplefilter('always')
    f = ll.Feature('plot_inputs')(['Mali'])
S['feature_warnings'] = sorted({f'{x.category.__name__}: {str(x.message)[:200]}' for x in w})
f.to_pickle(os.path.join(outdir, 'mali_feature.pkl'))
S['feature_rows'] = int(len(f))
S['feature_index_names'] = list(f.index.names)
fl = list(f.index.names)
S['feature_groupby_size_sum'] = int(f.groupby(fl).size().sum())
S['feature_nan_by_level'] = {lvl: int(pd.isna(f.index.get_level_values(lvl)).sum())
                             for lvl in fl}
fn = tr.nitrogen_kg(f)
S['feature_nitrogen_rows'] = int(len(fn))
S['feature_nitrogen_total'] = float(fn['Nitrogen_kg'].sum())
S['feature_nitrogen_tally'] = fn.attrs.get('nitrogen_input_match')
fs = tr.seed_kg(f)
S['feature_seed_kg_rows'] = int(len(fs))
S['feature_seed_kg_total'] = float(fs.select_dtypes('number').sum().sum())

with open(os.path.join(outdir, 'summary.json'), 'w') as fh:
    json.dump(S, fh, indent=1, sort_keys=True, default=str)
print(json.dumps({k: S[k] for k in ('rows', 'crop_nan', 'crop_marker', 'feature_rows',
                                    'feature_groupby_size_sum', 'nitrogen_total',
                                    'seed_kg_total')}, indent=1))
