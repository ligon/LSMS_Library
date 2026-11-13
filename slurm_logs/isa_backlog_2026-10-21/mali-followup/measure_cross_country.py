"""nitrogen_kg / seed_kg over Feature('plot_inputs')() for every country.

Usage: python measure_cross_country.py <checkout> <outdir>

Writes <outdir>/feature_all.pkl and <outdir>/nitrogen_all.pkl (row-level;
keep them OUT of the repo) and <outdir>/summary.json (aggregates only):
per-country nitrogen_kg / seed_kg totals and the nitrogen label tally, the
pooled call's tally, and -- for the feature.py promotion docstring -- each
country's `u` column census (null count, literal 'Unknown' count) on
Country(c).plot_inputs() and, for Mali / Nigeria / Tanzania,
Country(c).crop_production().
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

top = subprocess.check_output(['git', '-C', checkout, 'rev-parse', '--show-toplevel'],
                              text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
warnings.simplefilter('ignore')

S = {'head': subprocess.check_output(['git', '-C', checkout, 'rev-parse', 'HEAD'],
                                     text=True).strip()}
f = ll.Feature('plot_inputs')()
f.to_pickle(os.path.join(outdir, 'feature_all.pkl'))
countries = sorted(pd.unique(f.index.get_level_values('country')))
S['countries'] = countries
S['rows'] = {c: int(n) for c, n in f.groupby(level='country').size().items()}
fl = list(f.index.names)
S['groupby_size_sum'] = {c: int(n) for c, n in f.groupby(fl).size().groupby(level='country').sum().items()}
S['j_nan'] = {c: int(pd.isna(f.xs(c, level='country', drop_level=False)
                             .index.get_level_values('j')).sum()) for c in countries}

pooled = tr.nitrogen_kg(f)
pooled.to_pickle(os.path.join(outdir, 'nitrogen_all.pkl'))
S['pooled_nitrogen'] = {'rows': int(len(pooled)), 'total': float(pooled['Nitrogen_kg'].sum()),
                        'tally': pooled.attrs.get('nitrogen_input_match')}
per = {}
for c in countries:
    sub = f.xs(c, level='country', drop_level=False)
    n = tr.nitrogen_kg(sub)
    s = tr.seed_kg(sub)
    per[c] = {'nitrogen_rows': int(len(n)), 'nitrogen_total': float(n['Nitrogen_kg'].sum()),
              'nitrogen_tally': n.attrs.get('nitrogen_input_match'),
              'seed_rows': int(len(s)), 'seed_total': float(s['Seed_kg'].sum())}
S['per_country'] = per


def _u_census(df):
    if 'u' in df.index.names:
        u, where = pd.Series(df.index.get_level_values('u')), 'index'
    elif 'u' in df.columns:
        u, where = df['u'], 'column'
    else:
        return {'where': 'absent'}
    return {'where': where, 'rows': int(len(df)), 'null': int(u.isna().sum()),
            'literal_Unknown': int((u.astype('string') == 'Unknown').sum())}


ucen = {}
for c in countries:
    ucen[f'{c}/plot_inputs'] = _u_census(ll.Country(c).plot_inputs())
for c in ('Mali', 'Nigeria', 'Tanzania'):
    ucen[f'{c}/crop_production'] = _u_census(ll.Country(c).crop_production())
S['u_census'] = ucen

with open(os.path.join(outdir, 'summary.json'), 'w') as fh:
    json.dump(S, fh, indent=1, sort_keys=True, default=str)
print(json.dumps({k: S[k] for k in ('rows', 'groupby_size_sum')}, indent=1))
print(json.dumps(ucen, indent=1))
