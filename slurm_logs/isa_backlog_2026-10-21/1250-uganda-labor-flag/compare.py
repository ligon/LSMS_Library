"""Before/after aggregates for GH #1250 from the two cold-built API frames.
Aggregates only are written."""
import json, os, sys
import numpy as np, pandas as pd
S = sys.argv[1]
import lsms_library
top = os.environ['CHECKOUT']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library import transformations as tr
KEY = ['t', 'i', 'plot_id', 'source', 'season']
fr = {}
for v in ('base', 'branch'):
    d = pd.read_parquet(f'{S}/{v}_plot_labor.parquet')
    fr[v] = d
b = fr['base'].reset_index(); n = fr['branch'].reset_index()
for d in (b, n):
    d['PersonDays'] = d['PersonDays'].astype('float64'); d['Wage'] = d['Wage'].astype('float64')
    d['pid_nan'] = d['plot_id'].isna()
# v must be identical where both serve a key
m = b.merge(n, on=KEY, how='outer', suffixes=('_b', '_n'), indicator=True)
assert len(m) == len(b.merge(n, on=KEY, how='outer')), 'merge sanity'
m['pid_nan'] = m['plot_id'].isna()
res = {'rows': {'base': len(b), 'branch': len(n)},
       'totals': {'base': float(b['PersonDays'].sum()), 'branch': float(n['PersonDays'].sum())},
       'only_in_branch': int((m['_merge'] == 'right_only').sum())}
both = m[m['_merge'] == 'both']
res['v_differs_on_common_keys'] = int(((both['v_b'] != both['v_n']) & ~(both['v_b'].isna() & both['v_n'].isna())).sum())
removed = m[m['_merge'] == 'left_only']
res['removed'] = {'n': len(removed), 'all_zero_in_base': bool((removed['PersonDays_b'] == 0).all()),
                  'with_wage': int(removed['Wage_b'].notna().sum()),
                  'wage_values': {'n': int(removed['Wage_b'].notna().sum()),
                                  'min': float(removed['Wage_b'].min()) if removed['Wage_b'].notna().any() else None,
                                  'median': float(removed['Wage_b'].median()) if removed['Wage_b'].notna().any() else None,
                                  'max': float(removed['Wage_b'].max()) if removed['Wage_b'].notna().any() else None,
                                  'sum': float(removed['Wage_b'].sum())}}
pb, pn = both['PersonDays_b'], both['PersonDays_n']
res['common'] = {'n': len(both),
                 'positive_in_base_changed': int(((pb > 0) & (pn != pb)).sum()),
                 'zero_to_positive': int(((pb == 0) & (pn > 0)).sum()),
                 'zero_to_positive_days': float((pn - pb)[(pb == 0) & (pn > 0)].sum()),
                 'other_change': int(((pn != pb) & ~((pb == 0) & (pn > 0))).sum()),
                 'wage_changed': int(((both['Wage_b'] != both['Wage_n']) & ~(both['Wage_b'].isna() & both['Wage_n'].isna())).sum()),
                 'zero_to_positive_nan_plot': int(((pb == 0) & (pn > 0) & both['pid_nan']).sum())}
rows = []
for (t, s), g in m.groupby(['t', 'season']):
    gb = g[g['_merge'] != 'right_only']; gn = g[g['_merge'] != 'left_only']
    rem = g[g['_merge'] == 'left_only']; c = g[g['_merge'] == 'both']
    z2p = c[(c['PersonDays_b'] == 0) & (c['PersonDays_n'] > 0)]
    rows.append({'t': t, 'season': s,
                 'rows_base': len(gb), 'rows_branch': len(gn),
                 'removed_nan_plot': int(rem['pid_nan'].sum()), 'removed_valid_plot': int((~rem['pid_nan']).sum()),
                 'zero_to_positive': len(z2p), 'zero_to_positive_days': float((z2p['PersonDays_n'] - z2p['PersonDays_b']).sum()),
                 'zeros_base': int((gb['PersonDays_b'] == 0).sum()), 'zeros_branch': int((gn['PersonDays_n'] == 0).sum()),
                 'positive_base': int((gb['PersonDays_b'] > 0).sum()), 'positive_branch': int((gn['PersonDays_n'] > 0).sum()),
                 'total_base': float(gb['PersonDays_b'].sum()), 'total_branch': float(gn['PersonDays_n'].sum()),
                 'nan_plot_base': int(gb['pid_nan'].sum()), 'nan_plot_branch': int(gn['pid_nan'].sum()),
                 'nan_plot_positive_branch': int((gn['pid_nan'] & (gn['PersonDays_n'] > 0)).sum()),
                 'nan_v_base': int(gb['v_b'].isna().sum()), 'nan_v_branch': int(gn['v_n'].isna().sum())})
res['by_wave_season'] = rows
by_src = []
for (t, s, src), g in m.groupby(['t', 'season', 'source']):
    rem = g[g['_merge'] == 'left_only']
    by_src.append({'t': t, 'season': s, 'source': src, 'removed_nan_plot': int(rem['pid_nan'].sum()),
                   'removed_valid_plot': int((~rem['pid_nan']).sum())})
res['removed_by_source'] = by_src
# downstream transforms
ds = {}
for name, fn in (('total_labor_days', tr.total_labor_days), ('total_family_labor_days', tr.total_family_labor_days),
                 ('total_hired_labor_days', tr.total_hired_labor_days)):
    out = {}
    for v in ('base', 'branch'):
        r = fn(fr[v])
        col = r.columns[0]
        per = r.groupby(level='t')[col].agg(['size', 'sum', lambda s: int((s == 0).sum())])
        per.columns = ['hh_rows', 'days', 'hh_with_zero_days']
        out[v] = {'rows': int(len(r)), 'days': float(r[col].sum()),
                  'per_wave': {t: {k: float(x) for k, x in row.items()} for t, row in per.iterrows()}}
    ds[name] = out
res['downstream'] = ds
rep = {}
for v in ('base', 'branch'):
    j = json.load(open(f'{S}/{v}_plot_labor.json'))
    rep[v] = j['null_index_reports']
res['site_I'] = rep
json.dump(res, open(f'{S}/compare.json', 'w'), indent=1, default=str)
print(json.dumps({k: res[k] for k in ('rows', 'totals', 'only_in_branch', 'v_differs_on_common_keys', 'removed', 'common')}, indent=1))
