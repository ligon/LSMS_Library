"""Base (90fc1e18d) vs branch (wage-only hired rows served) on the two cold-built
API frames, joined to the raw oracle's wage-only rows.  Aggregates only.

Usage: CHECKOUT=<branch checkout> python compare_r2.py <scratch dir> <out.json>
Reads <scratch>/{base,branch}_plot_labor.parquet, <scratch>/oracle_wage_only.parquet
and <scratch>/sites_{base,branch}.json.
"""
import json, os, sys
import numpy as np, pandas as pd
import lsms_library
top = os.environ['CHECKOUT']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library import transformations as tr
S, OUT = sys.argv[1], sys.argv[2]
KEY = ['t', 'i', 'plot_id', 'source', 'season']
fr = {v: pd.read_parquet(f'{S}/{v}_plot_labor.parquet') for v in ('base', 'branch')}
b, n = (fr[v].reset_index() for v in ('base', 'branch'))
for d in (b, n):
    d['PersonDays'] = d['PersonDays'].astype('float64')
    d['Wage'] = d['Wage'].astype('float64')
m = b.merge(n, on=KEY, how='outer', suffixes=('_b', '_n'), indicator=True)


def neq(x, y):
    return (x != y) & ~(x.isna() & y.isna())


both = m[m['_merge'] == 'both']
added = m[m['_merge'] == 'right_only'].copy()
res = {'rows': {'base': len(b), 'branch': len(n)},
       'removed': int((m['_merge'] == 'left_only').sum()),
       'added': len(added),
       'common': len(both),
       'common_changed': {c: int(neq(both[f'{c}_b'], both[f'{c}_n']).sum())
                          for c in ('PersonDays', 'Wage', 'v')}}
# the added keys must be EXACTLY the oracle's wage-only rows
o = pd.read_parquet(f'{S}/oracle_wage_only.parquet')
o['Wage'] = o['Wage'].astype('float64')
j = added.merge(o, on=KEY, how='outer', indicator='_o')
res['added_vs_oracle'] = {k: int(v) for k, v in j['_o'].value_counts().items()}
jj = j[j['_o'] == 'both']
res['added_checks'] = {
    'personDays_nan': int(jj['PersonDays_n'].isna().sum()),
    'wage_equals_oracle': int((jj['Wage_n'] == jj['Wage']).sum()),
    'source_hired': int((jj['source'] == 'hired').sum()),
    'plot_id_nan': int(jj['plot_id'].isna().sum()),
    'v_nan': int(jj['v_n'].isna().sum()),
    'wage_sum': float(jj['Wage_n'].sum()),
}
res['added_by_wave_season_flag_wage'] = [
    {'t': t, 'season': s, 'flag': f, 'wage': w, 'rows': int(len(g)), 'wage_sum': float(g['Wage_n'].sum())}
    for (t, s, f, w), g in jj.groupby(['t', 'season', 'flag', 'wage'])]
res['added_by_wave_season'] = {f'{t}/{s}': int(len(g)) for (t, s), g in jj.groupby(['t', 'season'])}
res['added_by_flag'] = {k: int(v) for k, v in jj['flag'].value_counts().items()}
res['added_by_wage'] = {k: int(v) for k, v in jj['wage'].value_counts().items()}
# "No" + wage rows in the served frames: PersonDays 0 with a Wage
for v, d in (('base', b), ('branch', n)):
    h = d[d['source'] == 'hired']
    res.setdefault('hired_zero_days_with_wage', {})[v] = {
        'rows': int(((h['PersonDays'] == 0) & h['Wage'].notna()).sum()),
        'wage_sum': float(h.loc[(h['PersonDays'] == 0) & h['Wage'].notna(), 'Wage'].sum())}
# downstream: the three day totals must be byte-identical (NaN days add nothing)
ds = {}
for name, fn in (('total_labor_days', tr.total_labor_days),
                 ('total_family_labor_days', tr.total_family_labor_days),
                 ('total_hired_labor_days', tr.total_hired_labor_days)):
    rb, rn = fn(fr['base']), fn(fr['branch'])
    col = rb.columns[0]
    try:
        pd.testing.assert_frame_equal(rb, rn)
        same = True
    except AssertionError:
        same = False
    ds[name] = {'identical': same, 'rows': {'base': len(rb), 'branch': len(rn)},
                'days': {'base': float(rb[col].sum()), 'branch': float(rn[col].sum())}}
res['downstream_days'] = ds
# downstream: median-wage valuation of hired days (the WB hired_labor_value
# construct; no library wrapper exists, so median_price_valuation is called
# directly: price = Wage / PersonDays, quantity = PersonDays, ladder v -> national,
# stratified by wave t)
mpv = {}
vals = {}
for v in ('base', 'branch'):
    h = fr[v].xs('hired', level='source', drop_level=False).copy()
    h['PersonDays'] = h['PersonDays'].astype('float64')
    h['Wage'] = h['Wage'].astype('float64')
    with np.errstate(divide='ignore', invalid='ignore'):
        price = h['Wage'] / h['PersonDays']
    price = price.replace([np.inf, -np.inf], np.nan)
    r = tr.median_price_valuation(h, ['v'], value_col='Wage', kg_qty=h['PersonDays'],
                                  quantity_col='PersonDays', item_keys=('t',))
    vals[v] = r
    mpv[v] = {'hired_rows': len(h), 'priced_rows_in_pool': int((price > 0).sum()),
              'value_nonnull': int(r['Value'].notna().sum()), 'value_sum': float(r['Value'].sum()),
              'value_sum_by_wave': {t: float(x) for t, x in r['Value'].groupby(level='t').sum().items()}}
common_idx = vals['base'].index.intersection(vals['branch'].index)
new_idx = vals['branch'].index.difference(vals['base'].index)
mpv['unit_price_changed_on_common_rows'] = int(neq(vals['base'].loc[common_idx, '_unit_price'],
                                                  vals['branch'].loc[common_idx, '_unit_price']).sum())
mpv['value_changed_on_common_rows'] = int(neq(vals['base'].loc[common_idx, 'Value'],
                                             vals['branch'].loc[common_idx, 'Value']).sum())
mpv['new_rows'] = len(new_idx)
mpv['new_rows_value_nan'] = int(vals['branch'].loc[new_idx, 'Value'].isna().sum())
mpv['new_rows_unit_price_nonnull'] = int(vals['branch'].loc[new_idx, '_unit_price'].notna().sum())
res['downstream_median_wage_valuation'] = mpv
# Sites B / Q / I (warm read, fresh process, each side's own data dir)
res['sites'] = {}
for v in ('base', 'branch'):
    sj = json.load(open(f'{S}/sites_{v}.json'))
    res['sites'][v] = {'site_B_reports': len(sj['site_B_null_read']), 'site_Q_reports': len(sj['site_Q_quantity']),
                       'site_I': sj['site_I_null_index'], 'warning_classes': sj['warning_classes']}
json.dump(res, open(OUT, 'w'), indent=1, default=str)
print(json.dumps({k: res[k] for k in ('rows', 'removed', 'added', 'common_changed', 'added_vs_oracle',
                                      'added_checks', 'added_by_wave_season', 'added_by_flag', 'added_by_wage',
                                      'hired_zero_days_with_wage', 'downstream_days')}, indent=1, default=str))
print(json.dumps(mpv, indent=1, default=str))
