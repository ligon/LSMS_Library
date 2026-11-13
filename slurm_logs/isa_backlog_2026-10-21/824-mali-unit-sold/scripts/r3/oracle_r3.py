"""Raw oracle for the two PR #1272 rulings, against a served table.

Ruling 1 (+ round 3): every served tree row's Quantity / Quantity_sold / Value_sold equals the raw
per-month answer x the raw month count under the stated edge rule, written
here from scratch (not mali.derive_tree_twelve_month_total); the quantity missing codes are
masked, and 999999 on the value.
Ruling 2: every served 2017-18 field row's Quantity_sold / Value_sold equals
0 where s7gq20 == 'Non', else the raw s7gq21a / s7gq22 AS RECORDED (NA stays
NA), with the single-plot rule.  Keys use mali.i / mali._crop_labels.
usage: oracle_r2.py <label> <checkout> [served-label].  Aggregates only."""
import os, re, sys, warnings; warnings.simplefilter('ignore')
label, checkout = sys.argv[1], sys.argv[2]
served_label = sys.argv[3] if len(sys.argv) > 3 else label
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep)
import numpy as np, pandas as pd
from lsms_library.local_tools import get_dataframe, id_walk
C = f'{checkout}/lsms_library/countries/Mali'
sys.path.insert(0, f'{C}/_')
from mali import i as mali_i, _crop_labels
upd = ll.Country('Mali').updated_ids
sv = pd.read_parquet(f'/local/job40390255/data-private/824-ml-r2/out/{served_label}_served.parquet').reset_index()
walk = lambda df: id_walk(df.set_index(['t', 'i']), upd).reset_index()
hh = lambda df, key: df.apply(lambda r: mali_i(pd.Series([r['grappe'], r[key]])), axis=1)
num = lambda s: pd.to_numeric(s, errors='coerce').astype('float64')
def months_raw(s):
    def conv(v):
        if pd.isna(v): return np.nan
        t = str(v).strip()
        if t.lower().startswith('aucune'): return 0.0
        m = re.match(r'^(\d+)(?:\.0+)?(?:\s*mois)?$', t)
        n = int(m.group(1)) if m else -1
        return float(n) if 0 <= n <= 12 else np.nan
    return s.map(conv).astype('float64')
def total(per_month, months, mask=True):
    x = per_month.copy()
    if mask:
        x[x.isin([99, 9999]) | np.isclose(x, 9999.9)] = np.nan
    out = np.where(x == 0, 0.0, np.where((months == 0) & (x > 0), np.nan, x * months))
    return pd.Series(out, index=per_month.index)
def eq(a, b):
    a = a.astype('float64'); b = b.astype('float64')
    return ((a == b) | (a.isna() & b.isna())).fillna(False)
def report(name, m, pairs):
    for a, e in pairs:
        ok = eq(m[a], m[e])
        print(f'  {name} {a} == oracle: {int(ok.sum())} of {len(m)}')
# ---- ruling 1: trees
for wave, fn, crop, held, hk, mv, qv, sv_, vv in [
        ('2014-15', 'EACIS3B_p2.dta', 's3bq01', 's3bq03', 'menage', 's3bq09', 's3bq10a', 's3bq13a', 's3bq14'),
        ('2017-18', 'eaci17_s11fp1.dta', 's11fq01', 's11fq03', 'ménage', 's11fq10', 's11fq11a', 's11fq14a', 's11fq15')]:
    os.chdir(f'{C}/{wave}/_')
    b = get_dataframe(f'../Data/{fn}')
    b = b[b[crop].notna() & b[held].astype('string').str.strip().eq('Oui')]
    mo = months_raw(b[mv])
    e = pd.DataFrame({'t': wave, 'i': hh(b, hk), 'crop': _crop_labels(b[crop]).values,
                      'exp_q': total(num(b[qv]), mo).values, 'exp_qs': total(num(b[sv_]), mo).values,
                      'exp_v': total(num(b[vv]).mask(num(b[vv]) == 999999), mo, mask=False).values, 'exp_m': mo.values,
                      'per_month_q': num(b[qv]).values})
    e = walk(e.dropna(subset=['crop']))
    tv = sv[(sv.t == wave) & sv.perennial.fillna(False)]
    m = tv.merge(e, on=['t', 'i', 'crop'], how='inner')
    print(f'{wave} trees: served {len(tv)}, joined {len(m)} (a served key may join >1 raw row)')
    report(wave + ' tree', m, [('Quantity', 'exp_q'), ('Quantity_sold', 'exp_qs'), ('Value_sold', 'exp_v'), ('months_harvested', 'exp_m')])
    print(f'  {wave} tree Quantity == raw per-month answer (the e28 wiring), where months > 1: '
          f'{int((eq(m.Quantity, m.per_month_q) & (m.exp_m > 1) & (m.per_month_q > 0)).sum())}')
# ---- ruling 2: 2017-18 field sale
os.chdir(f'{C}/2017-18/_')
f = get_dataframe('../Data/eaci17_s7fp2.dta')
def plot(df, a, b):
    x = df[a].astype('Int64').astype('string'); y = df[b].astype('Int64').astype('string')
    return (x + '_' + y).where(x.notna() & y.notna(), pd.NA)
fh = pd.DataFrame({'i': hh(f, 'exploitation'), 'plot_id': plot(f, 's7fq01', 's7fq02'),
                   'rawcrop': f['s7fq03'].astype('string').values, 'crop': _crop_labels(f['s7fq03']).values})
npl = fh.dropna(subset=['plot_id']).groupby(['i', 'rawcrop'])['plot_id'].nunique().rename('np').reset_index()
g7 = get_dataframe('../Data/eaci17_s7gp2.dta')
flag = g7['s7gq20'].astype('string').str.strip()
sale = pd.DataFrame({'i': hh(g7, 'exploitation'), 'rawcrop': g7['s7gq01'].astype('string').values,
                     'exp_qs': np.where(flag.eq('Non').fillna(False), 0.0, num(g7['s7gq21a'])),
                     'exp_v': np.where(flag.eq('Non').fillna(False), 0.0, num(g7['s7gq22'])),
                     'flag': flag.fillna('<NA>').values})
assert not sale.duplicated(['i', 'rawcrop']).any()
x = fh.merge(sale.merge(npl, on=['i', 'rawcrop'], how='left'), on=['i', 'rawcrop'], how='left')
multi = x.np.fillna(0) > 1
x.loc[multi, ['exp_qs', 'exp_v']] = np.nan
x['t'] = '2017-18'
x = walk(x.dropna(subset=['crop'])[['t', 'i', 'plot_id', 'crop', 'exp_qs', 'exp_v', 'flag']])
fv = sv[(sv.t == '2017-18') & ~sv.perennial.fillna(False)]
m = fv.merge(x, on=['t', 'i', 'plot_id', 'crop'], how='inner')
print(f'2017-18 field: served {len(fv)}, joined {len(m)}')
report('2017-18 field', m, [('Quantity_sold', 'exp_qs'), ('Value_sold', 'exp_v')])
z = m[(m.Quantity_sold == 0)]
print(f'  served Quantity_sold == 0 on {len(z)} rows; their raw flag: {z.flag.value_counts(dropna=False).to_dict()}')
z = m[(m.Value_sold == 0)]
print(f'  served Value_sold == 0 on {len(z)} rows; their raw flag: {z.flag.value_counts(dropna=False).to_dict()}')
