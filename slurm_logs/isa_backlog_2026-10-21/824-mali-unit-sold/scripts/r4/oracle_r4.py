"""Round 4 (revised ruling, 2026-10-23): raw oracle for the tree rows.

Written from the raw rosters, independently of mali.py (only mali.i and
mali._crop_labels build the join keys):
  Quantity      = per-month harvest x months under the harvest edge rule
                  (codes 99 / 9999 / 9999.9 masked first; 0 -> 0; months NA,
                  or months 0 beside a positive answer -> NA);
  Quantity_sold = s3bq13a / s11fq14a AS RECORDED, codes 99 / 9999 / 9999.9 masked;
  Value_sold    = s3bq14 / s11fq15 AS RECORDED, 999999 masked;
  Derivation    = tree-twelve-month-total on every tree row, joined (sorted,
                  '+') with tree-sale-window where the served sale is positive.
Also: the edge rows (positive sale beside 0 / missing months) and what they serve.
usage: oracle_r4.py <label> <checkout> <served-label> <out dir>.  Aggregates only."""
import os, re, sys, warnings; warnings.simplefilter('ignore')
label, checkout, served_label, OUT = sys.argv[1:5]
import lsms_library as ll
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__
import numpy as np, pandas as pd
from lsms_library.local_tools import get_dataframe, id_walk
C = f'{checkout}/lsms_library/countries/Mali'
sys.path.insert(0, f'{C}/_')
from mali import i as mali_i, _crop_labels
TREES = 'Mali::crop_production::tree-twelve-month-total'
SALES = 'Mali::crop_production::tree-sale-window'
BOTH = '+'.join(sorted((TREES, SALES)))
upd = ll.Country('Mali').updated_ids
sv = pd.read_parquet(f'{OUT}/{served_label}_served.parquet').reset_index()
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
def qmask(x):
    x = x.copy(); x[x.isin([99, 9999]) | np.isclose(x, 9999.9)] = np.nan; return x
def total(per_month, months):
    x = qmask(per_month)
    out = np.where(x == 0, 0.0, np.where((months == 0) & (x > 0), np.nan, x * months))
    return pd.Series(out, index=per_month.index)
def eq(a, b):
    if a.dtype == object or str(a.dtype) == 'string':
        return ((a.astype('string') == b.astype('string')) | (a.isna() & b.isna())).fillna(False)
    a = a.astype('float64'); b = b.astype('float64')
    return ((a == b) | (a.isna() & b.isna())).fillna(False)
for wave, fn, crop, held, hk, mv, qv, sq, vv in [
        ('2014-15', 'EACIS3B_p2.dta', 's3bq01', 's3bq03', 'menage', 's3bq09', 's3bq10a', 's3bq13a', 's3bq14'),
        ('2017-18', 'eaci17_s11fp1.dta', 's11fq01', 's11fq03', 'ménage', 's11fq10', 's11fq11a', 's11fq14a', 's11fq15')]:
    os.chdir(f'{C}/{wave}/_')
    b = get_dataframe(f'../Data/{fn}')
    b = b[b[crop].notna() & b[held].astype('string').str.strip().eq('Oui')]
    mo = months_raw(b[mv])
    e = pd.DataFrame({'t': wave, 'i': hh(b, hk), 'crop': _crop_labels(b[crop]).values,
                      'exp_q': total(num(b[qv]), mo).values,
                      'exp_qs': qmask(num(b[sq])).values,
                      'exp_v': num(b[vv]).mask(num(b[vv]) == 999999).values,
                      'exp_m': mo.values,
                      'qs_x_m': (qmask(num(b[sq])) * mo).values})
    e = walk(e.dropna(subset=['crop']))
    # a served key may join >1 raw row (two 2014-15 rows, both all-zero): sum like the build
    g = e.groupby(['t', 'i', 'crop'])
    e = pd.DataFrame({c: g[c].sum(min_count=1) for c in ['exp_q', 'exp_qs', 'exp_v', 'qs_x_m']}
                     | {'exp_m': g['exp_m'].first(), 'n_raw': g.size()}).reset_index()
    e['sold'] = (e.exp_qs > 0) | (e.exp_v > 0)
    e['exp_key'] = np.where(e.sold, BOTH, TREES)
    tv = sv[(sv.t == wave) & sv.perennial.fillna(False)]
    m = tv.merge(e, on=['t', 'i', 'crop'], how='inner')
    print(f'{wave} trees: served {len(tv)}, joined {len(m)} (served keys with 2 raw rows: {int((m.n_raw > 1).sum())})')
    for a, x in [('Quantity', 'exp_q'), ('Quantity_sold', 'exp_qs'), ('Value_sold', 'exp_v'),
                 ('months_harvested', 'exp_m'), ('Derivation', 'exp_key')]:
        print(f'  {wave} tree {a} == oracle: {int(eq(m[a], m[x]).sum())} of {len(m)}')
    qpos = (m.Quantity_sold > 0).fillna(False).astype(bool); vpos = (m.Value_sold > 0).fillna(False).astype(bool)
    print(f'  {wave} sale key on {int((m.Derivation == BOTH).sum())} rows; positive-sale rows {int(m.sold.sum())}; '
          f'of which priced (Qs>0 & V>0) {int((qpos & vpos).sum())}, Qs>0 & V==0 {int((qpos & (m.Value_sold == 0).fillna(False).astype(bool)).sum())}, '
          f'Qs>0 & V NA {int((qpos & m.Value_sold.isna()).sum())}, V>0 & Qs not >0 {int((vpos & ~qpos).sum())}')
    # the edge rows: a positive sale beside 0 or missing months
    edge0 = m.sold & (m.exp_m == 0); edgena = m.sold & m.exp_m.isna()
    for name, msk in [('months 0', edge0), ('months missing', edgena)]:
        print(f'  {wave} positive sale beside {name}: {int(msk.sum())} rows; served Quantity_sold non-null {int(m.Quantity_sold[msk].notna().sum())}, '
              f'Value_sold non-null {int(m.Value_sold[msk].notna().sum())}; Quantity (harvest) NA {int(m.Quantity[msk].isna().sum())}')
    print(f'  {wave} rescaling check: served Quantity_sold x months_harvested == raw per-month x months on '
          f'{int(eq(m.Quantity_sold * m.months_harvested.astype("float64"), m.qs_x_m).sum())} of {len(m)}')
