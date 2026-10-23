"""Round 5 (red-team follow-up): measurements on two cold builds of Mali
crop_production (build_all.py layout: reset_index parquets).  Aggregates only.
usage: probe_r5.py <base dir> <branch dir> <checkout of the branch>"""
import os, sys, json, warnings
warnings.simplefilter('ignore')
import numpy as np, pandas as pd
base_dir, br_dir, checkout = sys.argv[1:4]
IDX = ['i', 't', 'v', 'plot_id', 'crop']
fr = {s: pd.read_parquet(f'{d}/crop_production.parquet') for s, d in (('base', base_dir), ('branch', br_dir))}
out = {}
def kind(f):
    return np.where(f.perennial.fillna(False).astype(bool), 'tree', 'field')
for s, f in fr.items():
    f = f.copy(); f['kind'] = kind(f)
    o = {}
    pos = lambda c: (f[c] > 0).fillna(False).astype(bool)
    priced = pos('Quantity_sold') & pos('Value_sold')
    for (w, k), g in f[priced].groupby(['t', 'kind']):
        knows = g.Unit_sold.notna() & g.u.notna()
        o[f'PRICED {w} {k}'] = [len(g), int(g.Unit_sold.notna().sum()), int(knows.sum()),
                               int((knows & (g.Unit_sold != g.u)).fillna(False).sum()),
                               int((g.Unit_sold == 'Kg').fillna(False).sum())]
    o['Value_sold == 999999 by t/kind'] = f[(f.Value_sold == 999999).fillna(False)].groupby(['t', 'kind']).size().astype(int).to_dict()
    o['Value_sold == 999999 by t/kind'] = {f'{a}/{b}': v for (a, b), v in o['Value_sold == 999999 by t/kind'].items()}
    tr = f[f.kind == 'tree']
    o['tree NA by wave'] = {w: {c: int(g[c].isna().sum()) for c in ['Quantity', 'Quantity_sold', 'Value_sold']} for w, g in tr.groupby('t')}
    sold = (pos('Quantity_sold') | pos('Value_sold'))[f.kind == 'tree']
    o['tree positive-sale rows by wave'] = tr[sold].groupby('t').size().astype(int).to_dict()
    o['Derivation'] = {str(k): int(v) for k, v in f['Derivation'].astype('string').value_counts(dropna=False).items()}
    z = tr[(tr.Quantity == 0).fillna(False) & sold]
    o['zero harvest beside positive sale by wave'] = z.groupby('t').size().astype(int).to_dict()
    o['  ... of which months_harvested (value counts)'] = {str(k): int(v) for k, v in z.months_harvested.astype('string').value_counts(dropna=False).items()}
    # window mismatch, same-unit tree rows with a positive harvest and a recorded sale
    same = tr[(tr.Unit_sold == tr.u).fillna(False) & (tr.Quantity > 0).fillna(False) & tr.Quantity_sold.notna()]
    m = same.months_harvested.astype('float64')
    ratio = same.Quantity_sold / same.Quantity
    share = same.Quantity_sold * m / same.Quantity
    o['WINDOW_MISMATCH (rows, median ratio, median rescaled share, sold>harvest, rescaled sold>harvest)'] = [
        len(same), round(float(ratio.median()), 3), round(float(share.median()), 3),
        int((same.Quantity_sold > same.Quantity).sum()), int((same.Quantity_sold * m > same.Quantity).sum())]
    o['  ... by wave'] = {w: [len(g), round(float((g.Quantity_sold / g.Quantity).median()), 3)] for w, g in same.groupby('t')}
    # price table: tree rows sold in Kg, priced
    kg = tr[(tr.Unit_sold == 'Kg').fillna(False) & pos('Quantity_sold')[f.kind == 'tree'] & pos('Value_sold')[f.kind == 'tree']]
    p_same = kg.Value_sold / kg.Quantity_sold
    p_tot = kg.Value_sold / (kg.Quantity_sold * kg.months_harvested.astype('float64'))
    tbl = pd.DataFrame({'t': kg.t, 'crop': kg.crop, 'same': p_same, 'tot': p_tot}).groupby(['t', 'crop']).agg(
        n=('same', 'size'), same_footing=('same', 'median'), total_reading=('tot', 'median'))
    o['price per kg (tree, sold in Kg)'] = {f'{a} {b}': [int(r.n), round(float(r.same_footing), 1), round(float(r.total_reading), 1)]
                                           for (a, b), r in tbl.iterrows()}
    out[s] = o
# changed cells base -> branch
a = fr['base'].set_index(IDX).sort_index(); b = fr['branch'].set_index(IDX).sort_index()
out['index equal'] = bool(a.index.equals(b.index))
k = pd.Series(kind(a.reset_index()), index=a.index)
diffs = {}
for c in a.columns:
    if c not in b.columns: continue
    x, y = a[c], b[c]
    try:
        xf, yf = pd.to_numeric(x, errors='raise').astype('float64'), pd.to_numeric(y, errors='raise').astype('float64')
        eq = (xf == yf) | (xf.isna() & yf.isna())
    except Exception:
        xs, ys = x.astype('string'), y.astype('string'); eq = (xs == ys).fillna(False) | (xs.isna() & ys.isna())
    eq = eq.astype(bool)
    if (~eq).any():
        lab = a.index.get_level_values('t')[~eq.to_numpy()].astype(str) + '/' + k[~eq].to_numpy()
        diffs[c] = pd.Series(lab).value_counts().to_dict()
out['changed rows by column, wave/kind'] = diffs
# downstream: gross_crop_revenue, harvest_kg, median_price_valuation (default and Kg-only, per wave)
from lsms_library import transformations as T
for s, f in fr.items():
    cp = f.set_index(IDX)
    d = {}
    g = T.gross_crop_revenue(cp)
    g = g.iloc[:, 0] if isinstance(g, pd.DataFrame) else g
    d['gross_crop_revenue sum by t'] = {k2: float(v) for k2, v in g.groupby(level='t').sum().items()}
    d['gross_crop_revenue rows'] = int(len(g))
    for name in ('default', 'kgonly'):
        kw = {} if name == 'default' else dict(kg_qty=cp['Quantity_sold'].where(cp['Unit_sold'].astype('string') == 'Kg'))
        r = T.median_price_valuation(cp, ['v'], item_keys=('t', 'crop'), quantity_col='Quantity', **kw)
        d[f'mpv {name}'] = r
    out[f'down {s}'] = d
res = {}
for name in ('default', 'kgonly'):
    pa, pb = out['down base'][f'mpv {name}'], out['down branch'][f'mpv {name}']
    ua, ub = pa['_unit_price'], pb['_unit_price']
    ch = ~((ua == ub) | (ua.isna() & ub.isna()))
    r = {'rows': len(pa), 'price changed rows': int(ch.sum()),
         'changed by crop': pd.Series(ch[ch].index.get_level_values('crop')).value_counts().to_dict(),
         'changed by t': pd.Series(ch[ch].index.get_level_values('t')).value_counts().to_dict(),
         'Value sum by t base': {k2: float(v) for k2, v in pa.groupby(level='t')['Value'].sum().items()},
         'Value sum by t branch': {k2: float(v) for k2, v in pb.groupby(level='t')['Value'].sum().items()}}
    if ch.any():
        r['price ratio branch/base'] = {k2: float(v) for k2, v in (ub[ch] / ua[ch]).describe().items()}
    res[name] = r
for s in ('base', 'branch'):
    out[f'down {s}'] = {k2: v for k2, v in out[f'down {s}'].items() if not k2.startswith('mpv')}
out['median_price_valuation base -> branch'] = res
# raw: served 2014-15 field rows whose raw s3aq24 is the 999999 code
sys.path.insert(0, f'{checkout}/lsms_library/countries/Mali/_')
os.chdir(f'{checkout}/lsms_library/countries/Mali/_')
import lsms_library as ll
from lsms_library.local_tools import get_dataframe, id_walk
import mali
s3 = get_dataframe(f'{checkout}/lsms_library/countries/Mali/2014-15/Data/EACIS3A_p2.dta')
v = pd.to_numeric(s3['s3aq24'], errors='coerce')
non = s3['s3aq22'].astype('string').str.strip().eq('Non').fillna(False)
out['raw s3aq24'] = {'rows': int(len(s3)), '== 999999': int((v == 999999).sum()), '== 999999 and not Non': int(((v == 999999) & ~non).sum()),
                     '== 999999 beside s3aq23a == 0': int(((v == 999999) & (pd.to_numeric(s3['s3aq23a'], errors='coerce') == 0)).sum()),
                     '>= 999998': int((v >= 999998).sum()), 'max below 999999': float(v[v < 999999].max()),
                     'values > 900000 (count)': int(((v > 900000) & (v < 999999)).sum())}
s3b = get_dataframe(f'{checkout}/lsms_library/countries/Mali/2014-15/Data/EACIS3B_p2.dta')
vb = pd.to_numeric(s3b['s3bq14'], errors='coerce')
out['raw s3bq14'] = {'== 999999': int((vb == 999999).sum()), '>= 999998': int((vb >= 999998).sum()), 'max below 999999': float(vb[vb < 999999].max())}
print(json.dumps(out, indent=1, default=str))
# the served field rows those raw code rows land on (the test's join), both sides
code = ((v == 999999) & ~non).fillna(False).to_numpy(dtype=bool)
f0 = s3[code]
hh = f0.apply(lambda r: mali.i(pd.Series([r['grappe'], r['menage']])), axis=1)
pf = f0['s3aq01'].astype('Int64').astype('string') + '_' + f0['s3aq02'].astype('Int64').astype('string')
keys = pd.DataFrame({'t': '2014-15', 'i': hh.values, 'plot_id': pf.values,
                     'crop': mali._crop_labels(f0['s3aq03b']).values}).dropna(subset=['crop'])
keys = id_walk(keys.set_index(['t', 'i']), ll.Country('Mali').updated_ids).reset_index().drop_duplicates()
for s, f in fr.items():
    ff = f[~f.perennial.fillna(False).astype(bool)].copy()
    for c in ('t', 'i', 'plot_id', 'crop'):
        ff[c] = ff[c].astype('string'); keys[c] = keys[c].astype('string')
    j = ff.merge(keys, on=['t', 'i', 'plot_id', 'crop'], how='inner')
    print(json.dumps({f'served field rows on a raw 999999 code ({s})': [int(len(j)), int(j.Value_sold.notna().sum()),
                      int((j.Value_sold == 999999).sum()), int((j.Quantity_sold == 0).sum()), int((j.Quantity_sold > 0).sum())]}))
