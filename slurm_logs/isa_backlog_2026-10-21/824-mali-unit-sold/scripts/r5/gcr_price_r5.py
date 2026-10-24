"""gross_crop_revenue by household, base vs branch (changed households by wave
and direction), and the tree price table (aggregates, every cell's n).
usage: gcr_price_r5.py <base dir> <branch dir> <checkout> <price table out>"""
import os, sys, warnings
warnings.simplefilter('ignore')
import numpy as np, pandas as pd
base_dir, br_dir, checkout, ptab = sys.argv[1:5]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(checkout) + os.sep), lsms_library.__file__
from lsms_library import transformations as T
IDX = ['i', 't', 'v', 'plot_id', 'crop']
g = {}
for s, d in (('base', base_dir), ('branch', br_dir)):
    cp = pd.read_parquet(f'{d}/crop_production.parquet').set_index(IDX)
    r = T.gross_crop_revenue(cp)
    g[s] = r.iloc[:, 0] if isinstance(r, pd.DataFrame) else r
a, b = g['base'], g['branch']
common = a.index.intersection(b.index)
d = ~np.isclose(a[common].to_numpy(float), b[common].to_numpy(float), equal_nan=True)
ch = common[d]
print('gross_crop_revenue rows', len(a), len(b), 'added', len(b.index.difference(a.index)), 'dropped', len(a.index.difference(b.index)))
for w in sorted(set(ch.get_level_values('t'))):
    m = ch.get_level_values('t') == w
    up = int((b[ch[m]] > a[ch[m]]).sum()); down = int((b[ch[m]] < a[ch[m]]).sum())
    print(f'  {w}: changed {int(m.sum())} (up {up}, down {down}); sum {a[a.index.get_level_values("t") == w].sum():.6g} -> {b[b.index.get_level_values("t") == w].sum():.6g}')
# harvest_kg and yield_kg, base vs branch
hk, yk = {}, {}
for s_, d_ in (('base', base_dir), ('branch', br_dir)):
    cp = pd.read_parquet(f'{d_}/crop_production.parquet').set_index(IDX)
    h = T.harvest_kg(cp); hk[s_] = h
    pf = pd.read_parquet(f'{d_}/plot_features.parquet')
    import json as _json
    pf_idx = _json.load(open(f'{d_}/summary.json'))['plot_features']['index']
    try:
        yk[s_] = T.yield_kg(cp, pf.set_index(pf_idx))
    except Exception as e:
        yk[s_] = f'ERR {type(e).__name__}: {str(e)[:200]}'
def _eq(x, y):
    if isinstance(x, str) or isinstance(y, str):
        return f'{x!r} / {y!r}'
    return {'shape': [list(x.shape), list(y.shape)], 'equal': bool(x.equals(y))}
print('harvest_kg base vs branch', _eq(hk['base'], hk['branch']))
print('yield_kg base vs branch', _eq(yk['base'], yk['branch']))
f = pd.read_parquet(f'{br_dir}/crop_production.parquet')
tr = f[f.perennial.fillna(False).astype(bool)]
kg = tr[(tr.Unit_sold == 'Kg').fillna(False) & (tr.Quantity_sold > 0).fillna(False) & (tr.Value_sold > 0).fillna(False)]
tbl = pd.DataFrame({'wave': kg.t, 'crop': kg.crop, 'same': kg.Value_sold / kg.Quantity_sold,
                    'total': kg.Value_sold / (kg.Quantity_sold * kg.months_harvested.astype('float64'))})
tbl = tbl.groupby(['wave', 'crop']).agg(n=('same', 'size'), same_footing=('same', 'median'), total_reading=('total', 'median'))
tbl['thin (n < 5)'] = np.where(tbl.n < 5, 'thin', '')
with open(ptab, 'w') as fh:
    fh.write('# Mali crop_production, tree rows sold in Kg and priced (Quantity_sold > 0, Value_sold > 0),\n'
             '# served table at the branch head (cold build).  Median FCFA/kg:\n'
             '#   same_footing  = Value_sold / Quantity_sold (the value on the per-month quantity\'s footing; served)\n'
             '#   total_reading = Value_sold / (Quantity_sold x months_harvested) (the value read as a total\n'
             '#                   over the harvest months, the quantity per month)\n'
             '# n is the cell; cells with n < 5 are thin.  Aggregates only.\n')
    fh.write(tbl.round(1).to_string() + '\n')
print(open(ptab).read())
