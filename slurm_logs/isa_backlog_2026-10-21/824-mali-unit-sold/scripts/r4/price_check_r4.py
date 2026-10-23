"""Round 4: tree prices (Value_sold / Quantity_sold) before and after, and the
Kg spot check; gross_crop_revenue by wave.
usage: price_check_r4.py <old-label> <new-label> <out dir>.  Aggregates only."""
import sys, warnings; warnings.simplefilter('ignore')
import numpy as np, pandas as pd
old, new, OUT = sys.argv[1:4]
b = pd.read_parquet(f'{OUT}/{old}_served.parquet'); n = pd.read_parquet(f'{OUT}/{new}_served.parquet')
assert b.index.equals(n.index)
tree = n.perennial.fillna(False).to_numpy(bool)
t = n.index.get_level_values('t'); crop = n.index.get_level_values('crop')
def price(f):
    return (f.Value_sold / f.Quantity_sold).where((f.Quantity_sold > 0) & (f.Value_sold > 0))
pb, pn = price(b), price(n)
for w in ['2014-15', '2017-18']:
    m = tree & (t == w)
    both = m & pb.notna().to_numpy() & pn.notna().to_numpy()
    same = np.isclose(pb[both].to_numpy(float), pn[both].to_numpy(float))
    print(f'{w} tree: priced {old} {int((m & pb.notna().to_numpy()).sum())}, {new} {int((m & pn.notna().to_numpy()).sum())}; '
          f'priced in both {int(both.sum())}, price identical on {int(same.sum())}; new-only priced {int((m & pn.notna().to_numpy() & pb.isna().to_numpy()).sum())}')
for lab, f, p in [(old, b, pb), (new, n, pn)]:
    print(f'== {lab}: tree price per Kg sold (FCFA/kg), crops with n >= 5')
    for w in ['2014-15', '2017-18']:
        m = tree & (t == w) & (f.Unit_sold == 'Kg').fillna(False).to_numpy(bool) & p.notna().to_numpy()
        g = pd.DataFrame({'crop': crop[m], 'p': p[m].to_numpy()}).groupby('crop').p.agg(['size', 'median'])
        for c, r in g[g['size'] >= 5].iterrows():
            print(f'   {w} {c:16s} n={int(r["size"]):3d} median {r["median"]:.1f}')
for name in ['gross_crop_revenue', 'gross_crop_revenue_j']:
    a = pd.read_parquet(f'{OUT}/{old}_{name}.parquet').iloc[:, 0]; z = pd.read_parquet(f'{OUT}/{new}_{name}.parquet').iloc[:, 0]
    sa = a.groupby(level='t').sum(); sz = z.groupby(level='t').sum()
    common = a.index.intersection(z.index)
    d = ~np.isclose(a[common].to_numpy(float), z[common].to_numpy(float), equal_nan=True)
    ch = common[d]
    up = int((z[ch] > a[ch]).sum()); down = int((z[ch] < a[ch]).sum())
    print(f'{name}: rows {len(a)} -> {len(z)}; by wave sum ' + ', '.join(f'{w} {sa.get(w, 0):.4g} -> {sz.get(w, 0):.4g}' for w in sorted(set(sa.index) | set(sz.index))) +
          f'; changed in place {int(d.sum())} (up {up}, down {down}) by wave {pd.Index(ch.get_level_values("t")).value_counts().to_dict()}; '
          f'added {len(z.index.difference(a.index))}, dropped {len(a.index.difference(z.index))}')
