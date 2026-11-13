"""Round 3: served tree prices (Value_sold / Quantity_sold) vs the H-monthly
ratio (per-month value / per-month quantity), and the cashew spot check.
usage: price_check_r3.py <label> [<label> ...].  Aggregates only."""
import sys, warnings; warnings.simplefilter('ignore')
import numpy as np, pandas as pd
OUT = '/local/job40390255/data-private/824-ml-r2/out'
dev = pd.read_parquet(f'{OUT}/dev_served.parquet')   # dev served the per-month qs (s3bq13a / s11fq14a) and raw value
for lab in sys.argv[1:]:
    n = pd.read_parquet(f'{OUT}/{lab}_served.parquet')
    tree = n.perennial.fillna(False).to_numpy(bool)
    t = n.index.get_level_values('t'); crop = n.index.get_level_values('crop')
    p = (n.Value_sold / n.Quantity_sold).where((n.Quantity_sold > 0) & (n.Value_sold > 0))
    print(f'== {lab}')
    for w in ['2014-15', '2017-18']:
        m = tree & (t == w) & p.notna().to_numpy()
        # H-monthly ratio straight from the development frame, where the tree
        # sold quantity was the per-month answer and the value was recorded
        h = (dev.Value_sold / dev.Quantity_sold)[m]
        same = np.isclose(p[m].to_numpy(float), h.to_numpy(float), equal_nan=True)
        print(f'  {w}: priced tree rows {int(m.sum())}; price == per-month value / per-month qty sold on {int(same.sum())}')
    for w in ['2014-15', '2017-18']:
        m = tree & (t == w) & (n.Unit_sold == 'Kg').fillna(False).to_numpy(bool) & p.notna().to_numpy()
        g = pd.DataFrame({'crop': crop[m], 'p': p[m].to_numpy()}).groupby('crop').p.agg(['size', 'median'])
        print(f'  {w} tree price per Kg sold (FCFA/kg), crops with n >= 5:')
        for c, r in g[g['size'] >= 5].iterrows():
            print(f'     {c:20s} n={int(r["size"]):3d} median {r["median"]:.0f}')
