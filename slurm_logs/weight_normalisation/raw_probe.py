"""Probe the RAW L2-country sample parquets: per-wave mean/sum, scale classes.

Answers the two questions a raw-layer scale test needs:
  1. is there an unambiguous gap between the mean-1 and expansion classes?
  2. within a country, are the EXPANSION waves' raw totals stable?
"""
import warnings
warnings.filterwarnings('ignore')
import numpy as np, pandas as pd
import lsms_library as ll
assert 'worktrees' in ll.__file__, ll.__file__
from lsms_library.paths import data_root

rows = []
for c in sorted(ll.countries()):
    p = data_root(c) / 'var' / 'sample.parquet'
    if not p.exists():
        continue
    raw = pd.read_parquet(p)
    names = list(raw.index.names)
    if 't' in names:
        t = raw.index.get_level_values('t')
    elif 't' in raw.columns:
        t = raw['t']
    else:
        print(f'{c}: NO t (index={names}, cols={list(raw.columns)[:6]})')
        continue
    if 'weight' not in raw.columns:
        print(f'{c}: no weight column, cols={list(raw.columns)}')
        continue
    w = pd.to_numeric(raw['weight'], errors='coerce')
    g = pd.DataFrame({'t': np.asarray(t, dtype=object), 'w': w.to_numpy()})
    for wave, sub in g.groupby('t', dropna=False):
        s = sub['w'].dropna()
        if s.empty:
            rows.append((c, str(wave), 0, np.nan, np.nan)); continue
        rows.append((c, str(wave), len(s), float(s.mean()), float(s.sum())))

df = pd.DataFrame(rows, columns=['country', 'wave', 'n', 'mean', 'sum'])
print(f'\ncountries with a raw parquet: {df.country.nunique()}   cells: {len(df)}')
w = df[df['n'] > 0].copy()
print(f'weighted cells: {len(w)}')

near1 = w[np.isclose(w['mean'], 1.0, rtol=0.01)]
rest = w[~np.isclose(w['mean'], 1.0, rtol=0.01)]
print(f'\nRAW mean within 1% of 1.0: {len(near1)} cells  '
      f'[{near1["mean"].min():.8f} .. {near1["mean"].max():.8f}]')
print(f'RAW mean elsewhere:        {len(rest)} cells  '
      f'[{rest["mean"].min():.4f} .. {rest["mean"].max():.4f}]')
print(f'\nGAP: largest near-1 mean {near1["mean"].max():.8f} '
      f'vs smallest other mean {rest["mean"].min():.4f}  '
      f'-> ratio {rest["mean"].min()/near1["mean"].max():.1f}x')
print('\nAny cell with mean in (1.01, 10)?')
mid = w[(w['mean'] > 1.01) & (w['mean'] < 10)]
print(mid.to_string() if len(mid) else '  NONE')

print('\n=== within-country EXPANSION-class raw total stability ===')
for c, g in rest.groupby('country'):
    g = g.sort_values('wave')
    if len(g) < 2:
        print(f'{c:<24} {len(g)} expansion wave(s) -- no ratio')
        continue
    tot = g.set_index('wave')['sum']
    r = (tot / tot.shift(1)).dropna()
    flag = '' if ((r > 0.2) & (r < 5.0)).all() else '   <<< OUT OF (0.2, 5.0)'
    print(f'{c:<24} totals {[f"{v:,.0f}" for v in tot]}  ratios '
          f'{[f"{v:.2f}" for v in r]}{flag}')

print('\n=== CotedIvoire raw, all waves ===')
print(w[w.country == 'CotedIvoire'].to_string(index=False))
