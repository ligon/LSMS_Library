#!/usr/bin/env python
"""GH #782 acceptance, phase 2: census + derived tables + nutrition, in-process.

Phase 1 wrote the food_acquired caches; this reads them warm.  The census runs
in-process rather than via a parquet dump because food_acquired's `visit` index
level is mixed-type ('since last visit' in the 1980s waves, ints elsewhere) and
pyarrow refuses it -- a pre-existing quirk, not something #782 introduced.
"""
import re
import sys
import time

import pandas as pd

import lsms_library as ll
from lsms_library.paths import countries_root, data_root

TAG = sys.argv[1]


def key(l):
    s = re.sub(r'[^a-z0-9]+', ' ', str(l).casefold()).strip()
    return ' '.join(w[:-1] if len(w) > 3 and w.endswith('s') and not w.endswith('ss')
                    else w for w in s.split())


print('countries_root:', countries_root())
print('data_root     :', data_root())
c = ll.Country('GhanaLSS')

t0 = time.time()
fa = c.food_acquired()
print(f'[{TAG}] food_acquired rows={len(fa):,}  ({time.time()-t0:.0f}s)')

d = pd.DataFrame({'t': fa.index.get_level_values('t').astype(str),
                  'j': fa.index.get_level_values('j').astype(str)})
print(f'[{TAG}] distinct j = {d["j"].nunique()}')
print(f'[{TAG}] per wave:')
for w, g in d.groupby('t'):
    low = g['j'].apply(lambda s: bool(s) and s[0].islower())
    print(f'[{TAG}]   {w}: rows={len(g):>9,} distinct_j={g["j"].nunique():>4} '
          f'lowercase_rows={int(low.sum()):>7,} distinct_lowercase={g.loc[low,"j"].nunique():>3}')

lab = pd.Series(sorted(d['j'].unique()))
grp = lab.groupby(lab.map(key)).apply(list)
coll = {k: v for k, v in grp.items() if len(v) > 1}
rby = d['j'].value_counts()
nr = sum(rby.get(x, 0) for v in coll.values() for x in v)
print(f'[{TAG}] COLLISION GROUPS = {len(coll)}   rows = {nr:,} '
      f'({100*nr/len(fa):.1f}%)')
for k in sorted(coll):
    print(f'[{TAG}]    {coll[k]}')

for name in ('food_expenditures', 'food_quantities', 'food_prices'):
    t0 = time.time()
    try:
        x = getattr(c, name)()
        print(f'[{TAG}] {name}: rows={len(x):,} ({time.time()-t0:.0f}s)')
    except Exception as e:
        print(f'[{TAG}] {name}: FAILED {type(e).__name__}: {e}')

try:
    t0 = time.time()
    cp = c.community_prices()
    cj = set(cp.index.get_level_values('j').astype(str))
    fj = set(d['j'])
    onaxis = cp.index.get_level_values('j').astype(str).isin(fj)
    print(f'[{TAG}] community_prices: rows={len(cp):,} distinct_j={len(cj)} '
          f'rows_with_j_on_food_axis={int(onaxis.sum()):,} '
          f'({100*onaxis.mean():.1f}%)  ({time.time()-t0:.0f}s)')
except Exception as e:
    print(f'[{TAG}] community_prices: FAILED {type(e).__name__}: {e}')

# nutrition coverage: share of food MASS carrying an FCT code
try:
    sys.path.insert(0, str(countries_root() / 'GhanaLSS' / '_'))
    import nutrition
    bw, bl = nutrition._load_food_codes(countries_root())
    q = nutrition._quantities(countries_root())
    n = q.rename('Quantity').reset_index()
    n = n.merge(bw[['t', 'j', 'FCT Code']], on=['t', 'j'], how='left')
    fb = n['j'].map(bl)
    n['FCT Code'] = n['FCT Code'].where(n['FCT Code'].notna(), fb).fillna('')
    tot = n['Quantity'].sum()
    cov = n.loc[n['FCT Code'] != '', 'Quantity'].sum()
    print(f'[{TAG}] NUTRITION coverage: {100*cov/tot:.2f}% of mass ({cov:,.0f}/{tot:,.0f})')
    miss = (n[n['FCT Code'] == ''].groupby('j')['Quantity'].sum()
            .sort_values(ascending=False).head(8))
    for j, v in miss.items():
        print(f'[{TAG}]    unmapped {j!r:34} {v:,.0f}')
except Exception:
    import traceback; traceback.print_exc()

try:
    t0 = time.time()
    f = ll.Feature('food_acquired')(['GhanaLSS'])
    print(f'[{TAG}] Feature(food_acquired)([GhanaLSS]): rows={len(f):,} '
          f'levels={list(f.index.names)} ({time.time()-t0:.0f}s)')
except Exception as e:
    print(f'[{TAG}] Feature: FAILED {type(e).__name__}: {e}')
