"""Is W2->W3 (i, plotid) the same physical plot?

Test: for households present in both waves, compare attributes that ought to be
time-invariant for a given piece of land.  Compare the TRUE pairing against a
NULL built by permuting plotid within the household -- if the true rate is not
above the null, the numbering carries no information.
"""
import warnings
warnings.filterwarnings('ignore')
import numpy as np, pandas as pd
from lsms_library.local_tools import get_dataframe

rng = np.random.default_rng(0)


def load(w, f):
    return get_dataframe(f'../../GhanaSPS/{w}/Data/{f}.dta')


HA = {'acre': 0.404694, 'acres': 0.404694, 'pole': 0.409551,
      'poles': 0.409551, 'rope': 0.236342, 'ropes': 0.236342,
      'robes': 0.236342, 'plot': 0.102388, 'plots': 0.102388}


def prep(w):
    d = load(w, '04h_agsection')
    unit = d['plotunit'] if 'plotunit' in d.columns else d['plotsizeunit']
    f = unit.astype(str).str.strip().str.lower().map(HA)
    out = pd.DataFrame({
        'i': d.FPrimary.astype(str),
        'plotid': pd.to_numeric(d.plotid, errors='coerce'),
        'ha': pd.to_numeric(d.totalsize, errors='coerce') * f,
        'soilcolor': d.soilcolor.astype(str).str.strip().str.lower(),
        'dist': pd.to_numeric(d.plotdistance, errors='coerce'),
        'region': d.plotlocation_region.astype(str).str.strip().str.lower(),
    })
    return out.dropna(subset=['plotid'])


a2, a3 = prep('2013-14'), prep('2017-18')
common = set(a2.i) & set(a3.i)
a2, a3 = a2[a2.i.isin(common)], a3[a3.i.isin(common)]
print(f'households in both waves: {len(common)}   '
      f'W2 plots {len(a2)}   W3 plots {len(a3)}')


def agree(m):
    r = {}
    ok = m.ha_x.notna() & m.ha_y.notna()
    r['area within 10%'] = (
        (np.abs(m.ha_x - m.ha_y) <= 0.10 * np.maximum(m.ha_x, m.ha_y))[ok].mean(),
        int(ok.sum()))
    for col, lab in [('soilcolor', 'soil colour'), ('region', 'plot region')]:
        x, y = m[f'{col}_x'], m[f'{col}_y']
        ok = (~x.isin(['nan', ''])) & (~y.isin(['nan', '']))
        r[lab] = ((x == y)[ok].mean(), int(ok.sum()))
    ok = m.dist_x.notna() & m.dist_y.notna()
    r['distance within 20%'] = (
        (np.abs(m.dist_x - m.dist_y)
         <= 0.20 * np.maximum(m.dist_x, m.dist_y))[ok].mean(), int(ok.sum()))
    return r


true = a2.merge(a3, on=['i', 'plotid'], suffixes=('_x', '_y'))
print(f'\nTRUE pairing on (i, plotid): {len(true)} matched plots')
tr = agree(true)
for k, (v, n) in tr.items():
    print(f'  {k:<22} {100*v:5.1f}%   (n={n})')

# NULL: permute plotid within household in W3, then match
print('\nNULL (plotid permuted within household in W3), 20 draws:')
acc = {k: [] for k in tr}
for _ in range(20):
    b3 = a3.copy()
    b3['plotid'] = b3.groupby('i')['plotid'].transform(
        lambda s: rng.permutation(s.values))
    m = a2.merge(b3, on=['i', 'plotid'], suffixes=('_x', '_y'))
    for k, (v, n) in agree(m).items():
        acc[k].append(v)
for k in tr:
    mu, sd = np.mean(acc[k]), np.std(acc[k])
    t, _ = tr[k]
    print(f'  {k:<22} null {100*mu:5.1f}% +/- {100*sd:.1f}   '
          f'true {100*t:5.1f}%   lift {100*(t-mu):+5.1f} pts')

# How much of the null is forced? households with exactly 1 plot in both waves
n2 = a2.groupby('i').size(); n3 = a3.groupby('i').size()
single = {h for h in common if n2.get(h, 0) == 1 and n3.get(h, 0) == 1}
print(f'\nhouseholds with exactly ONE plot in both waves: {len(single)} '
      f'({100*len(single)/len(common):.1f}% of common) -- for these the '
      f'permutation is a no-op, so the null is inflated')
multi = true[~true.i.isin(single)]
print(f'\nTRUE pairing restricted to MULTI-plot households ({len(multi)} plots):')
for k, (v, n) in agree(multi).items():
    print(f'  {k:<22} {100*v:5.1f}%   (n={n})')
acc2 = {k: [] for k in tr}
a2m, a3m = a2[~a2.i.isin(single)], a3[~a3.i.isin(single)]
for _ in range(20):
    b3 = a3m.copy()
    b3['plotid'] = b3.groupby('i')['plotid'].transform(
        lambda s: rng.permutation(s.values))
    m = a2m.merge(b3, on=['i', 'plotid'], suffixes=('_x', '_y'))
    for k, (v, n) in agree(m).items():
        acc2[k].append(v)
print('  ...against the multi-plot null:')
for k in tr:
    mu = np.mean(acc2[k]); sd = np.std(acc2[k])
    t = agree(multi)[k][0]
    print(f'  {k:<22} null {100*mu:5.1f}% +/- {100*sd:.1f}   '
          f'true {100*t:5.1f}%   lift {100*(t-mu):+5.1f} pts')
