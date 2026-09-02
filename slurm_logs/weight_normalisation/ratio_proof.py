"""Ratio-invariance proof on real data + the pooled caveat made concrete.

Compares the RAW weights in the L2-country parquet against the NORMALISED
weights the API returns, for a real country.
  - per-wave weighted shares must agree to floating tolerance (the guarantee);
  - the pooled cross-wave weighted share generally does NOT (the caveat).
"""
import sys, warnings
warnings.filterwarnings('ignore')
import numpy as np, pandas as pd
import lsms_library as ll
assert 'worktrees' in ll.__file__, ll.__file__
from lsms_library.paths import data_root

COUNTRIES = sys.argv[1:] or ['CotedIvoire', 'Malawi', 'Uganda']


def _indicator(s):
    """Rural -> 1/0. Handles bool, 'Rural'/'Urban' strings and numerics."""
    if s.dtype == bool or isinstance(s.dtype, pd.BooleanDtype):
        return s.astype('Float64').astype('float64')
    t = s.astype('string').str.strip().str.lower()
    if t.isin(['rural', 'urban', 'semi-urban', 'true', 'false']).any():
        return t.map({'rural': 1.0, 'true': 1.0,
                      'urban': 0.0, 'semi-urban': 0.0, 'false': 0.0})
    return pd.to_numeric(s, errors='coerce')


for c in COUNTRIES:
    print('=' * 78)
    print(c)
    api = ll.Country(c).sample()
    raw_path = data_root(c) / 'var' / 'sample.parquet'
    if not raw_path.exists():
        print('  no L2-country parquet; skipping'); continue
    raw = pd.read_parquet(raw_path)
    if list(raw.index.names) != ['i', 't']:
        raw = raw.reset_index().set_index(['i', 't'])
    raw.index = pd.MultiIndex.from_arrays(
        [raw.index.get_level_values(0).astype(str),
         raw.index.get_level_values(1).astype(str)], names=['i', 't'])
    a = api.copy()
    a.index = pd.MultiIndex.from_arrays(
        [a.index.get_level_values('i').astype(str),
         a.index.get_level_values('t').astype(str)], names=['i', 't'])

    print('  Rural sample values:', list(pd.unique(a['Rural'].astype('string')))[:6])
    j = a[['weight', 'Rural']].join(
        raw[['weight']].rename(columns={'weight': 'weight_raw'}), how='inner')
    j['x'] = _indicator(j['Rural'])
    j['weight'] = pd.to_numeric(j['weight'], errors='coerce')
    j['weight_raw'] = pd.to_numeric(j['weight_raw'], errors='coerce')
    j = j.dropna(subset=['weight', 'weight_raw', 'x'])
    if j.empty:
        print('  no overlapping non-null rows'); continue
    print(f'  matched rows with weight+Rural: {len(j)}')
    print(f'  {"wave":<12} {"n":>7} {"mean raw":>14} {"mean api":>9} '
          f'{"share raw":>14} {"share api":>14} {"absdiff":>9}')
    worst = 0.0
    for wave, g in j.groupby(level='t'):
        if g.weight_raw.sum() == 0 or g.weight.sum() == 0:
            print(f'  {wave:<12} {len(g):>7}  (zero weight sum -- skipped)'); continue
        s_raw = (g.weight_raw * g.x).sum() / g.weight_raw.sum()
        s_api = (g.weight * g.x).sum() / g.weight.sum()
        worst = max(worst, abs(s_raw - s_api))
        print(f'  {wave:<12} {len(g):>7} {g.weight_raw.mean():>14.4f} '
              f'{g.weight.mean():>9.6f} {s_raw:>14.11f} {s_api:>14.11f} '
              f'{abs(s_raw - s_api):>9.1e}')
    print(f'  WORST per-wave |share_raw - share_api| = {worst:.3e}  '
          f'-> {"PASS (invariant)" if worst < 1e-9 else "FAIL"}')
    pr = (j.weight_raw * j.x).sum() / j.weight_raw.sum()
    pa = (j.weight * j.x).sum() / j.weight.sum()
    print(f'  POOLED cross-wave weighted Rural share: raw {pr:.6f}  '
          f'normalised {pa:.6f}  diff {pa - pr:+.6f}')
