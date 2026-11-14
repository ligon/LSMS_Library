"""Per-wave drill on countries the author did not measure and that move.

usage: movers.py BASE_TX_PY OUTDIR COUNTRY [...]   (root already warm from extras.py)
"""
import os
import sys
import json
import warnings
import importlib.util

sys.path.insert(0, os.path.dirname(__file__))
import guard  # noqa: F401,E402

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402
from lsms_library import transformations as T  # noqa: E402

spec = importlib.util.spec_from_file_location('lsms_library._base_tx', sys.argv[1])
TB = importlib.util.module_from_spec(spec)
sys.modules['lsms_library._base_tx'] = TB
spec.loader.exec_module(TB)
warnings.simplefilter('ignore')
OUT = sys.argv[2]


def kg_by_t(q):
    u = np.asarray(q.index.get_level_values('u').astype(str))
    t = np.asarray(q.index.get_level_values('t').astype(str))
    m = u == 'kg'
    return {k: float(v) for k, v in pd.Series(q['Quantity'].to_numpy()[m]).groupby(t[m]).sum().items()}


for name in sys.argv[3:]:
    c = ll.Country(name)
    fa = c._aggregate_wave_data(None, 'food_acquired')
    kw = dict(unit_kg=c._unit_kg_factors(), no_kg=c._no_kg_factor_cells())
    kc, kb = T.food_kg_factors(fa, **kw), TB.food_kg_factors(fa, **kw)
    t = np.asarray(fa.index.get_level_values('t').astype(str))
    s = np.asarray(fa.index.get_level_values('s'), dtype=object)
    cf, bf = kc['kg_per_unit'].to_numpy(float), kb['kg_per_unit'].to_numpy(float)
    same = (np.isnan(bf) & np.isnan(cf)) | np.isclose(bf, cf, rtol=1e-9, atol=0)
    r = np.where(same, 1.0, cf / bf)
    rec = {'rows_changed_by_t': {k: int(v) for k, v in pd.Series(t[~same]).value_counts().sort_index().items()},
           'rows_changed_by_s': {str(k): int(v) for k, v in pd.Series(s[~same]).value_counts().items()},
           'ratio_quantiles_changed': [float(np.nanquantile(r[~same], q)) for q in (0.01, 0.1, 0.5, 0.9, 0.99)] if (~same).any() else None,
           'rows_changed_over_2x': int((~same & ((r > 2) | (r < 0.5))).sum())}
    qc = T.food_quantities_from_acquired(fa, units='kgs', **kw)
    qb = TB.food_quantities_from_acquired(fa, units='kgs', **kw)
    a, b = kg_by_t(qb), kg_by_t(qc)
    rec['kg_by_t'] = {w: [a.get(w), b.get(w), (b.get(w, 0) / a[w] - 1) * 100 if a.get(w) else None] for w in sorted(set(a) | set(b))}
    rec['kg_total'] = [sum(a.values()), sum(b.values())]
    pc = T.food_prices_from_acquired(fa, units='kgvalue', **kw)
    pb = TB.food_prices_from_acquired(fa, units='kgvalue', **kw)
    j = pb[['Price']].join(pc[['Price']], how='outer', rsuffix='_c')
    rec['kgvalue'] = {'rows': [len(pb), len(pc)],
                      'only_base': int(j['Price_c'].isna().sum()), 'only_cand': int(j['Price'].isna().sum()),
                      'changed': int((~np.isclose(j['Price'], j['Price_c'], rtol=1e-9) & j.notna().all(axis=1)).sum()),
                      'median_ratio_changed': None}
    ch = j.dropna()
    ch = ch[~np.isclose(ch['Price'], ch['Price_c'], rtol=1e-9)]
    if len(ch):
        rec['kgvalue']['median_ratio_changed'] = float((ch['Price_c'] / ch['Price']).median())
    with open(f'{OUT}/{name}_movers.json', 'w') as f:
        json.dump(rec, f, indent=1, default=str)
    print('movers', name, rec['kg_total'], flush=True)
