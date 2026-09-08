"""Re-derive the GH #770 numbers pinned by PR #784, cold on the merged tree.

Usage: BEFORE=0|1 python measure_770.py out.json

BEFORE=1 empties ``transformations._CURRENCY_DENOMINATED_UNITS`` in-process;
every site that consults it reads the module global at call time
(``_is_currency_denominated``, ``conversion_to_kgs`` line ~851,
``_get_kg_factors`` line ~1078), so this reproduces the pre-#770 behaviour on
the IDENTICAL food_acquired frame -- isolating the fix from #785/#803 drift.
"""
import json
import numpy as np
import os
import sys
import warnings

import lsms_library as ll

assert 'wt-pr-784' in ll.__file__, ll.__file__
from lsms_library.paths import data_root

assert 'data-pr-784' in str(data_root()), data_root()
from lsms_library import transformations as T

BEFORE = os.environ.get('BEFORE') == '1'
if BEFORE:
    T._CURRENCY_DENOMINATED_UNITS = frozenset()

out = {'BEFORE': BEFORE, 'pkg': ll.__file__, 'data_root': str(data_root())}
for name in sys.argv[2:] or ['GhanaLSS', 'Panama']:
    c = ll.Country(name, preload_panel_ids=False, verbose=False)
    fa = c.food_acquired()
    r = {}
    r['fa_rows'] = len(fa)
    u = fa.index.get_level_values('u').astype(str)
    r['fa_value_rows'] = int((u.str.lower() == 'value').sum())
    r['fa_value_rows_per_wave'] = {
        str(k): int(v) for k, v in
        fa[np.asarray(u.str.lower() == "value")].groupby(level='t').size().items()}
    r['fa_j_nunique'] = int(fa.index.get_level_values('j').nunique())
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        f = T._get_kg_factors(fa)
    r['n_factors'] = len(f)
    r['factor_keys'] = sorted(f)
    r['value_factor'] = f.get('value')
    for units in ['kgvalue', 'unitvalue', 'kgprice', 'unitprice']:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            p = c.food_prices(units=units)
        r[f'prices_{units}_rows'] = len(p)
        per_wave = p.groupby(level='t').size()
        r[f'prices_{units}_per_wave'] = {str(k): int(v) for k, v in per_wave.items()}
        if units == 'kgvalue':
            r['kgvalue_std_per_wave'] = {
                str(k): float(v) for k, v in p.groupby(level='t')['Price'].std().items()}
            r['kgvalue_median_per_wave'] = {
                str(k): float(v) for k, v in p.groupby(level='t')['Price'].median().items()}
            r['kgvalue_tally'] = p.attrs.get('price_rows_dropped')
    for units in ['kgs', 'units']:
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            q = c.food_quantities(units=units)
        r[f'quantities_{units}_rows'] = len(q)
        uq = q.index.get_level_values('u').astype(str)
        r[f'quantities_{units}_kg_rows'] = int((uq == 'kg').sum())
        r[f'quantities_{units}_value_rows'] = int((uq.str.lower() == 'value').sum())
    out[name] = r
    print(name, 'done', file=sys.stderr, flush=True)

with open(sys.argv[1], 'w') as fh:
    json.dump(out, fh, indent=1, default=str)
print('wrote', sys.argv[1])
