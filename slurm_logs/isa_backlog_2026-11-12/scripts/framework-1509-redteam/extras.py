"""In-process base-vs-candidate TRANSFORM comparison on one cold food_acquired.

usage: extras.py BASE_TX_PY OUTDIR COUNTRY [...]
The candidate package is imported normally; the base ``transformations.py``
(identical sibling modules: the diff touches only transformations.py and
country.py docstrings) is loaded as ``lsms_library._base_tx``.
"""
import os
import sys
import json
import time
import traceback
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
assert 'item_unit_nonpurchase' not in TB.FOOD_KG_FACTOR_LAYERS
assert 'item_unit_nonpurchase' in T.FOOD_KG_FACTOR_LAYERS
OUT = sys.argv[2]
warnings.simplefilter('ignore')


def tv(df):
    return np.asarray(df.index.get_level_values('t').astype(str))


def kg_total(q):
    u = np.asarray(q.index.get_level_values('u').astype(str))
    return float(q.loc[u == 'kg', 'Quantity'].sum())


for name in sys.argv[3:]:
    rec = {'country': name}
    t0 = time.time()
    try:
        c = ll.Country(name)
        fa = c._aggregate_wave_data(None, 'food_acquired')
        rec['build_s'] = round(time.time() - t0, 1)
        rec['rows'] = len(fa)
        idx = list(fa.index.names)
        rec['has_s'] = 's' in idx
        s = np.asarray(fa.index.get_level_values('s'), dtype=object) if 's' in idx else None
        u = np.asarray(fa.index.get_level_values('u'), dtype=object)
        E = pd.to_numeric(fa['Expenditure'], errors='coerce').to_numpy(float) if 'Expenditure' in fa else None
        Q = pd.to_numeric(fa['Quantity'], errors='coerce').to_numpy(float) if 'Quantity' in fa else None
        if E is not None and Q is not None and s is not None:
            cur = np.asarray(T._is_currency_denominated(pd.Index(u)), dtype=bool)
            priced = np.isfinite(E) & (E > 0) & np.isfinite(Q) & (Q > 0) & ~cur
            nonzero_E = np.isfinite(E) & (E != 0)
            rec['priced_by_s'] = {str(k): int(v) for k, v in pd.Series(s[priced]).value_counts(dropna=False).items()}
            rec['nonzero_E_nonpurchase'] = int((nonzero_E & (s != 'purchased')).sum())
            rec['negative_E'] = int((np.isfinite(E) & (E < 0)).sum())
        kw = dict(unit_kg=c._unit_kg_factors(), no_kg=c._no_kg_factor_cells())
        kc = T.food_kg_factors(fa, **kw)
        kb = TB.food_kg_factors(fa, **kw)
        cs, bs = kc['KgFactorSource'].to_numpy(), kb['KgFactorSource'].to_numpy()
        cf, bf = kc['kg_per_unit'].to_numpy(float), kb['kg_per_unit'].to_numpy(float)
        same = (np.isnan(bf) & np.isnan(cf)) | np.isclose(bf, cf, rtol=1e-9, atol=0)
        rec['kgf_rows_changed'] = int((~same).sum())
        rec['kgf_src_changed'] = int((cs != bs).sum())
        ct = pd.crosstab(pd.Series(bs, name='b'), pd.Series(cs, name='c'))
        rec['transitions'] = {f'{a}->{b}': int(ct.loc[a, b]) for a in ct.index
                              for b in ct.columns if ct.loc[a, b] and a != b}
        rec['cand_sources'] = kc.attrs['kg_factor_sources']
        rec['cand_evidence'] = kc.attrs['kg_factor_evidence']
        for mode in ('kgs', 'units'):
            qc = T.food_quantities_from_acquired(fa, units=mode, **kw)
            qb = TB.food_quantities_from_acquired(fa, units=mode, **kw)
            r = {'rows': [len(qb), len(qc)]}
            if mode == 'kgs':
                r['kg'] = [kg_total(qb), kg_total(qc)]
                try:
                    pd.testing.assert_frame_equal(qb.sort_index(), qc.sort_index(), check_dtype=False)
                    r['equal'] = True
                except AssertionError:
                    r['equal'] = False
            else:
                drop = T._unit_sentinel_mask(qb)
                try:
                    pd.testing.assert_frame_equal(qb[~drop].sort_index(), qc.sort_index(), check_dtype=False)
                    r['cand_eq_base_minus_unknown'] = True
                except AssertionError:
                    r['cand_eq_base_minus_unknown'] = False
                r['tally'] = qc.attrs.get('quantity_rows_dropped')
            rec[f'fq_{mode}'] = r
        for mode in ('kgvalue', 'kgprice', 'unitvalue'):
            try:
                pc = T.food_prices_from_acquired(fa, units=mode, **kw)
                pb = TB.food_prices_from_acquired(fa, units=mode, **kw)
            except Exception as e:  # noqa: BLE001
                rec[f'fp_{mode}'] = {'error': f'{type(e).__name__}: {e}'[:300]}
                continue
            r = {'rows': [len(pb), len(pc)]}
            if mode == 'unitvalue':
                ub = pb.index.get_level_values('u')
                drop = np.asarray(T._is_currency_denominated(ub), dtype=bool) | T._unit_sentinel_mask(pb)
                try:
                    pd.testing.assert_frame_equal(pb[~drop].sort_index(), pc.sort_index(), check_dtype=False)
                    r['cand_eq_base_minus_label'] = True
                except AssertionError:
                    r['cand_eq_base_minus_label'] = False
                r['base_value_unknown_groups'] = int(drop.sum())
                r['tally'] = {k: pc.attrs['price_rows_dropped'].get(k) for k in ('currency', 'unknown', 'money_only', 'dropped_total')}
            else:
                try:
                    pd.testing.assert_frame_equal(pb.sort_index(), pc.sort_index(), check_dtype=False)
                    r['equal'] = True
                except AssertionError:
                    r['equal'] = False
            rec[f'fp_{mode}'] = r
    except Exception:  # noqa: BLE001
        rec['fatal'] = traceback.format_exc()[-3000:]
    rec['seconds'] = round(time.time() - t0, 1)
    with open(f'{OUT}/{name}.json', 'w') as f:
        json.dump(rec, f, indent=1, default=str)
    print('extras done', name, rec.get('seconds'), rec.get('kgf_rows_changed'), flush=True)
