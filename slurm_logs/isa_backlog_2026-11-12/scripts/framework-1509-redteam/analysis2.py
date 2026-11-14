"""Post-hoc drill-downs on the saved arms.  usage: analysis2.py OUTROOT"""
import os
import sys
import json
import traceback

sys.path.insert(0, os.path.dirname(__file__))
import guard  # noqa: F401,E402

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from lsms_library import transformations as T  # noqa: E402

ROOT = sys.argv[1]
NP, UNP = 'item_unit_nonpurchase', 'unit_nonpurchase'
res = {}


def rd(arm, name, what):
    p = f'{ROOT}/{arm}/{name}_{what}.parquet'
    return pd.read_parquet(p) if os.path.exists(p) else None


for name in ('GhanaLSS', 'Uganda', 'Cambodia', 'Serbia', 'Malawi', 'Niger', 'Ethiopia'):
    try:
        fa, kb, kc = rd('cand', name, 'fa'), rd('base', name, 'kgf'), rd('cand', name, 'kgf')
        if fa is None or kb is None or kc is None:
            continue
        r = {}
        cs, bs = kc['KgFactorSource'].to_numpy(), kb['KgFactorSource'].to_numpy()
        cf, bf = kc['kg_per_unit'].to_numpy(float), kb['kg_per_unit'].to_numpy(float)
        Q = pd.to_numeric(fa['Quantity'], errors='coerce').to_numpy(float)
        Q = np.where(np.isfinite(Q) & (Q > 0), Q, np.nan)
        t = fa['t'].astype(str).to_numpy()
        s = fa['s'].astype(object).to_numpy()
        npc = kc['kg_item_unit_nonpurchase'].to_numpy(float)
        unpc = kc['kg_unit_nonpurchase'].to_numpy(float)
        has_p = kc['kg_cell_has_purchase'].to_numpy(bool)
        # 1. Ladder order: a no-purchase cell served the purchase `unit` rung
        #    although its own item-level non-purchase estimate exists.
        m = (cs == 'unit') & ~has_p & np.isfinite(npc)
        r['unit_outranks_item_fallback'] = {
            'rows': int(m.sum()),
            'by_t': {k: int(v) for k, v in pd.Series(t[m]).value_counts().sort_index().items()},
            'by_s': {str(k): int(v) for k, v in pd.Series(s[m]).value_counts().items()},
            'by_base_src': {str(k): int(v) for k, v in pd.Series(bs[m]).value_counts().items()},
            'kg_as_served_unit': float(np.nansum(Q[m] * cf[m])),
            'kg_if_item_fallback': float(np.nansum(Q[m] * npc[m])),
            'kg_base': float(np.nansum(Q[m] * bf[m])),
            'item_fallback_eq_base_rows': int(np.isclose(npc[m], bf[m], rtol=1e-9).sum()),
            'median_unit_over_item_fb': float(np.nanmedian(cf[m] / npc[m])) if m.any() else None,
        }
        # 2. Where all of the purchases-only loss sits, by transition (kg).
        tr = {}
        for a in np.unique(bs):
            for b in np.unique(cs):
                mm = (bs == a) & (cs == b)
                if not mm.any():
                    continue
                d = np.nansum(Q[mm] * np.nan_to_num(cf[mm])) - np.nansum(Q[mm] * np.nan_to_num(bf[mm]))
                if a != b or abs(d) > 1:
                    tr[f'{a}->{b}'] = {'rows': int(mm.sum()), 'kg_delta': float(d)}
        r['kg_delta_by_transition'] = tr
        # 3. Rows the UNP rung serves: does the purchase u arm have a factor for the label?
        mu = cs == UNP
        r['unp'] = {'rows': int(mu.sum()),
                    'purchase_kg_unit_finite': int(np.isfinite(kc['kg_unit'].to_numpy(float)[mu]).sum()),
                    'np_item_candidate_finite': int(np.isfinite(npc[mu]).sum()),
                    'refused_by_item_fb_gate': None}
        # 4. Rows with a finite non-purchase candidate in a cell WITH purchases: must be none.
        r['np_candidate_in_purchase_cell'] = int((has_p & (np.isfinite(npc) | np.isfinite(unpc))).sum())
        res[name] = r
        print(name, json.dumps(r['unit_outranks_item_fallback']), flush=True)
    except Exception:  # noqa: BLE001
        res[name] = {'error': traceback.format_exc()[-1500:]}
with open(f'{ROOT}/analysis2.json', 'w') as f:
    json.dump(res, f, indent=1, default=str)
print('analysis2 written')
