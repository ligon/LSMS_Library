"""Which attrs survive Feature() for one vs two countries.  usage: feature_check.py OUT.json"""
import os
import sys
import json
import warnings

sys.path.insert(0, os.path.dirname(__file__))
import guard  # noqa: F401,E402

import lsms_library as ll  # noqa: E402

warnings.simplefilter('ignore')
KEYS = ('kg_factor_sources', 'kg_factor_evidence', 'price_rows_dropped',
        'quantity_rows_dropped')
res = {}
for table, kw in (('food_quantities', {'units': 'kgs'}),
                  ('food_quantities', {'units': 'units'}),
                  ('food_prices', {'units': 'unitvalue'}),
                  ('food_prices', {})):
    for cs in (['Cambodia'], ['Cambodia', 'Serbia']):
        lab = f"{table}|{kw}|{'+'.join(cs)}"
        try:
            df = ll.Feature(table)(cs, **kw)
            res[lab] = {'rows': len(df),
                        'attrs_keys': sorted(df.attrs),
                        'carried': {k: (k in df.attrs) for k in KEYS}}
        except Exception as e:  # noqa: BLE001
            res[lab] = {'error': f'{type(e).__name__}: {e}'[:500]}
        print(lab, res[lab], flush=True)
with open(sys.argv[1], 'w') as f:
    json.dump(res, f, indent=1, default=str)
