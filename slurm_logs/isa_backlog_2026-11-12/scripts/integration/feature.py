"""Feature('sample') and Feature('food_acquired') over the measured countries.

usage: feature.py OUT.json        (cwd = the checkout; warm private root)
Asserts nothing itself; records rows, index uniqueness, the countries present
in the `country` level and the warnings (a failing country is DROPPED with a
warning, so presence must be checked, not assumed).
"""
import json
import os
import subprocess
import sys
import time
import warnings

import lsms_library  # noqa: E402
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library import paths  # noqa: E402
_r = os.path.realpath(str(paths.data_root()))
assert '/private/integration/' in _r, _r
import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402

COUNTRIES = ['Malawi', 'Togo', 'Benin', 'CotedIvoire', 'Niger', 'Mali', 'Nigeria',
             'GhanaLSS', 'GhanaSPS', 'Uganda']
rec = {'sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
       'countries': COUNTRIES}
for table in ('sample', 'food_acquired'):
    t0 = time.time()
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter('always')
        try:
            df = ll.Feature(table)(COUNTRIES)
            err = None
        except Exception as exc:  # noqa: BLE001
            df, err = None, f'{type(exc).__name__}: {exc}'[:3000]
    r = {'seconds': round(time.time() - t0, 1), 'error': err,
         'warning_classes': sorted({x.category.__name__ for x in w}),
         'drop_or_feature_warnings': [f'{x.category.__name__}: {str(x.message)[:400]}' for x in w
                                      if 'drop' in str(x.message).lower()
                                      or 'Feature' in str(x.message)][:20]}
    if df is not None:
        got = sorted(set(map(str, df.index.get_level_values('country'))))
        r.update({'rows': len(df), 'index_unique': bool(df.index.is_unique),
                  'n_duplicated': int(df.index.duplicated().sum()),
                  'countries_present': got, 'missing': sorted(set(COUNTRIES) - set(got)),
                  'rows_by_country': {str(k): int(v) for k, v in
                                      pd.Series(df.index.get_level_values('country')).value_counts().sort_index().items()},
                  'index_names': list(df.index.names)})
        del df
    rec[table] = r
    print(table, json.dumps({k: v for k, v in r.items() if k != 'drop_or_feature_warnings'}), flush=True)
with open(sys.argv[1], 'w') as fh:
    json.dump(rec, fh, indent=1, sort_keys=True)
