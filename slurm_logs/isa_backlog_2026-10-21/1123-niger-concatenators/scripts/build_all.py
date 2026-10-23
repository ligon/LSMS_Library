"""Cold-build Niger tables in the current LSMS_DATA_DIR / backend and record
AGGREGATES to <out.json>; row-level frames go to <private_dir> only.

Usage: python build_all.py <checkout> <label> <out.json> <private_dir> [table,table,...]
With no table list, builds every entry of Country('Niger').data_scheme.
"""
import collections
import json
import os
import sys
import time
import warnings
from pathlib import Path

checkout, label, out_json, private = sys.argv[1:5]
only = [t for t in sys.argv[5].split(',') if t] if len(sys.argv) > 5 else []
private = Path(private)
private.mkdir(parents=True, exist_ok=True)

import pandas as pd  # noqa: E402
import lsms_library as ll  # noqa: E402

assert os.path.realpath(ll.__file__).startswith(os.path.realpath(checkout) + os.sep), ll.__file__

c = ll.Country('Niger')
tables = only or list(c.data_scheme)
res = {'label': label, 'backend': os.environ.get('LSMS_BUILD_BACKEND', 'default'),
       'tables': {}}
for tbl in tables:
    t0 = time.time()
    rec = {}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            obj = getattr(c, tbl)
            if callable(obj):
                obj = obj()
            if isinstance(obj, dict):
                rec['type'] = 'dict'
                rec['len'] = len(obj)
                (private / f'{tbl}.json').write_text(json.dumps(obj, sort_keys=True, default=str))
            else:
                df = obj
                rec['type'] = 'frame'
                rec['rows'] = int(len(df))
                rec['index'] = list(df.index.names)
                rec['columns'] = [str(x) for x in df.columns]
                if 't' in (df.index.names or []):
                    vc = df.index.get_level_values('t').value_counts().sort_index()
                    rec['rows_by_wave'] = {str(k): int(v) for k, v in vc.items()}
                (private / f'{tbl}.attrs.json').write_text(
                    json.dumps(df.attrs, sort_keys=True, default=str))
                flat = df.copy()
                flat.attrs = {}
                flat.to_parquet(private / f'{tbl}.parquet')
            rec['error'] = None
        except Exception as e:  # noqa: BLE001 -- recorded, compared base vs branch
            rec['error'] = f'{type(e).__name__}: {str(e)[:300]}'
    rec['seconds'] = round(time.time() - t0, 1)
    rec['warning_categories'] = dict(collections.Counter(w.category.__name__ for w in caught))
    rec['warning_heads'] = dict(collections.Counter(
        str(w.message)[:60] for w in caught
        if w.category.__name__ in ('UserWarning', 'RuntimeWarning')))
    res['tables'][tbl] = rec
    print(tbl, json.dumps({k: rec.get(k) for k in ('rows', 'len', 'error', 'seconds')}), flush=True)

Path(out_json).write_text(json.dumps(res, indent=1, sort_keys=True))
