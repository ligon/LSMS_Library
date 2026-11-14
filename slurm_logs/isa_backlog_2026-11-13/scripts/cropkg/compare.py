"""Base vs candidate cold frames, GH #1559: the screen must change no served value.

Usage: compare.py BASE_FRAMES CAND_FRAMES OUT_JSON.  Aggregates only.
"""
import json
import sys
from pathlib import Path

import pandas as pd

base, cand, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
res = {}
for country in ('Niger', 'Mali'):
    a = pd.read_pickle(base / f'{country}_crop_production.pkl')
    b = pd.read_pickle(cand / f'{country}_crop_production.pkl')
    r = {'rows_base': int(len(a)), 'rows_cand': int(len(b))}
    try:
        pd.testing.assert_frame_equal(a, b, check_exact=True)
        r['frame_equal'] = True
    except AssertionError as e:
        r['frame_equal'] = False
        r['diff'] = str(e)[:500]
    r['attrs_equal'] = (repr(sorted(a.attrs.items(), key=str)) == repr(sorted(b.attrs.items(), key=str)))
    r['attrs_differ'] = sorted(k for k in set(a.attrs) | set(b.attrs) if repr(a.attrs.get(k)) != repr(b.attrs.get(k)))
    res[country] = r
    print(country, json.dumps(r)[:400], flush=True)
for label in ('base', 'cand'):
    pass
out.write_text(json.dumps(res, indent=1))
