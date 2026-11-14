"""Base vs candidate frames and warnings.  Usage: rt_compare.py FR_BASE FR_CAND BJSON CJSON OUT"""
import json
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

fb, fc, bj, cj, out = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3], sys.argv[4], Path(sys.argv[5])
B, C = json.load(open(bj)), json.load(open(cj))
R = {'base': B['head'], 'cand': C['head']}
for country in [k for k in C if k not in ('label', 'head', 'has_screen')]:
    a = pd.read_pickle(fb / f'{country}_crop_production.pkl')
    b = pd.read_pickle(fc / f'{country}_crop_production.pkl')
    rec = {'rows_base': len(a), 'rows_cand': len(b), 'digest_equal': B[country]['digest'] == C[country]['digest']}
    try:
        pd.testing.assert_frame_equal(a, b, check_exact=True)
        rec['assert_frame_equal'] = True
    except AssertionError as e:
        rec['assert_frame_equal'] = str(e)[:400]
    rec['attrs_equal'] = (a.attrs == b.attrs)
    if not rec['attrs_equal']:
        rec['attrs_keys_diff'] = sorted(set(a.attrs) ^ set(b.attrs)) or [k for k in a.attrs if a.attrs[k] != b.attrs.get(k)]
    wb, wc = Counter(B[country]['warnings']), Counter(C[country]['warnings'])
    rec['warnings_only_cand'] = sorted((wc - wb).elements())
    rec['warnings_only_base'] = sorted((wb - wc).elements())
    rec['reports_base'] = B[country]['reports']
    rec['reports_cand'] = C[country]['reports']
    rec['strict_base'] = B[country]['strict']
    rec['strict_cand'] = C[country]['strict']
    rec['direct_check_cand'] = C[country]['direct_check']
    R[country] = rec
out.write_text(json.dumps(R, indent=1, default=str))
print(json.dumps(R, indent=1, default=str)[:6000])
