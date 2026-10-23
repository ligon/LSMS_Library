"""Aggregate order-identity results over the batch builds (positions/counts only).

Usage: python summarize.py <out_dir> <pairs.txt> <result.json>
pairs.txt lines: label a_name b_name   (names without .pkl)
"""
import json
import sys
import warnings

import pandas as pd

warnings.simplefilter('ignore')
out_dir, pairs, res_path = sys.argv[1:4]
res = []
for line in open(pairs):
    if not line.strip():
        continue
    label, an, bn = line.split()
    try:
        a = pd.read_pickle(f'{out_dir}/{an}.pkl')
        b = pd.read_pickle(f'{out_dir}/{bn}.pkl')
    except FileNotFoundError as e:
        res.append({'label': label, 'a': an, 'b': bn, 'error': f'missing {e.filename.split("/")[-1]}'})
        continue
    r = {'label': label, 'a': an, 'b': bn, 'rows_a': len(a), 'rows_b': len(b),
         'index_order_identical': bool(a.index.equals(b.index)
                                       and list(a.index.names) == list(b.index.names))}
    try:
        pd.testing.assert_frame_equal(a, b)
        r['order_identical'] = True
    except AssertionError as e:
        r['order_identical'] = False
        r['order_diff'] = str(e).split('\n')[0][:200]
    try:
        pd.testing.assert_frame_equal(a.sort_index(), b.sort_index())
        r['sorted_equal'] = True
    except AssertionError as e:
        r['sorted_equal'] = False
        r['sorted_diff'] = str(e).split('\n')[0][:200]
    if len(a) == len(b) and not r['order_identical']:
        ia, ib = a.index.to_list(), b.index.to_list()
        diff = [k for k, (x, y) in enumerate(zip(ia, ib)) if x != y]
        r['positions_differing'] = len(diff)
        if diff:
            r['first_diff_pos'] = diff[0]
            if 't' in a.index.names:
                r['first_diff_t'] = str(a.index.get_level_values('t')[diff[0]])
                tl = a.index.get_level_values('t')
                r['waves_with_diffs'] = sorted({str(tl[k]) for k in diff})
    res.append(r)
json.dump(res, open(res_path, 'w'), indent=1)
for r in res:
    print({k: v for k, v in r.items() if k not in ('a', 'b')})
