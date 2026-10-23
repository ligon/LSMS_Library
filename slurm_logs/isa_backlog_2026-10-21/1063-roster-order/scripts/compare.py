"""Compare two pickled frames: order identity, first divergence position.

Reports positions and counts only (no ids).
Usage: python compare.py a.pkl b.pkl
"""
import sys

import numpy as np
import pandas as pd

a = pd.read_pickle(sys.argv[1])
b = pd.read_pickle(sys.argv[2])
print('shapes', a.shape, b.shape)
same_set = a.index.sort_values().equals(b.index.sort_values()) if a.index.is_unique else None
try:
    pd.testing.assert_frame_equal(a, b)
    print('ORDER-IDENTICAL: yes')
except AssertionError as e:
    print('ORDER-IDENTICAL: no')
    if a.shape == b.shape:
        ia = a.index.to_list()
        ib = b.index.to_list()
        diff = [k for k, (x, y) in enumerate(zip(ia, ib)) if x != y]
        print('n positions with differing index tuple:', len(diff))
        if diff:
            print('first differing position:', diff[0], 'last:', diff[-1])
            # which level differs at the first position
            x, y = ia[diff[0]], ib[diff[0]]
            print('levels differing at first pos:',
                  [n for n, p, q in zip(a.index.names, x, y) if p != q])
            # t at that position
            if 't' in a.index.names:
                ti = a.index.names.index('t')
                print('t at first pos (a,b):', x[ti], y[ti])
                ta = pd.Series(a.index.get_level_values('t'))
                tb = pd.Series(b.index.get_level_values('t'))
                print('t block order a:', ta[ta.ne(ta.shift())].tolist())
                print('t block order b:', tb[tb.ne(tb.shift())].tolist())
    try:
        pd.testing.assert_frame_equal(a.sort_index(), b.sort_index())
        print('SORTED-EQUAL: yes')
    except AssertionError as e2:
        print('SORTED-EQUAL: no', str(e2)[:300])
print('a monotonic index:', a.index.is_monotonic_increasing, 'b:', b.index.is_monotonic_increasing)
print('index unique a/b:', a.index.is_unique, b.index.is_unique, 'same set:', same_set)
