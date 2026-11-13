"""Which key ordering does each frame follow?  Prints booleans only, no ids."""
import sys

import pandas as pd

for p in sys.argv[1:]:
    df = pd.read_pickle(p)
    idx = df.index.to_frame(index=False)
    print('==', p.split('/')[-1], 'names', list(df.index.names), 'dtypes', dict(idx.dtypes.astype(str)))
    cands = {}
    names = list(idx.columns)
    import itertools
    for r in range(1, len(names) + 1):
        for combo in itertools.permutations(names, r):
            s = idx[list(combo)]
            # sorted (lexicographic as stored dtype)?
            ok = s.apply(tuple, axis=1).is_monotonic_increasing if len(combo) > 1 else s[combo[0]].is_monotonic_increasing
            if ok:
                cands[combo] = True
    print('monotone under orderings:', list(cands)[:10])
    # group contiguity: are households contiguous?
    i = idx['i']
    runs = (i != i.shift()).sum()
    print('household runs', runs, 'distinct households', i.nunique())
    # is pid ascending within household (as numeric)?
    pid_num = pd.to_numeric(idx['pid'], errors='coerce')
    within = (pid_num.groupby(i.values).apply(lambda s: s.is_monotonic_increasing)).mean()
    print('share households with pid ascending (numeric):', round(float(within), 4))
    # v as numeric ascending?
    vn = pd.to_numeric(idx['v'], errors='coerce')
    print('v numeric monotone:', vn.is_monotonic_increasing, 'v str monotone:', idx['v'].astype(str).is_monotonic_increasing,
          'i str monotone:', idx['i'].astype(str).is_monotonic_increasing)
