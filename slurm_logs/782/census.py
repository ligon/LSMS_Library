#!/usr/bin/env python
"""Collision census on GhanaLSS food_acquired.j (GH #782).

The collision key is casefold + punctuation-to-space + singularise EVERY word.
This exact key reproduces the census published in #782 on `development`
@ 9e12753f: 35 groups / 1,468,238 rows / 27.9% of 5,259,344.
"""
import re
import sys

import pandas as pd


def key(lbl):
    s = re.sub(r'[^a-z0-9]+', ' ', str(lbl).casefold()).strip()
    return ' '.join(w[:-1] if len(w) > 3 and w.endswith('s') and not w.endswith('ss')
                    else w for w in s.split())


def census(path, label=''):
    fa = pd.read_parquet(path)
    j = pd.Series(fa.index.get_level_values('j').astype(str))
    t = pd.Series(fa.index.get_level_values('t').astype(str))
    df = pd.DataFrame({'t': t, 'j': j})

    print(f'===== {label or path} =====')
    print(f'total rows : {len(fa):,}')
    print(f'distinct j : {df["j"].nunique()}')

    print('\n-- per wave --')
    for w, g in df.groupby('t'):
        low = g['j'].apply(lambda s: bool(s) and s[0].islower())
        print(f'  {w}: rows={len(g):>9,}  distinct_j={g["j"].nunique():>4}  '
              f'lowercase_rows={int(low.sum()):>7,}  '
              f'distinct_lowercase={g.loc[low, "j"].nunique():>3}')

    lab = pd.Series(sorted(df['j'].unique()))
    grp = lab.groupby(lab.map(key)).apply(list)
    coll = {k: v for k, v in grp.items() if len(v) > 1}
    rows_by_j = df['j'].value_counts()
    nrows = sum(rows_by_j.get(x, 0) for v in coll.values() for x in v)
    print(f'\n-- collision groups: {len(coll)}   rows involved: {nrows:,} '
          f'({100 * nrows / len(fa):.1f}%) --')
    for k in sorted(coll):
        print('   ', coll[k])
    return len(fa), len(coll), nrows


if __name__ == '__main__':
    census(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else '')
