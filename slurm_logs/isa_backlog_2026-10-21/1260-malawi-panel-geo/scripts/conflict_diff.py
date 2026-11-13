import sys
import pandas as pd
cols = ['Region', 'District', 'Rural', 'Latitude', 'Longitude']
def conflicted(p):
    f = pd.read_parquet(p).reset_index()
    return {v for v, x in f.groupby(f.v.astype(str)) if len(x[cols].astype(str).drop_duplicates()) > 1}, f
for w in ['2016-17', '2019-20']:
    (c1, f1), (c2, f2), (c3, f3) = (conflicted(f'{d}/wave_{w}.parquet') for d in sys.argv[1:4])
    print(w, 'conflicting groups r1', len(c1), 'r2', len(c2), 'r3', len(c3), '| in r2 not r3', len(c2 - c3), '| in r3 not r2', len(c3 - c2), '| in r3 not r1', len(c3 - c1), '| in r1 not r3', len(c1 - c3))
    for v in sorted(c2 - c3):
        x = f1[f1.v.astype(str) == v]
        print('   EA conflicting only in r2: households', len(x), '| r1 distinct rows', len(x[cols].astype(str).drop_duplicates()))
