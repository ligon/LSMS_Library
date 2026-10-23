"""Design-B crosswalk ids vs served ids (development build).  usage: measure_targets.py DUMPDIR SERVEDDIR"""
import json, sys, pandas as pd
dump, served = sys.argv[1], sys.argv[2]
for C in ['Malawi', 'Nigeria']:
    r = pd.read_parquet(f'{served}/{C}_household_roster.parquet').index.to_frame(index=False)
    s = pd.read_parquet(f'{served}/{C}_sample.parquet').index.to_frame(index=False)
    ros = {t: set(g['i']) for t, g in r.groupby('t')}
    sam = {t: set(g['i']) for t, g in s.groupby('t')}
    p = json.load(open(f'{dump}/{C}_panel_ids.json'))
    u = json.load(open(f'{dump}/{C}_updated_ids.json'))
    print('=====', C)
    for t, m in u.items():
        canon = set(m.values()); vals = list(m.values())
        print(f' updated_ids[{t}]: n={len(m)} identity={sum(k == v for k, v in m.items())} '
              f'suffixed={sum("_" in v for v in vals)} many_to_one={len(vals) - len(canon)} '
              f'chains={sum(v in m and m[v] != v for v in vals)} | canonical ids not in roster={len(canon - ros.get(t, set()))} '
              f'not in sample={len(canon - sam.get(t, set()))} | roster ids in wave={len(ros.get(t, set()))}')
    by = {}
    for k, v in p.items():
        ct, ci = k.split(',', 1); pt, pi = v.split(',', 1)
        cc = u.get(ct, {}).get(ci, ci); pc = u.get(pt, {}).get(pi, pi)
        b = by.setdefault(f'{ct}->{pt}', dict(n=0, cur_not_in_roster=0, prev_not_in_roster=0, prev_not_in_sample=0))
        b['n'] += 1
        b['cur_not_in_roster'] += cc not in ros.get(ct, set())
        b['prev_not_in_roster'] += pc not in ros.get(pt, set())
        b['prev_not_in_sample'] += pc not in sam.get(pt, set())
    for k, b in by.items():
        print(f' panel_ids {k}: {b}')
