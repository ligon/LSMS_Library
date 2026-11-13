"""Simulate single-pass vs closure on Burkina 2021-22 with the guard-free map.
Universe = the 2021-22 cover (every 2021-22 household, i = hhid form).
Truth = the cover's grappe_EHCVM1/menage_EHCVM1 rendered with ehcvm_i."""
import json, sys, os, warnings
warnings.simplefilter('ignore')
from collections import Counter
import pandas as pd
sys.path.insert(0, os.getcwd())
from lsms_library.local_tools import get_dataframe
from burkina_faso import ehcvm_i, i as country_i


def _close_id_map(mapping, wave=None):
    """Copy of the closure removed by GH #1245 (local_tools at d353b7ea7),
    kept here so this script runs against either checkout."""
    closed = {}
    for k in mapping:
        seen = [k]
        v = mapping[k]
        while v in mapping and mapping[v] != v:
            if v in seen:
                raise ValueError(f"Cycle detected in updated_ids for wave {wave!r}")
            seen.append(v)
            v = mapping[v]
        closed[k] = v
    return closed

scr = sys.argv[1]
cover18 = get_dataframe('../2018-19/Data/s00_me_bfa2018.dta')
base = {ehcvm_i(int(g), int(m)) for g, m in cover18[['grappe','menage']].dropna().drop_duplicates().itertuples(index=False)}
c21 = get_dataframe('../2021-22/Data/s00_me_bfa2021.dta')
cur = [country_i(pd.Series([int(g), int(m)])) for g, m in c21[['grappe','menage']].itertuples(index=False)]
truth = {}
for h, (g, m) in zip(cur, c21[['grappe_EHCVM1','menage_EHCVM1']].itertuples(index=False)):
    truth[h] = None if pd.isna(g) or pd.isna(m) else ehcvm_i(int(g), int(m))
res = {}
for label, path in [('committed_with_guard', 'updated_ids.json'), ('no_guard', f'{scr}/updated_ids.json')]:
    m = json.load(open(path))['2021-22']
    for sem, f in [('closure', _close_id_map(m)), ('single_pass', m)]:
        served = {h: f.get(h, h) for h in cur}
        dup = sum(n - 1 for n in Counter(served.values()).values() if n > 1)
        merged = sum(n for n in Counter(served.values()).values() if n > 1)
        right = sum(served[h] == truth[h] for h in cur)
        wrong = sum(served[h] in base and served[h] != truth[h] for h in cur)
        unlinked = sum(served[h] not in base for h in cur)
        chains = sum(1 for k, v in m.items() if v in m and m[v] != v and k != v)
        r = dict(entries=len(m), chain_keys=chains, households=len(cur), linked_correctly=right,
                 wrong_household=wrong, unlinked=unlinked, duplicate_ids=dup, households_in_dup_groups=merged)
        res[f'{label}/{sem}'] = r
        print(f'{label:22s} {sem:12s}', r)
json.dump(res, open(f'{scr}/simulate.json', 'w'), indent=1)
