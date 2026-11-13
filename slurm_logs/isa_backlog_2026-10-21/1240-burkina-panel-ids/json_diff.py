"""GH #1240: aggregate diff + identity checks of Burkina's committed crosswalk.

Usage: python json_diff.py <base_checkout> <branch_checkout>
Run with PYTHONPATH=<branch_checkout>.  Prints aggregates only (no ids beyond
the three wrong-household examples already quoted in the issue thread).
"""
import json, os, sys
from collections import Counter
from pathlib import Path
import pandas as pd

base, branch = map(Path, sys.argv[1:3])
rel = Path('lsms_library/countries/Burkina_Faso/_')
C = branch / rel
sys.path.insert(0, str(C)); os.chdir(C)
import burkina_faso as bf
from lsms_library.local_tools import get_dataframe, _close_id_map

def load(root):
    u = json.load(open(root / rel / 'updated_ids.json'))
    p = json.load(open(root / rel / 'panel_ids.json'))
    return u, p

ub, pb = load(base); un, pn = load(branch)
out = {}
for name, (b, n) in {'updated_ids[2021-22]': (ub['2021-22'], un['2021-22']),
                     'panel_ids': (pb, pn)}.items():
    kb, kn = set(b), set(n)
    common = kb & kn
    out[name] = dict(base_entries=len(b), branch_entries=len(n),
                     added=len(kn - kb), removed=len(kb - kn),
                     common=len(common),
                     common_target_changed=sum(b[k] != n[k] for k in common))
out['updated_ids other waves'] = {w: (len(ub.get(w, {})), len(un.get(w, {})))
                                  for w in sorted(set(ub) | set(un)) if w != '2021-22'}
# panel_ids <-> updated_ids agree on the branch
pn_as_map = {k.split(',')[1]: v.split(',')[1] for k, v in pn.items()}
out['branch panel_ids == updated_ids[2021-22]'] = pn_as_map == un['2021-22']
out['branch panel_ids prev_wave values'] = dict(Counter(v.split(',')[0] for v in pn.values()))
out['branch panel_ids cur_wave keys'] = dict(Counter(k.split(',')[0] for k in pn))

# Raw covers: identity checks
c18 = get_dataframe('../2018-19/Data/s00_me_bfa2018.dta')
c21 = get_dataframe('../2021-22/Data/s00_me_bfa2021.dta')
id18 = {}
for g, m in c18[['grappe', 'menage']].dropna().drop_duplicates().itertuples(index=False):
    id18.setdefault(bf.ehcvm_i(int(g), int(m)), set()).add((int(g), int(m)))
out['2018-19 cover: households, ehcvm_i injective'] = (len(id18), all(len(v) == 1 for v in id18.values()))
cur = {}
for r in c21[['hhid', 'grappe', 'menage', 'grappe_EHCVM1', 'menage_EHCVM1', 'hhid_EHCVM1']].itertuples(index=False):
    cur[str(int(r.hhid))] = r
out['2021-22 cover households'] = len(cur)
for label, m in (('base', ub['2021-22']), ('branch', un['2021-22'])):
    srcs_raw = sum(s in cur for s in m)
    tg_in = sum(t in id18 for t in m.values())
    right = wrong = 0
    for s, t in m.items():
        r = cur[s]
        truth = (int(r.grappe_EHCVM1), int(r.menage_EHCVM1))
        if t in id18:
            if id18[t] == {truth}: right += 1
            else: wrong += 1
    tc = Counter(m.values())
    out[f'{label}: checks'] = dict(
        sources_are_raw_2021_hhid=f'{srcs_raw}/{len(m)}',
        targets_are_2018_ehcvm_ids=f'{tg_in}/{len(m)}',
        target_is_the_SAME_household=right, target_is_a_DIFFERENT_household=wrong,
        many_to_one_targets=sum(v - 1 for v in tc.values() if v > 1),
        targets_that_are_also_keys=sum(t in m and m[t] != t for t in m.values()),
        target_eq_survey_hhid_EHCVM1=sum(t == str(int(cur[s].hhid_EHCVM1)) for s, t in m.items()),
    )
    # simulated walk over all 2021-22 households
    closed = _close_id_map(m)
    served = {s: closed.get(s, s) for s in cur}
    cls = Counter()
    for s, v in served.items():
        truth = bf.ehcvm_i(int(cur[s].grappe_EHCVM1), int(cur[s].menage_EHCVM1))
        cls['correct' if v == truth else ('other_2018_household' if v in id18 else 'not_a_2018_id')] += 1
    cls['served_duplicates'] = sum(v - 1 for v in Counter(served.values()).values() if v > 1)
    out[f'{label}: simulated id_walk over 2021-22 cover'] = dict(cls)
# Breakdown of changed / added / removed by 2018-19 menage width
b, n = ub['2021-22'], un['2021-22']
def width(s): return 3 if int(cur[s].menage_EHCVM1) >= 100 else 2
out['branch-vs-base by 2018-19 menage width'] = {
    'changed': dict(Counter(width(s) for s in set(b) & set(n) if b[s] != n[s])),
    'added': dict(Counter(width(s) for s in set(n) - set(b))),
    'removed': dict(Counter(width(s) for s in set(b) - set(n))),
    'menage_EHCVM1>=100 households': sum(int(r.menage_EHCVM1) >= 100 for r in cur.values()),
}
# The 1,108 three-digit households under the base map
three = [s for s in cur if int(cur[s].menage_EHCVM1) >= 100]
bc = Counter()
for s in three:
    if s not in b: bc['chain_skipped'] += 1
    elif b[s] in id18: bc['target_is_other_household'] += 1
    else: bc['target_in_no_2018_table'] += 1
out['base: 3-digit-menage households'] = dict(bc)
print(json.dumps(out, indent=1, default=str))
