"""GH #1240: panel linkage as the API serves it (aggregates only).

Usage: python api_linkage.py   (PYTHONPATH=<checkout>, fresh LSMS_DATA_DIR)
For each table, counts 2021-22 households whose SERVED id is also served for
2018-19 (naive), and how many of those are the RIGHT 2018-19 household per the
survey's own grappe_EHCVM1/menage_EHCVM1 (strict).  The raw household behind a
served 2021-22 id is recovered by inverting the checkout's own crosswalk.
Also compares household heads across linked pairs (sex; age gap 1-5 years),
split by link class, as a survey-independent check of the link.
"""
import json, os, sys, time, subprocess
from collections import Counter
import pandas as pd
import lsms_library
top = os.environ['CHECKOUT']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library import Country
from lsms_library.paths import countries_root
from lsms_library.local_tools import get_dataframe, _close_id_map
C = countries_root() / 'Burkina_Faso' / '_'
sys.path.insert(0, str(C))
import burkina_faso as bf

xw = json.load(open(C / 'updated_ids.json'))['2021-22']
closed = _close_id_map(xw)
inv = {v: k for k, v in closed.items()}
c21 = get_dataframe(str(C / '../2021-22/Data/s00_me_bfa2021.dta'))
truth = {str(int(r.hhid)): bf.ehcvm_i(int(r.grappe_EHCVM1), int(r.menage_EHCVM1))
         for r in c21.itertuples(index=False)}
width3 = {str(int(r.hhid)) for r in c21.itertuples(index=False) if int(r.menage_EHCVM1) >= 100}

def raw_of(served):
    return inv.get(served, served)

out = {'checkout': top}
c = Country('Burkina_Faso')
for table in ('sample', 'household_roster'):
    t0 = time.time()
    df = getattr(c, table)()
    ids = df.reset_index()[['t', 'i']].drop_duplicates()
    s18 = set(ids.loc[ids.t == '2018-19', 'i']); s21 = set(ids.loc[ids.t == '2021-22', 'i'])
    linked = s21 & s18
    strict = {s for s in linked if truth.get(raw_of(s)) == s}
    out[table] = dict(seconds=round(time.time() - t0, 1),
                      hh_2018_19=len(s18), hh_2021_22=len(s21),
                      naive_linked=len(linked), strict_linked=len(strict),
                      linked_to_a_different_household=len(linked - strict),
                      served_2021_22_ids_unmapped_and_unlinked=len(s21 - s18),
                      strict_linked_with_3digit_2018_menage=sum(raw_of(s) in width3 for s in strict))
    if table == 'household_roster':
        r = df.reset_index()
        head = r[r.Generation.eq(0).fillna(False).astype(bool) & r.Distance.eq(0).fillna(False).astype(bool) & r.Affinity.astype(object).eq('consanguineal').fillna(False).astype(bool)]
        nh = head.groupby(['t', 'i']).size()
        head = head.set_index(['t', 'i'])
        one = nh[nh == 1].index
        head = head.loc[head.index.isin(one)]
        h18 = head.xs('2018-19', level='t'); h21 = head.xs('2021-22', level='t')
        cls = {}
        for s in linked:
            k = ('strict_3digit' if raw_of(s) in width3 else 'strict_2digit') if s in strict else 'different_household'
            cls.setdefault(k, []).append(s)
        heads = {}
        for k, ss in cls.items():
            ss = [s for s in ss if s in h18.index and s in h21.index]
            a, b = h18.loc[ss], h21.loc[ss]
            gap = (b['Age'].astype(float) - a['Age'].astype(float))
            heads[k] = dict(pairs_with_one_head_each=len(ss),
                            same_head_sex=round(float((a['Sex'].astype(str).to_numpy() == b['Sex'].astype(str).to_numpy()).mean()), 3) if ss else None,
                            head_age_gap_1_to_5=round(float(gap.between(1, 5).mean()), 3) if ss else None)
        out['household_roster']['head_consistency'] = heads
print(json.dumps(out, indent=1))
