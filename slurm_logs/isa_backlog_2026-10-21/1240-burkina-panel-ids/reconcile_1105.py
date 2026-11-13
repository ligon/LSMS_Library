"""GH #1240: reconcile the parallel lane's 1,105 with the issue's 1,088 (aggregates only).

Usage: python reconcile_1105.py <base_checkout>   (PYTHONPATH=<any checkout>)
"""
import json, os, sys
from collections import Counter
from pathlib import Path
base = Path(sys.argv[1]); rel = Path('lsms_library/countries/Burkina_Faso/_')
sys.path.insert(0, str(base / rel)); os.chdir(base / rel)
import burkina_faso as bf
from lsms_library.local_tools import get_dataframe
c18 = get_dataframe('../2018-19/Data/s00_me_bfa2018.dta')
c21 = get_dataframe('../2021-22/Data/s00_me_bfa2021.dta')
id18 = {bf.ehcvm_i(int(g), int(m)): (int(g), int(m)) for g, m in c18[['grappe', 'menage']].dropna().itertuples(index=False)}
old = json.load(open('updated_ids.json'))['2021-22']
cls = Counter()
for r in c21.itertuples(index=False):
    src = str(int(r.hhid)); truth = (int(r.grappe_EHCVM1), int(r.menage_EHCVM1))
    pre = str(truth[0]) + str(truth[1]).rjust(3, '0')        # old formula, before the chain guard
    pre_cls = 'served_right' if id18.get(pre) == truth else ('served_WRONG_household' if pre in id18 else 'not_served')
    post_cls = 'chain_skipped' if src not in old else pre_cls
    cls[(pre_cls, post_cls)] += 1
for k, v in sorted(cls.items()): print('old formula:', k[0], '| in committed JSON:', k[1], '=', v)
print('lost before chain guard (old target not a served id):', sum(v for k, v in cls.items() if k[0] == 'not_served'))
print('of which in committed JSON (issue count):', cls[('not_served', 'not_served')], '; chain-skipped:', cls[('not_served', 'chain_skipped')])
