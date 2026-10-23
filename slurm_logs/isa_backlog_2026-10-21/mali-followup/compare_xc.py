"""Compare base vs branch cross-country results (from measure_cross_country.py).

Checks, per country and pooled: nitrogen_kg rows / totals / label tally
identical apart from the new `use_answer_rows` key; seed_kg rows / totals
identical; the pooled nitrogen_kg frames exactly equal; Feature row counts
equal; and reports the full-index groupby row counts and `j` NaN counts on
both sides.  Aggregate counts only.

Usage: python compare_xc.py <base_dir> <branch_dir> <out.json>
"""
import json
import sys

import pandas as pd

base_dir, branch_dir, out_path = sys.argv[1:4]
b = json.load(open(f'{base_dir}/summary.json'))
n = json.load(open(f'{branch_dir}/summary.json'))
res = {'countries_equal': b['countries'] == n['countries'],
       'feature_rows_equal': b['rows'] == n['rows'],
       'groupby_size_sum': {'base': b['groupby_size_sum'], 'branch': n['groupby_size_sum']},
       'j_nan': {'base': b['j_nan'], 'branch': n['j_nan']},
       'u_census_equal': b['u_census'] == n['u_census']}


def _strip(t):
    t = dict(t)
    return t, t.pop('use_answer_rows', 'ABSENT')


per = {}
for c in b['countries']:
    pb, pn = b['per_country'][c], n['per_country'][c]
    tb, ub = _strip(pb['nitrogen_tally'])
    tn, un = _strip(pn['nitrogen_tally'])
    per[c] = {'nitrogen_equal': (pb['nitrogen_rows'], pb['nitrogen_total'])
              == (pn['nitrogen_rows'], pn['nitrogen_total']),
              'seed_equal': (pb['seed_rows'], pb['seed_total'])
              == (pn['seed_rows'], pn['seed_total']),
              'tally_equal_except_new_key': tb == tn,
              'use_answer_rows': {'base': ub, 'branch': un}}
res['per_country'] = per
tb, ub = _strip(b['pooled_nitrogen']['tally'])
tn, un = _strip(n['pooled_nitrogen']['tally'])
res['pooled'] = {'rows_total_equal': (b['pooled_nitrogen']['rows'], b['pooled_nitrogen']['total'])
                 == (n['pooled_nitrogen']['rows'], n['pooled_nitrogen']['total']),
                 'tally_equal_except_new_key': tb == tn,
                 'use_answer_rows': {'base': ub, 'branch': un}}
pb = pd.read_pickle(f'{base_dir}/nitrogen_all.pkl')
pn = pd.read_pickle(f'{branch_dir}/nitrogen_all.pkl')
pd.testing.assert_frame_equal(pb, pn, check_exact=True)
res['pooled']['frames_exactly_equal'] = True
res['all_ok'] = (res['countries_equal'] and res['feature_rows_equal'] and res['u_census_equal']
                 and res['pooled']['rows_total_equal'] and res['pooled']['tally_equal_except_new_key']
                 and all(v['nitrogen_equal'] and v['seed_equal'] and v['tally_equal_except_new_key']
                         for v in per.values()))
with open(out_path, 'w') as fh:
    json.dump(res, fh, indent=1, sort_keys=True)
print(json.dumps(res, indent=1, sort_keys=True))
