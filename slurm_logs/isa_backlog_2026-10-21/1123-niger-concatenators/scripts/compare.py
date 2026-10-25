"""Compare base vs branch cold builds and hash censuses.  Aggregates only.

Usage: python compare.py <jobdir> <out.json>
"""
import json
import sys
from pathlib import Path

import pandas as pd

J = Path(sys.argv[1])
out = Path(sys.argv[2])
res = {'frames': {}, 'hash': {}}

for mode in ('default', 'make'):
    bj = J / 'out-base' / f'build-{mode}.json'
    rj = J / 'out-branch' / f'build-{mode}.json'
    if not (bj.exists() and rj.exists()):
        res['frames'][mode] = 'missing build json'
        continue
    b = json.loads(bj.read_text())['tables']
    r = json.loads(rj.read_text())['tables']
    per = {}
    for tbl in sorted(set(b) | set(r)):
        rec = {'base': {k: b.get(tbl, {}).get(k) for k in ('type', 'rows', 'len', 'error')},
               'branch': {k: r.get(tbl, {}).get(k) for k in ('type', 'rows', 'len', 'error')}}
        bp = J / 'private-base' / mode / f'{tbl}.parquet'
        rp = J / 'private-branch' / mode / f'{tbl}.parquet'
        if bp.exists() and rp.exists():
            fb, fr = pd.read_parquet(bp), pd.read_parquet(rp)
            try:
                pd.testing.assert_frame_equal(fb, fr, check_exact=True)
                rec['frame_equal'] = True
            except AssertionError as e:
                rec['frame_equal'] = False
                rec['frame_diff_head'] = str(e).splitlines()[0][:200]
                rec['rows_base_only'] = int((~fb.index.isin(fr.index)).sum())
                rec['rows_branch_only'] = int((~fr.index.isin(fb.index)).sum())
            ab = (J / 'private-base' / mode / f'{tbl}.attrs.json').read_text()
            ar = (J / 'private-branch' / mode / f'{tbl}.attrs.json').read_text()
            rec['attrs_equal'] = ab == ar
        else:
            bj2 = J / 'private-base' / mode / f'{tbl}.json'
            rj2 = J / 'private-branch' / mode / f'{tbl}.json'
            if bj2.exists() and rj2.exists():
                rec['dict_equal'] = bj2.read_text() == rj2.read_text()
        rec['rows_by_wave_equal'] = (b.get(tbl, {}).get('rows_by_wave')
                                     == r.get(tbl, {}).get('rows_by_wave'))
        rec['warning_categories_equal'] = (b.get(tbl, {}).get('warning_categories')
                                           == r.get(tbl, {}).get('warning_categories'))
        per[tbl] = rec
    res['frames'][mode] = per

# The concatenators' own output: var/<table>.parquet written by the make recipe.
res['var_parquets'] = {}
for tbl in ('plot_inputs', 'crop_production', 'livestock', 'plot_labor', 'people_last7days'):
    bp = J / 'data-base-make' / 'Niger' / 'var' / f'{tbl}.parquet'
    rp = J / 'data-branch-make' / 'Niger' / 'var' / f'{tbl}.parquet'
    if not (bp.exists() and rp.exists()):
        res['var_parquets'][tbl] = 'missing'
        continue
    fb, fr = pd.read_parquet(bp), pd.read_parquet(rp)
    try:
        pd.testing.assert_frame_equal(fb, fr, check_exact=True)
        eq = True
    except AssertionError:
        eq = False
    vc = fr.index.get_level_values('t').value_counts().sort_index()
    res['var_parquets'][tbl] = {'equal': eq, 'rows_base': int(len(fb)), 'rows_branch': int(len(fr)),
                                'rows_by_wave_branch': {str(k): int(v) for k, v in vc.items()}}

hb = J / 'out-base' / 'hash.json'
hr = J / 'out-branch' / 'hash.json'
if hb.exists() and hr.exists():
    b, r = json.loads(hb.read_text()), json.loads(hr.read_text())
    for kind in ('country', 'btf', 'wave'):
        keys = set(b[kind]) | set(r[kind])
        moved = sorted(k for k in keys if b[kind].get(k) != r[kind].get(k))
        countries = sorted({k.split('/')[0] for k in moved}) if kind != 'btf' else None
        res['hash'][kind] = {'total': len(keys), 'moved': len(moved),
                             'moved_countries': countries,
                             'moved_keys': moved if kind != 'wave' else None,
                             'niger_total': sum(k.startswith('Niger/') for k in keys)}
    res['hash']['errors_equal'] = b.get('errors') == r.get('errors')
    res['hash']['n_errors'] = len(r.get('errors') or {})

out.write_text(json.dumps(res, indent=1, sort_keys=True))
print(json.dumps(res['var_parquets'], indent=1))
print(json.dumps({m: ({t: (v.get('frame_equal', v.get('dict_equal')), v['base'].get('rows'),
                           v['branch'].get('rows'), v['base'].get('error') is None)
                       for t, v in per.items()} if isinstance(per, dict) else per)
                  for m, per in res['frames'].items()}, indent=1))
print(json.dumps({k: {kk: vv for kk, vv in v.items() if kk != 'moved_keys'}
                  if isinstance(v, dict) else v for k, v in res['hash'].items()}, indent=1))
