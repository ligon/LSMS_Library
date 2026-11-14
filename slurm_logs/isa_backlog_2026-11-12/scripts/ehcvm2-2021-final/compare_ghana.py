"""Compare GhanaLSS base vs candidate frames and attrs, table by table.

usage: python -I compare_ghana.py BASE_DIR CAND_DIR OUT_JSON
"""
import json, sys
from pathlib import Path
import pandas as pd

B, C = Path(sys.argv[1]), Path(sys.argv[2])
mb, mc = json.load(open(B / 'meta.json')), json.load(open(C / 'meta.json'))
out = {'size_words_added': sorted(set(mc['size_words']) - set(mb['size_words'])),
       'size_words_removed': sorted(set(mb['size_words']) - set(mc['size_words'])), 'tables': {}}
for t in ('food_acquired', 'food_quantities', 'food_prices', 'nutrition'):
    r = {'base': {k: v for k, v in mb['tables'][t].items() if k != 'traceback'},
         'cand': {k: v for k, v in mc['tables'][t].items() if k != 'traceback'}}
    fb, fc = B / f'{t}.parquet', C / f'{t}.parquet'
    if fb.exists() and fc.exists():
        a, b = pd.read_parquet(fb), pd.read_parquet(fc)
        idx = [x for x in mb['tables'][t]['index'] if x is not None]
        r['same_columns'] = list(a.columns) == list(b.columns)
        r['same_dtypes'] = a.dtypes.astype(str).to_dict() == b.dtypes.astype(str).to_dict()
        try:
            pd.testing.assert_frame_equal(a, b, check_exact=True)
            r['frame_equal_exact_same_order'] = True
        except AssertionError as exc:
            r['frame_equal_exact_same_order'] = False
            r['diff'] = str(exc)[:1500]
            try:
                a2 = a.sort_values(idx).reset_index(drop=True)
                b2 = b.sort_values(idx).reset_index(drop=True)
                pd.testing.assert_frame_equal(a2, b2, check_exact=True)
                r['frame_equal_exact_after_sort'] = True
            except AssertionError as exc2:
                r['frame_equal_exact_after_sort'] = False
                r['diff_sorted'] = str(exc2)[:1500]
                # Which rows differ
                if idx and len(a) == len(b):
                    m = a2.merge(b2, on=idx, how='outer', suffixes=('_b', '_c'), indicator=True)
                    r['merge_indicator'] = m['_merge'].value_counts().to_dict()
                    diffs = {}
                    for col in a.columns:
                        if col in idx:
                            continue
                        x, y = m.get(f'{col}_b'), m.get(f'{col}_c')
                        if x is None:
                            continue
                        ne = ~((x == y) | (x.isna() & y.isna()))
                        diffs[col] = int(ne.fillna(True).sum())
                    r['differing_cells_by_column'] = diffs
        ab = json.load(open(B / f'{t}.attrs.json')) if (B / f'{t}.attrs.json').exists() else None
        ac = json.load(open(C / f'{t}.attrs.json')) if (C / f'{t}.attrs.json').exists() else None
        r['attrs_equal'] = ab == ac
        if ab != ac:
            r['attrs_keys_differing'] = sorted(k for k in set(ab or {}) | set(ac or {})
                                               if (ab or {}).get(k) != (ac or {}).get(k))
            r['attrs_diff'] = {k: {'base': (ab or {}).get(k), 'cand': (ac or {}).get(k)}
                               for k in r['attrs_keys_differing']}
    out['tables'][t] = r
json.dump(out, open(sys.argv[3], 'w'), indent=1, default=str)
for t, r in out['tables'].items():
    print(t, r['base'].get('rows'), r['cand'].get('rows'), 'equal', r.get('frame_equal_exact_same_order'),
          r.get('frame_equal_exact_after_sort'), 'attrs', r.get('attrs_equal'),
          'tres grand rows', r['cand'].get('u_tres_grand_rows'))
