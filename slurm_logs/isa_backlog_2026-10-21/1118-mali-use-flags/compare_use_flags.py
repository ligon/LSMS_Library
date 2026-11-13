"""Compare Mali plot_inputs built on base vs the use-flag candidate.

Inputs are pickles written by ../1118-mali-units/build_1118.py on each checkout.
Usage: python compare_use_flags.py <base.pkl> <cand.pkl> <out.json>  (PYTHONPATH=<candidate checkout>)
"""
import json, pickle, sys, warnings
import pandas as pd
from lsms_library import transformations as T
warnings.simplefilter('ignore')
b = pickle.load(open(sys.argv[1], 'rb')); c = pickle.load(open(sys.argv[2], 'rb'))
common = c.loc[c.index.isin(b.index)]; new = c.loc[~c.index.isin(b.index)]
pd.testing.assert_frame_equal(b.sort_index(), common.drop(columns='Applied').sort_index(), check_exact=True)
assert b.index.isin(c.index).all() and common['Applied'].isna().all() and c.index.is_unique
assert new['Quantity'].isna().all() and new['Purchased'].isna().all() and (new['u'] == 'Unknown').all()
gi = new.index.get_level_values('input'); t = new.index.get_level_values('t')
tab = new.groupby([t, gi])['Applied'].value_counts(dropna=False)
out = {'rows_base': len(b), 'rows_cand': len(c), 'new_rows': len(new),
       'new_by_wave_input_applied': {f'{k[0]}|{k[1]}|{k[2]}': int(v) for k, v in tab.items()}}
for fn in ('nitrogen_kg', 'seed_kg'):
    rb, rc = getattr(T, fn)(b), getattr(T, fn)(c); col = rb.columns[0]
    out[fn] = {'identical': bool(rb.sort_index().equals(rc.sort_index())),
               'total_base': float(rb[col].sum()), 'total_cand': float(rc[col].sum())}
tb, tc = T.nitrogen_kg(b).attrs['nitrogen_input_match'], T.nitrogen_kg(c).attrs['nitrogen_input_match']
out['nitrogen_tally'] = {k: [tb[k], tc[k]] for k in ('matched_rows', 'unmatched_rows')}
json.dump(out, open(sys.argv[3], 'w'), indent=1); print(json.dumps(out, indent=1))
