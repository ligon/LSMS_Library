"""GH #637 round 3: warm == cold for Tanzania crop_production / plot_inputs (a
fresh process reading the cache snapshot.py wrote), and the cross-country
assembly Feature(t)(['Tanzania', 'Malawi']).  Aggregates only.

Usage: python feature_check.py <private_dir> <cold_label> <out_json>
"""
import json
import os
import subprocess
import sys
import warnings
from collections import Counter
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

import lsms_library as ll

priv, cold, out = Path(sys.argv[1]), sys.argv[2], Path(sys.argv[3])
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
res = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()}
c = ll.Country('Tanzania')
for t in ['plot_inputs', 'crop_production']:
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        warm = getattr(c, t)()
    try:
        assert_frame_equal(warm.sort_index(), pd.read_pickle(priv / f'{cold}_{t}.pkl').sort_index())
        res[f'{t}_warm_equals_cold'] = True
    except AssertionError as e:
        res[f'{t}_warm_equals_cold'] = str(e)[:300]
    with warnings.catch_warnings(record=True) as ws:
        warnings.simplefilter('always')
        f = ll.Feature(t)(['Tanzania', 'Malawi'])
    ent = {'rows': len(f), 'index_names': list(f.index.names), 'index_unique': bool(f.index.is_unique),
           'rows_by_country': {str(k): int(v) for k, v in f.groupby(level='country').size().items()},
           'columns': list(f.columns),
           'warnings': dict(Counter(type(w.message).__name__ for w in ws)),
           'canonical_alignment': json.loads(json.dumps(f.attrs.get('canonical_alignment', {}), default=str))}
    if 'production_obs' in f.index.names:
        po = pd.Series(f.index.get_level_values('production_obs'), index=f.index)
        ent['production_obs'] = {
            'Malawi_values': sorted(map(str, po.xs('Malawi', level='country').unique())),
            'Tanzania_reported_prefix': bool(po.xs('Tanzania', level='country').astype(str).str.match(r'^reported:(4A|6A|6B):\d+$').all())}
    if 'Unit_purchased' in f.columns:
        ent['Unit_purchased_nonnull_by_country'] = {str(k): int(v) for k, v in
                                                    f['Unit_purchased'].notna().groupby(level='country').sum().items()}
    res[f'Feature_{t}'] = ent
out.write_text(json.dumps(res, indent=1) + '\n')
print(json.dumps(res, indent=1))
