"""GH #637: Feature('plot_inputs' / 'crop_production')(['Tanzania']) shape and
row count, and warm-read == cold-read for both Country tables (a fresh process
reading the cache the cold snapshot wrote).  Aggregates only.

Usage: python feature_check.py <label> <private_dir> <cold_label> <out_json>
"""
import json
import os
import subprocess
import sys
import warnings
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

import lsms_library as ll

label, priv, cold, out = sys.argv[1], Path(sys.argv[2]), sys.argv[3], Path(sys.argv[4])
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(ll.__file__).startswith(os.path.realpath(top) + os.sep), ll.__file__
res = {'label': label}
c = ll.Country('Tanzania')
for t in ['plot_inputs', 'crop_production']:
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        warm = getattr(c, t)()
    cold_df = pd.read_pickle(priv / f'{cold}_{t}.pkl')
    try:
        assert_frame_equal(warm.sort_index(), cold_df.sort_index())
        res[f'{t}_warm_equals_cold'] = True
    except AssertionError as e:
        res[f'{t}_warm_equals_cold'] = str(e)[:300]
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        f = ll.Feature(t)(['Tanzania'])
    res[f'Feature_{t}'] = {
        'rows': len(f), 'index_names': list(f.index.names),
        'index_unique': bool(f.index.is_unique),
        'promoted': json.loads(json.dumps(f.attrs.get('canonical_alignment', {}).get('promoted', {}), default=str)),
        'u_values': sorted(map(str, pd.Series(f.index.get_level_values('u')).astype('string').fillna('<NA>').unique())),
        'country_rows': len(warm)}
out.write_text(json.dumps(res, indent=1) + '\n')
print(json.dumps(res, indent=1))
