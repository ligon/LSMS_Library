"""Red team (malawi delta): is the #1712 drop conditional, as claimed?  Synthetic H frames only.

usage: python probe_1712.py <clone>
"""
import importlib.util
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

clone = Path(sys.argv[1])
spec = importlib.util.spec_from_file_location('t1190', clone / 'tests/test_gh1190_selected_farm_carrier.py')
t = importlib.util.module_from_spec(spec)
sys.path.insert(0, str(clone / 'tests'))
spec.loader.exec_module(t)
spec2 = importlib.util.spec_from_file_location('mw_probe', clone / 'lsms_library/countries/Malawi/_/malawi.py')
mw = importlib.util.module_from_spec(spec2)
spec2.loader.exec_module(mw)

cases = {
    'all NA': [np.nan] * 9,
    'all blank strings': [''] * 9,
    'one value on a carried row': ['BICYCLE'] + [np.nan] * 8,
    'one value, all others blank': [''] * 8 + ['CAR'],
}
with warnings.catch_warnings():
    warnings.simplefilter('ignore')
    for col in ('ag_h17_os', 'ag_h33'):
        for name, vals in cases.items():
            h = t.hframe(9)
            h[col] = np.array(vals, dtype=object)   # positional: hframe's index is [44]*n
            out = t.components(mw, h)
            served = col in out.columns
            nonblank = int(out[col].astype('string').str.strip().replace('', pd.NA).notna().sum()) if served else None
            print(f'{col:10s} | {name:28s} | rows {len(out):3d} | column served: {served} | non-blank served cells: {nonblank}', flush=True)
print('PROBE DONE', flush=True)
