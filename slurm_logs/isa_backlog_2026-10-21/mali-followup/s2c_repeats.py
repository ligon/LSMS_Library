"""Mali 2014-15 EACIS2C_p2: (household, plot) keys that repeat.

Counts the repeated (grappe, menage, s2cq01, s2cq02) keys, whether the
repeats differ on the roster line s2cq00, and how their group answers
(s2cq21, s2cq26) compare across the repeat.  Aggregate counts only.

Usage (PYTHONPATH=<checkout>, private LSMS_DATA_DIR):
    python s2c_repeats.py <checkout> <out.json>
"""
import json
import os
import sys
import warnings
from pathlib import Path

checkout, out_path = Path(sys.argv[1]).resolve(), sys.argv[2]
import pandas as pd  # noqa: E402
import lsms_library  # noqa: E402
from lsms_library.local_tools import get_dataframe  # noqa: E402

assert os.path.realpath(lsms_library.__file__).startswith(str(checkout) + os.sep), lsms_library.__file__
warnings.simplefilter('ignore')

wave_dir = checkout / 'lsms_library/countries/Mali/2014-15/_'
cwd = os.getcwd()
os.chdir(wave_dir)
try:
    s2c = get_dataframe('../Data/EACIS2C_p2.dta')
finally:
    os.chdir(cwd)

key = ['grappe', 'menage', 's2cq01', 's2cq02']
dup = s2c[s2c.duplicated(key, keep=False)].copy()
res = {'rows': int(len(s2c)), 'repeated_keys': int(dup.groupby(key).ngroups),
       'rows_on_repeated_keys': int(len(dup))}
g = dup.groupby(key)
res['keys_where_s2cq00_differs'] = int((g['s2cq00'].nunique(dropna=False) > 1).sum())
res['keys_where_s2cq00_same'] = int((g['s2cq00'].nunique(dropna=False) == 1).sum())
for flag in ('s2cq21', 's2cq26'):
    ans = dup[flag].astype('string').str.strip().fillna('<NA>')
    combos = ans.groupby([dup[k] for k in key]).agg(lambda s: '/'.join(sorted(s)))
    res[f'{flag}_answer_patterns'] = {k: int(v) for k, v in combos.value_counts().items()}

with open(out_path, 'w') as fh:
    json.dump(res, fh, indent=1, sort_keys=True)
print(json.dumps(res, indent=1, sort_keys=True))
