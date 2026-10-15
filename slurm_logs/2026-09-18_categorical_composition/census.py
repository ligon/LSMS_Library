import sys, os, json, collections
from pathlib import Path
sys.path.insert(0, '/local/job39002769/mirrors/LSMS_Library')
import lsms_library
assert '/local/job39002769/mirrors/LSMS_Library' in lsms_library.__file__, lsms_library.__file__
from lsms_library.local_tools import all_dfs_from_orgfile
import pandas as pd

ROOT = Path('/local/job39002769/mirrors/LSMS_Library')
CDIR = ROOT/'lsms_library/countries'
GDIR = ROOT/'lsms_library/categorical_mapping'

files = []
for p in sorted(GDIR.glob('*.org')):
    files.append(('global', 'GLOBAL', '', p))
for p in sorted(CDIR.glob('*/_/categorical_mapping.org')):
    files.append(('country', p.parts[-3], '', p))
for p in sorted(CDIR.glob('*/*/_/categorical_mapping.org')):
    files.append(('wave', p.parts[-4], p.parts[-3], p))

rows = []
for level, country, wave, p in files:
    try:
        d = all_dfs_from_orgfile(p)
    except Exception as e:
        print('ERR', p, e); continue
    for name, df in d.items():
        rows.append(dict(level=level, country=country, wave=wave,
                         path=str(p.relative_to(ROOT)), table=name,
                         cols=list(df.columns), nrows=len(df), df=df))
print('files:', len(files), ' tables:', len(rows))
import pickle
pickle.dump([{k:v for k,v in r.items()} for r in rows], open('/local/job39002769/tmp/cm/rows.pkl','wb'))
