"""Per EHCVM country-wave: share of consumed food rows whose unit SIZE (s07bq03c) is not the
size-free code, i.e. information the served food_acquired.u drops.  Read-only."""
import os, sys, glob, json, warnings, yaml
import pandas as pd
ck = sys.argv[1]
import lsms_library
assert os.path.realpath(lsms_library.__file__).startswith(ck + os.sep)
from lsms_library.local_tools import get_dataframe
warnings.simplefilter('ignore')
class L(yaml.SafeLoader): pass
L.add_multi_constructor('!', lambda l, s, n: None)
out = {}
for di in sorted(glob.glob(f'{ck}/lsms_library/countries/*/*/_/data_info.yml')):
    d = yaml.load(open(di), Loader=L) or {}
    fa = d.get('food_acquired')
    if not isinstance(fa, dict) or 'ConsumptionUnitSize' not in str(fa): continue
    files = fa.get('file')
    if not isinstance(files, str):
        continue
    country, wave = di.split('/')[-4], di.split('/')[-3]
    cwd = os.getcwd(); os.chdir(os.path.dirname(di))
    try:
        df = get_dataframe(files if files.startswith('..') else '../Data/' + files)
    finally:
        os.chdir(cwd)
    q = pd.to_numeric(df['s07bq03a'], errors='coerce')
    size = df['s07bq03c']
    lab = size.astype('string').str.strip()
    consumed = q > 0
    sizefree = lab.str.contains('unique', case=False, na=False) | lab.isin(['0', '0.0']) | size.isna()
    sized = consumed & ~sizefree
    out[f'{country}/{wave}'] = {'consumed_rows': int(consumed.sum()), 'sized_rows': int(sized.sum()),
        'share': round(float(sized.sum()) / max(int(consumed.sum()), 1), 3),
        'top_sizes': lab[sized].value_counts().head(6).to_dict()}
    print(country, wave, out[f'{country}/{wave}']['consumed_rows'], out[f'{country}/{wave}']['sized_rows'], out[f'{country}/{wave}']['share'], flush=True)
json.dump(out, open(sys.argv[2], 'w'), indent=1, ensure_ascii=False)
