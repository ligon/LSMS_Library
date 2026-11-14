"""What does Senegal serve when ehcvm_welfare_sen2021.dta cannot be fetched?
Run in a FRESH, private, cold data root.  Patches get_dataframe to raise FileNotFoundError
for that one file only.  Aggregates -> $RESULTS/unavail.json"""
import json
import os
import traceback
import warnings
import collections

import lsms_library
top = os.environ['LSMS_CLONE']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep)
from lsms_library.paths import data_root
assert os.path.realpath(str(data_root())).startswith(
    '/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-redteam/')
from lsms_library import local_tools as lt
import lsms_library as ll

_orig = lt.get_dataframe
hits = collections.Counter()


def patched(path, *a, **k):
    if 'ehcvm_welfare_sen2021' in str(path):
        hits['welfare'] += 1
        raise FileNotFoundError(f'red-team simulated unavailable: {path}')
    return _orig(path, *a, **k)


lt.get_dataframe = patched
out = {}
for table, kw in (('food_acquired', {}), ('food_acquired', {'waves': ['2018-19']}),
                  ('food_expenditures', {}), ('food_acquired', {})):
    tag = table + ('' if not kw else '__' + '_'.join(kw['waves']))
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df = getattr(ll.Country('Senegal'), table)(**kw)
            t = df.index.get_level_values('t').astype(str)
            r = {'ok': True, 'rows': len(df), 'rows_by_t': {k: int((t == k).sum()) for k in sorted(set(t))}}
        except BaseException as e:
            r = {'ok': False, 'error': f'{type(e).__name__}: {str(e)[:600]}',
                 'mro': [c.__name__ for c in type(e).__mro__][:6]}
    r['warnings'] = dict(collections.Counter(type(w.message).__name__ for w in caught))
    r['warning_samples'] = [str(w.message)[:300] for w in caught if 'welfare' in str(w.message)][:3]
    out[tag + ('' if tag not in out else '__second_call')] = r
    print(tag, json.dumps(r)[:2000], flush=True)
out['patched_calls'] = dict(hits)
# what got written for 2021-22?
import glob
out['wave_parquets'] = sorted(os.path.relpath(p, str(data_root())) for p in glob.glob(str(data_root() / 'Senegal' / '*' / '_' / 'food_acquired.parquet')))
out['var_parquet'] = os.path.exists(str(data_root() / 'Senegal' / 'var' / 'food_acquired.parquet'))
json.dump(out, open(os.path.join(os.environ['RESULTS'], 'unavail.json'), 'w'), indent=1)
print('done unavail', flush=True)
