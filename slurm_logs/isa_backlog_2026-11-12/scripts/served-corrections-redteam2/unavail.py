"""Outage simulation at HEAD, in a FRESH private cold root.
Usage: python unavail.py sen|ner   -> $RESULTS/unavail_<which>.json
sen: ehcvm_welfare_sen2021.dta raises FileNotFoundError; the build must still serve.
ner: s00_me_ner2018.dta (the new #1708 build input) raises; report what serves."""
import collections
import json
import os
import sys
import warnings

which = sys.argv[1]
import lsms_library
top = os.environ['LSMS_CLONE']
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep)
from lsms_library.paths import data_root
dr = os.path.realpath(str(data_root()))
assert dr.startswith('/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-redteam2/'), dr
assert not os.path.exists(os.path.join(dr, 'Senegal' if which == 'sen' else 'Niger')), 'root not fresh'
from lsms_library import local_tools as lt
import lsms_library as ll

_orig = lt.get_dataframe
hits = collections.Counter()
NEEDLE = 'ehcvm_welfare_sen2021' if which == 'sen' else 's00_me_ner2018'


def patched(path, *a, **k):
    if NEEDLE in str(path):
        hits[NEEDLE] += 1
        raise FileNotFoundError(f'red-team simulated unavailable: {path}')
    return _orig(path, *a, **k)


lt.get_dataframe = patched
if which == 'sen':
    country = 'Senegal'
    CALLS = [('food_acquired', {}), ('food_expenditures', {}), ('food_quantities', {}),
             ('food_prices', {}), ('food_acquired', {'waves': ['2021-22']})]
else:
    country = 'Niger'
    CALLS = [('food_acquired', {'waves': ['2021-22']}), ('food_acquired', {'waves': ['2018-19']}),
             ('food_acquired', {}), ('food_expenditures', {'waves': ['2021-22']}),
             ('food_quantities', {'waves': ['2021-22']}), ('food_prices', {'waves': ['2021-22']}),
             ('food_quantities', {'waves': ['2021-22'], 'units': 'units'}), ('food_expenditures', {})]
out = {'needle': NEEDLE, 'calls': []}
for table, kw in CALLS:
    before = hits[NEEDLE]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        try:
            df = getattr(ll.Country(country), table)(**kw)
            t = df.index.get_level_values('t').astype(str) if 't' in df.index.names else None
            r = {'ok': True, 'rows': len(df),
                 'rows_by_t': {k: int((t == k).sum()) for k in sorted(set(t))} if t is not None else None}
        except BaseException as e:
            r = {'ok': False, 'error': f'{type(e).__name__}: {str(e)[:400]}',
                 'mro': [c.__name__ for c in type(e).__mro__][:6]}
    r['table'], r['kw'] = table, kw
    r['needle_reads'] = hits[NEEDLE] - before
    r['warnings'] = dict(collections.Counter(type(w.message).__name__ for w in caught))
    r['warning_samples_naming_file'] = [str(w.message)[:300] for w in caught if NEEDLE in str(w.message)][:2]
    out['calls'].append(r)
    print(json.dumps(r, default=str)[:2500], flush=True)
out['total_needle_reads'] = hits[NEEDLE]
json.dump(out, open(os.path.join(os.environ['RESULTS'], f'unavail_{which}.json'), 'w'), indent=1, default=str)
print('done unavail', which, flush=True)
