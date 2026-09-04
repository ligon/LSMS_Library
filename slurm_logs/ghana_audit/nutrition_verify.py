#!/usr/bin/env python
"""GH #563 STEP 3 verification, run on a burst node.  See nutrition_verify.sbatch."""
import time
import traceback

import numpy as np
import pandas as pd

import lsms_library as ll
from lsms_library.paths import countries_root

pd.set_option('display.width', 200)
pd.set_option('display.max_columns', 40)

R = countries_root()
assert 'wt-glss-fct' in str(R), R
print('countries_root:', R)
print('lsms_library  :', ll.__file__)


def household_size(hc):
    """Resident head count from household_characteristics.

    ``roster_to_characteristics`` returns one column per sex-age bucket PLUS
    ``log HSize``; summing all of them would add a log term to the count.
    Buckets already exclude non-resident members (the MonthsSpent/MonthsAway
    filter), so this is the resident count, not the raw roster row count.
    """
    buckets = [c for c in hc.columns if c != 'log HSize']
    return hc[buckets].sum(axis=1).rename('n')


def hdr(s):
    print('\n' + '=' * 78 + f'\n{s}\n' + '=' * 78)


# ---------------------------------------------------------------- CHECK 1
hdr('CHECK 1 -- Country("GhanaLSS").nutrition() builds (COLD)')
t0 = time.time()
c = ll.Country('GhanaLSS')
n = c.nutrition()
print(f'built in {time.time() - t0:.1f} s')
print('shape       :', n.shape)
print('index names :', list(n.index.names))
print('columns     :', list(n.columns))
print('dtypes      :', sorted({str(d) for d in n.dtypes}))
print('\nrows per wave:')
print(n.groupby('t').size().to_frame('rows').to_string())

print('\n-- comparison with the precedents --')
eth = ll.Country('Ethiopia')
try:
    e = eth.nutrition()
    src = 'Country("Ethiopia").nutrition()'
except Exception as exc:                                    # noqa: BLE001
    e = None
    print(f'Ethiopia nutrition() failed ({type(exc).__name__}: {exc}); '
          'falling back to the shipped parquet')
    for p in [f'{R}/../../.local/share/lsms_library/Ethiopia/var/nutrition.parquet',
              '/global/scratch/fsa/fc_jevons/ligon/cache/lsms_library/'
              'Ethiopia/var/nutrition.parquet']:
        try:
            e = pd.read_parquet(p)
            src = p
            break
        except Exception:                                   # noqa: BLE001,PERF203
            continue
if e is not None:
    print(f'Ethiopia source: {src}')
    print('Ethiopia index names:', list(e.index.names), '| GhanaLSS:', list(n.index.names))
    print('index names identical:', list(e.index.names) == list(n.index.names))
    missing = [x for x in e.columns if x not in n.columns]
    extra = [x for x in n.columns if x not in e.columns]
    print(f'columns in Ethiopia not in GhanaLSS: {missing}')
    print(f'columns in GhanaLSS not in Ethiopia: {extra}')

# ---------------------------------------------------------------- CHECK 2
hdr('CHECK 2 -- FCT coverage of food_quantities mass (kg), per wave')
import importlib.util                                        # noqa: E402
spec = importlib.util.spec_from_file_location(
    'glss_nutrition', R / 'GhanaLSS' / '_' / 'nutrition.py')
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
mod.build(report=True)

# ---------------------------------------------------------------- CHECK 3
hdr('CHECK 3 -- median per-capita daily Energy (kcal), per wave')
hc = c.household_characteristics()
print('household_characteristics index:', list(hc.index.names))
print('household_characteristics columns:', list(hc.columns))
# roster_to_characteristics returns sex_age BUCKET COUNTS plus a `log HSize`
# column.  Summing every column would add the log term to the head count.
size = household_size(hc)
print('summed bucket columns:', [c for c in hc.columns if c != "log HSize"])
d = n[['Energy']].join(size, how='inner')
print(f'joined {len(d)} of {len(n)} nutrition rows to a household size')
DAYS = 30.0     # GLSS visits are spread over about a month (CONTENTS.org)
d = d[d['n'] > 0]
d['kcal_cap_day'] = d['Energy'] / d['n'] / DAYS
out = d.groupby('t').agg(households=('Energy', 'size'),
                         median_hh_size=('n', 'median'),
                         median_kcal_cap_day=('kcal_cap_day', 'median'))
out['median_kcal_cap_day'] = out['median_kcal_cap_day'].round(1)
print(out.to_string())
print('\nSANITY BAND 1,500-3,500 kcal/cap/day -- in band?')
for t, v in out['median_kcal_cap_day'].items():
    print(f'  {t}: {v:>9.1f}  {"IN BAND" if 1500 <= v <= 3500 else "OUT OF BAND"}')

# ---------------------------------------------------------------- CHECK 4
hdr('CHECK 4 -- Feature("nutrition") assembles across countries')
for pair in (['GhanaLSS', 'Ethiopia'], ['GhanaLSS', 'Uganda']):
    try:
        t0 = time.time()
        f = ll.Feature('nutrition')(pair)
        print(f'{pair}: OK  shape={f.shape}  index={list(f.index.names)}  '
              f'({time.time() - t0:.1f} s)')
        print('   rows per country:', f.groupby('country').size().to_dict())
    except Exception as exc:                                 # noqa: BLE001
        print(f'{pair}: FAILED -- {type(exc).__name__}: {exc}')
        traceback.print_exc(limit=3)

print('\nDONE')
