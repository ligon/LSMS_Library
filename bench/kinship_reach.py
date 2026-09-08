"""Which kinship.yml keys does the corpus actually reach?

Wraps Country._expand_kinship's lookup input: for every country with a
household_roster, records the labels (post categorical mapping, i.e. exactly
what _expand_kinship sees) and reports kinship.yml keys no country produces.
Warm reads only -- run after the shared cache is re-warmed:

    PYTHONPATH=. .venv/bin/python bench/kinship_reach.py [Country ...]
"""
import sys, warnings, collections
warnings.simplefilter('ignore')
import pandas as pd, yaml
import lsms_library as ll
from lsms_library import country as cmod
from lsms_library.catalog import countries
# run with PYTHONPATH pointing at the checkout under test

seen = collections.defaultdict(set)          # normalised label -> {countries}
raw_seen = collections.defaultdict(set)
_orig = cmod._expand_kinship
def _rec(df, *a, **k):
    if isinstance(df, pd.DataFrame) and 'Relationship' in df.columns:
        for lab in pd.unique(df['Relationship'].dropna().astype(str)):
            seen[lab.strip().title()].add(_rec.country); raw_seen[lab].add(_rec.country)
    return _orig(df, *a, **k)
cmod._expand_kinship = _rec

only = sys.argv[1:]
for c in (only or countries()):
    C = ll.Country(c)
    if 'household_roster' not in C.data_scheme: continue
    _rec.country = c
    try:
        C.household_roster()
        print(f'ok   {c}', flush=True)
    except Exception as e:
        print(f'FAIL {c}: {type(e).__name__}: {str(e)[:100]}', flush=True)

km = yaml.safe_load(open(cmod.__file__.replace('country.py', 'categorical_mapping/kinship.yml')))
keys = [k for k in km if isinstance(k, str)]
def hit(k):
    return k.strip().title() in seen or k in raw_seen
unreached = [k for k in keys if not hit(k)]
print(f'\nkinship.yml keys: {len(keys)}; reached: {len(keys)-len(unreached)}; UNREACHED: {len(unreached)}')
for k in unreached: print('  ', repr(k), km[k])
unknown = sorted(l for l in raw_seen if l.strip().title() not in {k.strip().title() for k in keys} and l not in keys)
print(f'\nlabels seen but NOT in kinship.yml: {len(unknown)}')
for l in unknown: print('  ', repr(l), sorted(raw_seen[l]))
