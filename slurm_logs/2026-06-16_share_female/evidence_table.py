#!/usr/bin/env python
"""Complete evidence table for the interview_date roster-collision regression.

For each EHCVM country whose module defines interview_date(), report per wave:
  - does household_roster YAML declare an `interview_date` myvar? (collision precondition)
  - does the wave-level YAML build raise? (the regression)
  - does the country-level build still succeed (make/script fallback masks it)?
"""
import os
os.environ['LSMS_SKIP_AUTH'] = '1'
os.environ['LSMS_NO_CACHE'] = '1'
import warnings; warnings.filterwarnings('ignore')
import yaml
import lsms_library as ll
from lsms_library.paths import countries_root

COUNTRIES = ['Benin', 'Burkina_Faso', 'CotedIvoire', 'Guinea-Bissau',
             'Mali', 'Niger', 'Senegal', 'Togo']


def roster_has_interview_date(country, wave):
    p = countries_root() / country / wave / '_' / 'data_info.yml'
    if not p.exists():
        return '?'
    d = yaml.safe_load(open(p))
    hr = d.get('household_roster', {})
    def find(o):
        if isinstance(o, dict):
            if 'myvars' in o and isinstance(o['myvars'], dict) and 'interview_date' in o['myvars']:
                return True
            return any(find(v) for v in o.values())
        if isinstance(o, list):
            return any(find(v) for v in o)
        return False
    return find(hr)


print(f"{'country':14s} {'wave':9s} {'rosterHasIntDate':16s} {'waveBuild':28s}")
print('-' * 75)
for c in COUNTRIES:
    C = ll.Country(c)
    for wv in C.waves:
        has = roster_has_interview_date(c, wv)
        try:
            r = C[wv].household_roster()
            wb = f'OK ({r.shape[0]} rows)'
        except Exception as e:
            wb = f'FAIL {type(e).__name__}: {str(e).splitlines()[0][:30]}'
        print(f'{c:14s} {wv:9s} {str(has):16s} {wb:28s}', flush=True)
    # country-level (fallback may mask)
    try:
        rc = C.household_roster()
        tv = sorted(set(rc.index.get_level_values('t')))
        print(f'  -> COUNTRY build: OK, waves present={tv}, rows={len(rc)}', flush=True)
    except Exception as e:
        print(f'  -> COUNTRY build: FAIL {type(e).__name__}', flush=True)
    print()
