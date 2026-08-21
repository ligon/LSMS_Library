#!/usr/bin/env python
"""Blast-radius probe: which countries' household_roster breaks when forced to
rebuild from source (no L2 cache), under the interview_date function-name
collision regression?  Candidates = countries whose module defines
interview_date() (branch #438 work)."""
import os
os.environ['LSMS_SKIP_AUTH'] = '1'
os.environ['LSMS_NO_CACHE'] = '1'          # force rebuild from source
import warnings; warnings.filterwarnings('ignore')
import lsms_library as ll

CANDIDATES = ['Benin', 'Burkina_Faso', 'CotedIvoire', 'Guinea-Bissau',
              'Mali', 'Niger', 'Senegal', 'Togo', 'Malawi']

for c in CANDIDATES:
    try:
        r = ll.Country(c).household_roster()
        print(f'OK   {c}: {r.shape[0]:>7,} rows', flush=True)
    except Exception as e:
        print(f'FAIL {c}: {type(e).__name__}: {str(e).splitlines()[0][:90]}', flush=True)
