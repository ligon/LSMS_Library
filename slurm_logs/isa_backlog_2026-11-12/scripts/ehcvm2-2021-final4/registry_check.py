"""Registry check on one checkout: BASES order, every registry loads, and the bases of the
Togo and Cote d'Ivoire 2021-22 s07bq03a records.  Config only (no data read).

usage: python registry_check.py OUT_JSON
"""
import json, os, sys, warnings
warnings.filterwarnings('ignore')
import lsms_library
CHECKOUT = os.environ['PYTHONPATH'].split(':')[0]
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(CHECKOUT) + os.sep), lsms_library.__file__
import importlib
# `lsms_library.derivations` the attribute is the public table function (a deliberate
# shadow, see lsms_library/__init__.py); the module is reached through importlib.
d = importlib.import_module('lsms_library.derivations')

out = {'checkout': CHECKOUT, 'BASES': list(d.BASES)}
table = d.derivations_table()
out['registry_rows'] = int(len(table))
out['bases_strings'] = {str(k): int(v) for k, v in table['bases'].value_counts().items()}
out['records'] = {}
for country in ('CotedIvoire', 'Togo'):
    for key, rec in d.derivation_records(country).items():
        if 's07bq03a' not in key:
            continue
        out['records'][key] = {
            'bases': list(rec.bases),
            'assumption_bases': [a.get('basis') for a in rec.assumptions],
            'table_bases': table.loc[key, 'bases'],
            'n_assumptions': int(table.loc[key, 'n_assumptions']),
        }
# Every record whose bases include both derived-from-data and filename (the only pair the
# reorder could flip): expected none.
out['records_with_filename'] = sorted(k for k, b in table['bases'].items() if 'filename' in str(b).split('+'))
json.dump(out, open(sys.argv[1], 'w'), indent=1, ensure_ascii=False)
print(json.dumps(out, ensure_ascii=False)[:3000])
