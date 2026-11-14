"""Cold check in a private root: which Quantity_sold_kg rows Site Q names, and the
new message text (GH #1560 red team F2).

usage: coldcheck.py <out.json>

Builds Mali and Niger crop_production through the Country API into an EMPTY
private data root, captures the QuantityImplausibleWarning text emitted by the
read, and compares the ledger's Quantity_sold_kg reports with a direct
audit_quantities call on the served frame.  Household and plot ids are redacted
from every message written out; the JSON carries crop, unit, wave, direction,
value, reference, ratio and rung only.
"""
import json
import os
import re
import subprocess
import sys
import warnings

import lsms_library

top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep), lsms_library.__file__
from lsms_library import paths  # noqa: E402

ROOT = paths.data_root()
assert '/private/sold-kg-final/' in os.path.realpath(str(ROOT)), ROOT
assert '/cache/lsms_library' not in str(ROOT) and '.local/share' not in str(ROOT), ROOT

import lsms_library as ll  # noqa: E402
from lsms_library.quantity_audit import (QuantityImplausibleWarning,  # noqa: E402
                                         audit_quantities, quantity_reports)

COLUMN = 'Quantity_sold_kg'
TABLE = 'crop_production'


def redact(text):
    text = re.sub(r'(?<![A-Za-z_])i=\S+', 'i=<redacted>', text)
    return re.sub(r'(?<![A-Za-z_])plot=\S+', 'plot=<redacted>', text)


def summary(reports):
    rows = [[r['wave'], r['direction'], o['j'], o['u'], o['value'], o['reference'],
             o['ratio'], o['rung']] for r in reports for o in r['offenders']]
    return sorted(rows, key=lambda x: json.dumps(x, default=str))


out = {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
       'data_root': os.path.realpath(str(ROOT))}
for country in ('Mali', 'Niger'):
    cold = not (ROOT / country).exists()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        df = ll.Country(country).crop_production()
    ledger = [r for r in quantity_reports(country=country, table=TABLE) if r['column'] == COLUMN]
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        direct = audit_quantities(df, COLUMN, country=country, table=TABLE, tails=('high', 'low'))
    messages = [redact(str(w.message)) for w in caught
                if issubclass(w.category, QuantityImplausibleWarning)
                and f"report a '{COLUMN}'" in str(w.message)]
    leaked = [m for m in messages if re.search(r'(?<![A-Za-z_])i=(?!<redacted>)', m)]
    out[country] = {'cold_at_start': cold, 'rows': int(len(df)),
                    'sold_kg_rows': int(df[COLUMN].notna().sum()),
                    'ledger': summary(ledger), 'direct': summary(direct),
                    'messages': messages, 'unredacted_messages': len(leaked),
                    'other_quantity_warnings': sum(
                        1 for w in caught if issubclass(w.category, QuantityImplausibleWarning)
                        and f"report a '{COLUMN}'" not in str(w.message))}

mali, niger = out['Mali'], out['Niger']
expected = sorted([['2021-22', 'high', 1150000.0], ['2021-22', 'high', 437500.0],
                   ['2021-22', 'high', 400000.0], ['2021-22', 'low', 5.0]])
checks = {
    'both_cold_at_start': mali['cold_at_start'] and niger['cold_at_start'],
    'mali_four_rows': sorted([r[0], r[1], r[4]] for r in mali['ledger']) == expected,
    'mali_ledger_equals_direct': mali['ledger'] == mali['direct'],
    'niger_none': niger['ledger'] == [] and niger['direct'] == [] and niger['messages'] == [],
    'mali_two_messages': len(mali['messages']) == 2,
    'messages_say_kg_sold': all('kg sold' in m and 'harvest_u=' in m and 'TOTAL kilograms sold' in m
                                for m in mali['messages']),
    'messages_have_no_bare_u': all(' u=' not in m for m in mali['messages']),
    'arachide_line': any('Arachide harvest_u=Sac (100 Kg) 1,150,000 kg sold (x511.1' in m
                         for m in mali['messages']),
    'redacted': mali['unredacted_messages'] == 0 and niger['unredacted_messages'] == 0,
}
out['checks'] = checks
with open(sys.argv[1], 'w') as fh:
    json.dump(out, fh, indent=1, default=str)
print('checks', json.dumps(checks))
for m in mali['messages']:
    print('MESSAGE', m)
assert all(checks.values()), checks
print('COLDCHECK OK')
