"""Recall coverage counts from the packaged records (the test_recall_period rule).

usage: recall_counts.py OUT.json
"""
import json
import os
import subprocess
import sys
from pathlib import Path

import lsms_library  # noqa: F401
top = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(top) + os.sep)
import lsms_library.recall  # noqa: E402,F401  (bind the submodule, not ll.recall)
R = sys.modules['lsms_library.recall']
from lsms_library.paths import countries_root  # noqa: E402
import lsms_library as ll  # noqa: E402

cells = []
for path in sorted(Path(countries_root()).glob("*/_/recall.yml")):
    country = path.parent.parent.name
    for wave, rec in sorted(R.recall_records(country).items()):
        cells.append((country, wave, R.unavailable_reason(rec) is None, rec.basis))
n_conv = sum(c[2] for c in cells)
bases = {}
for c in cells:
    bases[c[3]] = bases.get(c[3], 0) + 1
tbl = ll.recall()
out = {'cells': len(cells), 'convertible': n_conv, 'guide': f'**{n_conv} of {len(cells)}**',
       'bases': bases, 'll_recall_rows': int(len(tbl)),
       'new_cells': [c[:2] for c in cells if (c[0], c[1]) in
                     {('Malawi', '2024-25'), ('Togo', '2021-22'), ('Benin', '2021-22'),
                      ('CotedIvoire', '2021-22')}]}
with open(sys.argv[1], 'w') as fh:
    json.dump(out, fh, indent=1, sort_keys=True)
print(json.dumps(out, sort_keys=True))
