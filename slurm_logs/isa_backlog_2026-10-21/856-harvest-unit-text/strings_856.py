"""GH #856 step 2 -- per-string counts of the unit specify texts on code-13
rows, split into RECOVERED (in malawi._UNIT_TEXT_RECODE) and the near misses
deliberately LEFT (any 90 kg / dengu / basket mention that is not on the
list).  Raw module rows, all four modules, every wave with text.

    python strings_856.py <out.json>
"""
import collections
import importlib.util
import re
import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd

import lsms_library
TOP = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True,
                              cwd=Path(__file__).parent).strip()
assert os.path.realpath(lsms_library.__file__).startswith(os.path.realpath(TOP) + os.sep)
from lsms_library.data_access import get_data_file  # noqa: E402
from lsms_library.local_tools import get_dataframe  # noqa: E402
from lsms_library.paths import countries_root  # noqa: E402

M = countries_root() / 'Malawi' / '_'
sys.path.insert(0, str(M))
spec = importlib.util.spec_from_file_location('malawi_856s', M / 'malawi.py')
mw = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mw)

FILES = []
for w, half, sfx in [('2010-11', 'Full_Sample/Agriculture', ''),
                     ('2016-17', 'Cross_Sectional', ''), ('2016-17', 'Panel', '_16'),
                     ('2019-20', 'Cross_Sectional', ''), ('2019-20', 'Panel', '_19')]:
    os_ = '_os' if w == '2010-11' else '_oth'
    for mod, ucol in (('g', 'ag_g13b'), ('p', 'ag_p09b'), ('i', 'ag_i02b'), ('q', 'ag_q02b')):
        FILES.append((w, half, mod.upper(), f'Malawi/{w}/Data/{half}/ag_mod_{mod}{sfx}.dta',
                      ucol, ucol + os_))

target = {s: code for code, ss in mw._UNIT_TEXT_RECODE.items() for s in ss}
#: Texts naming a unit the code list STILL offers (left by design: the scope
#: is the two dropped codes 3 and 11).  Token match, first hit wins; counted
#: so the follow-up is sized.
LISTED = [('50 kg Bag (code 2)', r'(?:^|\D)50\s*-?\s*KG'),
          ('Pail (codes 4/5)', r'PAIL'), ('Bunch (code 8)', r'BUNCH'),
          ('Plate (codes 6/7)', r'PLATE'), ('Piece (code 9)', r'PIECE'),
          ('Bale (code 10)', r'\bBALES?\b'), ('Ox-Cart (code 12)', r'OX\s*-?\s*CART')]
listed = collections.defaultdict(collections.Counter)
near = ('90', '09KG', 'DENG', 'BASK', 'BUSK', 'BAMB')
rec = collections.defaultdict(collections.Counter)
left = collections.defaultdict(collections.Counter)
for w, half, mod, rel, ucol, tcol in FILES:
    df = get_dataframe(str(get_data_file(rel)), convert_categoricals=False)
    u = pd.to_numeric(df[ucol], errors='coerce')
    t = mw._normalise_unit_text(df[tcol])
    t = t[(u == 13) & t.notna()]
    for s, n in t.value_counts().items():
        cell = f'{mod}:{w}:{half.split("/")[0]}'
        if s in target:
            rec[s][cell] += int(n)
            continue
        if any(k in s for k in near):
            left[s][cell] += int(n)
        for name, rx in LISTED:
            if re.search(rx, s):
                listed[name][s] += int(n)
                break

out = {
    'recovered': {s: {'to_code': target[s], 'total': sum(c.values()), 'by_cell': dict(c)}
                  for s, c in sorted(rec.items(), key=lambda kv: -sum(kv[1].values()))},
    'unused_whitelist_entries': sorted(set(target) - set(rec)),
    'left_near_misses': {s: {'total': sum(c.values()), 'by_cell': dict(c)}
                         for s, c in sorted(left.items(), key=lambda kv: -sum(kv[1].values()))},
    'left_restating_a_listed_code': {
        name: {'total': sum(c.values()), 'exact_plain': int(c.get(name.split(' (')[0].upper(), 0)),
               'strings': dict(c.most_common())}
        for name, c in listed.items()},
}
Path(sys.argv[1]).write_text(json.dumps(out, indent=1) + '\n')
for code in (3, 11):
    tot = sum(v['total'] for v in out['recovered'].values() if v['to_code'] == code)
    print('code', code, 'raw rows recovered', tot)
print('unused whitelist entries:', out['unused_whitelist_entries'])
print('left:', {s: v['total'] for s, v in out['left_near_misses'].items()})
print('left, restating a listed code:', {k: v['total'] for k, v in out['left_restating_a_listed_code'].items()})
