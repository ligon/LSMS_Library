#!/usr/bin/env python
"""Check every measured claim in GhanaLSS/_/categorical_mapping.org:511-521
(the "No Aggregate Label column, deliberately" passage).

Run:  cd <repo> && PYTHONPATH=$PWD .venv/bin/python <this>
"""
import sys
from collections import defaultdict
import lsms_library
from lsms_library.local_tools import all_dfs_from_orgfile
from lsms_library.paths import countries_root

G = countries_root() / 'GhanaLSS'
BAD = {'', 'nan', 'None', '<NA>'}
ok = lambda b: 'OK  ' if b else 'FAIL'

country = all_dfs_from_orgfile(G / '_' / 'categorical_mapping.org')['harmonize_food']
cp = {x for x in country['Preferred Label'].astype(str).str.strip()} - BAD
print(f"[{ok('Aggregate Label' not in country.columns)}] country table has no 'Aggregate Label'")
print(f"[{ok(len(cp) == 195)}] 195 distinct Preferred Labels           measured {len(cp)}")

agg = defaultdict(lambda: defaultdict(set))
n_wave = 0
for w in sorted(p.parent.parent.name for p in G.glob('*/_/categorical_mapping.org')):
    t = all_dfs_from_orgfile(G / w / '_' / 'categorical_mapping.org').get('harmonize_food')
    if t is None or 'Aggregate Label' not in t.columns:
        continue
    n_wave += 1
    for p, a in zip(t['Preferred Label'].astype(str).str.strip(),
                    t['Aggregate Label'].astype(str).str.strip()):
        if p in BAD or a in BAD:
            continue
        agg[p][a].add(w)

conflict = {p: v for p, v in agg.items() if len(v) > 1}
orphan = cp - set(agg)
print(f"[{ok(n_wave == 7)}] 7 wave tables carry 'Aggregate Label'    measured {n_wave}")
print(f"[{ok(len(conflict) == 14)}] 14 Preferred Labels conflict            measured {len(conflict)}"
      f"   <-- prose says 14")
print(f"[{ok(len(orphan) == 9)}] 9 orphan Preferred Labels               measured {len(orphan)}")

files = [G / '_' / 'categorical_mapping.org'] + sorted(G.glob('*/_/categorical_mapping.org'))
moji = [(f.parent.parent.name, n, str(c), str(v))
        for f in files
        for n, t in all_dfs_from_orgfile(f).items()
        for c in t.columns if not hasattr(t[c], 'columns')
        for v in t[c].astype(str) if any(ord(ch) > 127 for ch in str(v))]
in_agg = [r for r in moji if r[2] == 'Aggregate Label']
print(f"[{ok(len(in_agg) >= 1)}] mojibake in an Aggregate Label cell      measured {len(in_agg)}"
      f"   <-- prose cites 'Cooked Rice and Stew Ê Ê'")
print(f"\n     (for context: {len(moji)} non-ASCII cells remain elsewhere in GhanaLSS mapping tables;"
      f"\n      {sum(1 for r in moji if any(ord(c) in (0x92, 0x94, 0x96) for c in r[3]))} are cp1252-as-Latin1 C1 controls)")
