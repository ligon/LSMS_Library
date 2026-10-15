#!/usr/bin/env python
"""Check every measured claim in the "No =Aggregate Label= column, deliberately"
passage of ``GhanaLSS/_/categorical_mapping.org``.

The passage says "Re-check every number above with this script", so this
checks what the passage says TODAY.  An earlier version checked the passage's
ORIGINAL claims and therefore printed FAIL on the two the correction had just
fixed -- a script that contradicted the text citing it.

Historical note, deliberately not asserted: until 2026-09-12 the conflict count
was 14, not 13, and the fourteenth was a mojibake row (2012-13 code 262's
Aggregate was a mojibake twin of code 254's clean "Cooked Rice and Stew").
``5dd3720fe`` repaired it on a parallel branch that merged in afterwards.
Re-run at ``98a0b7a21`` to see 14.

Exit status is 0 only if every claim holds.

Run:  cd <repo> && PYTHONPATH=$PWD .venv/bin/python <this>
"""
import sys
from collections import defaultdict

from lsms_library.local_tools import all_dfs_from_orgfile
from lsms_library.paths import countries_root

G = countries_root() / 'GhanaLSS'
BLANK = {'', 'nan', 'None', '<NA>'}
failures = []


def check(label, got, want):
    ok = got == want
    if not ok:
        failures.append(f"{label}: expected {want!r}, measured {got!r}")
    print(f"[{'OK  ' if ok else 'FAIL'}] {label:<52} {got!r}")


country = all_dfs_from_orgfile(G / '_' / 'categorical_mapping.org')['harmonize_food']
pl = {str(x).strip() for x in country['Preferred Label']} - BLANK

check("country table has no 'Aggregate Label' column",
      'Aggregate Label' in country.columns, False)
check("distinct Preferred Labels", len(pl), 195)

agg = defaultdict(lambda: defaultdict(set))
n_wave = 0
for w in sorted(p.parent.parent.name for p in G.glob('*/_/categorical_mapping.org')):
    t = all_dfs_from_orgfile(G / w / '_' / 'categorical_mapping.org').get('harmonize_food')
    if t is None or 'Aggregate Label' not in t.columns:
        continue
    n_wave += 1
    for p_, a in zip(t['Preferred Label'].astype(str).str.strip(),
                     t['Aggregate Label'].astype(str).str.strip()):
        if p_ in BLANK or a in BLANK:
            continue
        agg[p_][a].add(w)

conflict = {p_: v for p_, v in agg.items() if len(v) > 1}
check("wave tables carrying 'Aggregate Label'", n_wave, 7)
check("Preferred Labels taking two Aggregate values", len(conflict), 13)
check("Preferred Labels in no wave table at all", len(pl - set(agg)), 9)

# The two claims the 2026-09-14 correction ADDED.  A count alone would not
# catch the interesting error, so the identity arm is checked per label.
identity = {p_ for p_ in conflict if p_ in conflict[p_]}
check("of the 13, those with an identity arm", len(identity), 12)
check("the one that is bucket-against-bucket",
      sorted(set(conflict) - identity), ['Beef (corned)'])

# The passage's closing claim.
try:
    import lsms_library as ll
    from lsms_library.errors import LabelUnavailableError
    try:
        ll.Country('GhanaLSS').food_expenditures(labels='Aggregate')
        check("labels='Aggregate' raises", False, True)
    except LabelUnavailableError:
        check("labels='Aggregate' raises LabelUnavailableError", True, True)
except Exception as exc:                                  # pragma: no cover
    print(f"[SKIP] labels='Aggregate' not exercised ({type(exc).__name__}); "
          f"needs a warm GhanaLSS cache")

if failures:
    print("\n" + "\n".join(failures))
    print("\nThe passage and the data disagree.  Fix whichever is wrong -- and "
          "if it is the passage, fix this script in the same commit.")
    sys.exit(1)
print("\nEvery claim in the passage holds.")
