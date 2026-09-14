#!/usr/bin/env python
"""_relabel_j's .dropna() does not catch EMPTY-STRING crosswalk cells.

country.py:_relabel_j builds its rename dict as

    rdict = table[['Preferred Label', target]].dropna().set_index(...)[target].to_dict()

`.dropna()` is meant to leave an unlabelled food carrying its Preferred Label.
It only catches NaN.  Where the org table's blank cells parse as EMPTY STRINGS
(GhanaLSS, GhanaSPS), every such food is renamed to '' instead -- and because
food_expenditures / food_quantities call _relabel_j with reaggregate=True,
they are all SUMMED into one unnamed j bucket.

Part 1 is config-only (seconds).  Part 2 needs a warm cache.

Run:  cd <repo> && PYTHONPATH=$PWD .venv/bin/python <this> [--delivered]
"""
import sys
from lsms_library.local_tools import all_dfs_from_orgfile
from lsms_library.paths import countries_root

BLANK = ('', 'nan', 'None', '<NA>')
root = countries_root()

print("PART 1 -- config only: (country, column) pairs where a labels= call")
print("         merges >= 1 curated food into a single unnamed bucket\n")
print(f"{'country':<16} {'column':<22} {'foods->blank':>12} {'of':>5} {'NaNs dropna caught':>19}")
pairs = 0
for cdir in sorted(p for p in root.iterdir() if p.is_dir()):
    f = cdir / '_' / 'categorical_mapping.org'
    if not f.exists():
        continue
    try:
        d = all_dfs_from_orgfile(f)
    except Exception:
        continue
    t = d.get('food_items') or d.get('harmonize_food')
    if t is None or 'Preferred Label' not in t.columns:
        continue
    for col in [c for c in t.columns if c != 'Preferred Label']:
        sub = t[['Preferred Label', col]]
        if hasattr(sub[col], 'columns'):
            continue
        surv = sub.dropna()
        rd = dict(zip(surv['Preferred Label'].astype(str), surv[col].astype(str)))
        blank = sum(1 for v in rd.values() if v.strip() in BLANK)
        if blank:
            pairs += 1
            print(f"{cdir.name:<16} {str(col):<22} {blank:>12} {len(t):>5} {len(sub)-len(surv):>19}")
print(f"\n{pairs} affected (country, column) pairs")

if '--delivered' not in sys.argv:
    print("\n(pass --delivered to also measure the served frame; needs a warm cache)")
    raise SystemExit

print("\nPART 2 -- delivered: GhanaLSS food_expenditures")
import warnings, numpy as np, pandas as pd, lsms_library as ll
warnings.filterwarnings('ignore')
c = ll.Country('GhanaLSS')
base = c.food_expenditures()
col0 = base.columns[0]
tot0 = float(pd.to_numeric(base[col0], errors='coerce').sum())
print(f"  baseline {len(base):,} rows, total {col0} = {tot0:,.0f}")
for lab in ['1987-88', '1991-92', '2016-17', 'FCT Code']:
    d = c.food_expenditures(labels=lab)
    tot = float(pd.to_numeric(d[col0], errors='coerce').sum())
    j = d.index.get_level_values('j').astype(str)
    m = np.asarray(j.isin(list(BLANK)))
    print(f"  labels={lab:<9} {len(d):>9,} rows  total {'conserved' if abs(tot-tot0) < 1 else 'CHANGED'}"
          f"   blank-j rows {int(m.sum()):>7,}  buckets {len(set(j[m]))}")
