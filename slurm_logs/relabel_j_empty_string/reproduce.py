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

# The fix tests `str(x).strip() != ''` and nothing else, so this set must be
# just the empty string -- a LITERAL 'nan'/'None'/'<NA>' cell survives
# `.dropna()` (the column stays object) and the fix does NOT filter it.  There
# are 0 such cells corpus-wide, but counting them here while the fix ignores
# them would make this script and the code disagree the day one appears.
BLANK = ('',)
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
    t = d['food_items'] if 'food_items' in d else d.get('harmonize_food')
    if t is None or 'Preferred Label' not in t.columns:
        continue
    for col in [c for c in t.columns if c != 'Preferred Label']:
        sub = t[['Preferred Label', col]]
        if hasattr(sub[col], 'columns'):
            continue
        surv = sub.dropna()
        rd = dict(zip(surv['Preferred Label'].astype(str), surv[col].astype(str)))
        # A blank-Preferred-Label row keys '' -> '' and is NOT a merged food.
        # Counting it inflated this sweep from 13 real pairs to 17 and put Mali
        # on the list, which the commit message then contradicted two sentences
        # later.  (Those rows are a parse artefact in the first place: Mali's
        # `| # ...` comment rows and GhanaSPS's trailing padding rows are read
        # as data by all_dfs_from_orgfile.)
        blank = sum(1 for k, v in rd.items()
                    if v.strip() in BLANK and k.strip() not in BLANK)
        if blank:
            pairs += 1
            print(f"{cdir.name:<16} {str(col):<22} {blank:>12} {len(t):>5} {len(sub)-len(surv):>19}")
print(f"\n{pairs} affected (country, column) pairs"
      "  (expected 13: GhanaLSS 8, GhanaSPS 4, Panama 1 -- NOT Mali)")

if '--delivered' not in sys.argv:
    print("\n(pass --delivered to also measure the served frame; needs a warm cache)")
    raise SystemExit

print("\nPART 2 -- delivered: GhanaLSS food_expenditures")
import warnings, numpy as np, pandas as pd, lsms_library as ll
warnings.filterwarnings('ignore')
c = ll.Country('GhanaLSS')
ct = all_dfs_from_orgfile(countries_root() / 'GhanaLSS' / '_' / 'categorical_mapping.org')['harmonize_food']
base = c.food_expenditures()
served = {str(v) for v in base.index.get_level_values('j')}
col0 = base.columns[0]
tot0 = float(pd.to_numeric(base[col0], errors='coerce').sum())
print(f"  baseline {len(base):,} rows, total {col0} = {tot0:,.0f}")
for lab in ['1987-88', '1991-92', '2016-17', 'FCT Code']:
    d = c.food_expenditures(labels=lab)
    tot = float(pd.to_numeric(d[col0], errors='coerce').sum())
    j = d.index.get_level_values('j').astype(str)
    m = np.asarray(j.isin(list(BLANK)))
    # Three counts differ; only the third is "foods merged".  Quoting the
    # first as the third is the error this line exists to prevent.
    sub = ct[['Preferred Label', lab]].dropna()
    rows_blank = sum(1 for v in sub[lab] if str(v).strip() in BLANK)
    old = {}
    for k, v in zip(sub['Preferred Label'], sub[lab]):
        old[str(k).strip()] = str(v).strip()          # last-wins, like to_dict()
    pl_blank = {k for k, v in old.items() if v in BLANK and k not in BLANK}
    print(f"  labels={lab:<9} {len(d):>9,} rows  total "
          f"{'conserved' if abs(tot-tot0) < 1 else 'CHANGED'}"
          f"   blank-j rows {int(m.sum()):>7,}  buckets {len(set(j[m]))}"
          f"   | blank ROWS {rows_blank}, distinct PL {len(pl_blank)},"
          f" SERVED foods merged {len(pl_blank & served)}")
