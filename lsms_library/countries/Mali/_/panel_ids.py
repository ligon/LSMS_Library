#!/usr/bin/env python3
"""Build Mali panel_ids + updated_ids for the EHCVM panel.

Mali has four waves: 2014-15 EACI, 2017-18 EACI, 2018-19 EHCVM,
2021-22 EHCVM. Only the two EHCVM waves are panel-linked; EACI
and EHCVM are different survey programs with no household overlap.

The 2021-22 cover sheet (``s00_me_mli2021.dta``) has a mixed panel
structure that is more heterogeneous than Niger's or Senegal's:

- ``PanelHH`` flag: 5023 households marked ``'Oui'``, 1120 marked
  ``'Non'``.
- ``s00q07f1`` / ``s00q07f2``: previous-wave grappe / menage
  columns. Only 2997 of the 5023 panel households have these
  columns filled. Of those 2997, 2986 have ``(cur_g, cur_m) ==
  (prev_g, prev_m)`` (identity) and 11 have moved between
  clusters.
- The remaining 2026 panel households have no linkage columns,
  but 2025 of them have a natural ``(grappe, menage)`` match in
  the 2018-19 baseline — i.e. they are identity-linked by
  convention.

The script handles both sub-groups:

1. **Explicit linkage** (2997 HHs): build ``prev_i`` from the
   s00q07f1 / s00q07f2 columns, use as-is (including the 11
   moved households).
2. **Implicit linkage** (2026 HHs): use identity ``(cur_g, cur_m)``
   as the previous-wave ID; keep only entries whose canonical form
   exists in the 2018-19 baseline cover.

Total: ~5022 households linked (one orphan in the no-linkage
bucket is dropped).

The ID construction uses Mali's ``i()`` formatter from
``mali.py``: ``format_id(grappe) + '0' + format_id(menage, zp=2)``.
For ``(grappe=1, menage=1)`` this gives ``'1001'``; for
``(grappe=74, menage=12)`` it gives ``'74012'``. Matches what
``Country('Mali').household_roster()`` emits.
"""

import json
import sys

import pandas as pd

sys.path.append('../../_')
from lsms_library.local_tools import RecursiveDict, get_dataframe  # noqa: E402
from mali import i as mali_i  # noqa: E402


def _canonical_i(grappe, menage) -> str | None:
    """Wrap mali.i() for the scalar inputs this script deals with."""
    if pd.isna(grappe) or pd.isna(menage):
        return None
    return mali_i(pd.Series([int(grappe), int(menage)], index=['grappe', 'menage']))


# -----------------------------------------------------------------------
# 2018-19 baseline IDs
# -----------------------------------------------------------------------

cover_18 = get_dataframe('../2018-19/Data/s00_me_mli2018.dta')
baseline_ids: set[str] = set()
for _, row in cover_18[['grappe', 'menage']].dropna().drop_duplicates().iterrows():
    i_value = _canonical_i(row['grappe'], row['menage'])
    if i_value is not None:
        baseline_ids.add(i_value)


# -----------------------------------------------------------------------
# 2021-22 panel cover
# -----------------------------------------------------------------------

cover_21 = get_dataframe('../2021-22/Data/s00_me_mli2021.dta')
panel_mask = cover_21['PanelHH'].astype(str).str.strip() == 'Oui'
panel_21 = cover_21[panel_mask].copy()

candidates: list[tuple[str, str]] = []

for _, row in panel_21.iterrows():
    cur_i = _canonical_i(row['grappe'], row['menage'])
    if cur_i is None:
        continue

    has_explicit = pd.notna(row['s00q07f1']) and pd.notna(row['s00q07f2'])
    if has_explicit:
        # Explicit linkage path: use the columns (which may differ from
        # the current grappe/menage for the 11 moved households).
        prev_i = _canonical_i(row['s00q07f1'], row['s00q07f2'])
    else:
        # Implicit linkage path: panel HHs without linkage columns keep
        # the same (grappe, menage) across waves.
        prev_i = cur_i

    if prev_i is None:
        continue
    if prev_i not in baseline_ids:
        # Orphaned: panel flag set, but the previous ID is not in the
        # 2018-19 baseline cover. Drop rather than produce a phantom.
        continue
    candidates.append((cur_i, prev_i))

# Detect + skip transitive-chain collisions. Mali has 11 "moved"
# households where the 2021-22 cover's explicit prev-grappe/prev-menage
# columns differ from the current grappe/menage, and empirically 3 of
# those form 2-step chains like 244008 -> 244009 -> 244010: two
# different current households both claim adjacent 2018-19 slots as
# their previous selves. When id_walk applies these in a single pass
# it rewrites 244008 -> 244009, and a second pass would then rewrite
# that 244009 -> 244010 (collision with the row that was originally
# 244009). We skip the entry whose prev_i is itself another HH's
# cur_i in the same candidate set.
cur_set = {cur for cur, _ in candidates}
ehcvm_21_to_18: dict[str, str] = {}
skipped_chains = 0
for cur, prev in candidates:
    if prev in cur_set and cur != prev:
        # This would form a 2-step chain. Drop rather than let
        # id_walk's second pass collapse the chain into a collision.
        skipped_chains += 1
        continue
    ehcvm_21_to_18[cur] = prev


# -----------------------------------------------------------------------
# Assemble outputs
# -----------------------------------------------------------------------

# Baseline waves get empty maps. EACI waves (2014-15, 2017-18) are
# cross-sectional with respect to EHCVM and get empty maps too;
# id_walk will leave their IDs alone.
updated_ids = {
    '2014-15': {},
    '2017-18': {},
    '2018-19': {},
    '2021-22': ehcvm_21_to_18,
}

recursive_D: RecursiveDict = RecursiveDict()
for cur, prev in ehcvm_21_to_18.items():
    recursive_D[('2021-22', cur)] = ('2018-19', prev)


# -----------------------------------------------------------------------
# Write JSON
# -----------------------------------------------------------------------

with open('panel_ids.json', 'w') as f:
    json_ready = {','.join(k): ','.join(v) for k, v in recursive_D.data.items()}
    json.dump(json_ready, f)

with open('updated_ids.json', 'w') as f:
    json.dump(updated_ids, f)


print(f"EHCVM 2021-22 → 2018-19: {len(ehcvm_21_to_18)} households linked "
      f"({skipped_chains} transitive-chain entries skipped)")
