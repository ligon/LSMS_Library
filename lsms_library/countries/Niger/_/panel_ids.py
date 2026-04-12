#!/usr/bin/env python3
"""Build Niger panel_ids + updated_ids for TWO disjoint program panels.

ECVMA (2011-12 ↔ 2014-15) and EHCVM (2018-19 ↔ 2021-22) are separate
survey programs with no household overlap. This script emits panel
linkage for each program independently, bypassing
`local_tools.panel_ids()` to avoid cross-program chaining (which would
otherwise try to link 2021-22 back through 2014-15 when 2018-19 is
absent from the wave aggregation).

**ECVMA linkage is implicit.** ECVMA-II (2014-15) re-uses the same
``(grappe, menage)`` numbering as ECVMA-I (2011-12) for re-visited
households. The 2011-12 household ID is a scalar ``hid = grappe*100 +
menage``. The 2014-15 household ID uses ``niger.i()``, and the
2014-15 household roster declares ``idxvars: i: [GRAPPE, MENAGE]``
(without EXTENSION), so the roster-level current ID is
``format_id(grappe) + '0' + format_id(menage, zp=2)`` — e.g. ``'1001'``
for ``(g=1, m=1)``. We link a 2014-15 household to its 2011-12
counterpart when the reconstructed hid exists in 2011-12. Extension 2
(split-off) cover rows collapse with their parent in the roster (they
share the same ``(grappe, menage)``), so the linkage is effectively
at the ``(grappe, menage)`` level.

Empirical counts (2026-04): 3557 unique ``(GRAPPE, MENAGE)`` pairs in
the 2014-15 cover, ~3534 of which have a matching 2011-12 hid.

**EHCVM linkage is explicit.** The 2021-22 cover sheet
``s00_me_ner2021`` has ``s00q07f1`` (previous grappe) and
``s00q07f2`` (previous menage) columns plus a ``PanelHH`` flag. We
link when ``PanelHH == 'Ménage panel'`` AND the previous linkage
columns are present AND the reconstructed previous ID exists in the
2018-19 baseline roster. Empirically 4589 households satisfy all
three conditions and in every case ``(cur_grappe, cur_menage) ==
(prev_grappe, prev_menage)`` — so the 2021-22 → 2018-19 mapping is
effectively identity.

Output: ``panel_ids.json`` (RecursiveDict: (wave, id) → (prev_wave,
prev_id)) and ``updated_ids.json`` ({wave: {current_id: canonical_id}}).
Baseline waves (2011-12, 2018-19) appear in ``updated_ids`` with empty
maps so that ``id_walk()`` knows to leave their IDs alone.

Known diagnostic false positives
--------------------------------
``check_panel_consistency(Country('Niger'))`` emits two warnings
that are benign for disjoint-program countries like Niger:

1. ``updated_ids_cover_waves: No ID mappings for waves: ['2018-19']``
   — the check assumes every wave after the first is a follow-up,
   but 2018-19 is a fresh baseline of a second program (EHCVM), not
   a follow-up of ECVMA-II.
2. ``ids_self_consistent: 2021-22: 4589 self-referential ID(s)`` —
   every EHCVM panel household has ``(cur_grappe, cur_menage) ==
   (prev_grappe, prev_menage)``, so the rewrite is identity. The
   check flags these as potential bugs, but they are factual.

Do not try to "fix" these by inserting identity mappings into
2011-12/2018-19 or by removing the 2021-22 entries — the current
state is the correct one, and the 2021-22 entries are retained for
auditability (they document which households are explicitly flagged
as panel households by the survey's ``PanelHH`` variable).
"""
import json
import sys

import pandas as pd

sys.path.append('../../_')
from lsms_library.local_tools import RecursiveDict, get_dataframe  # noqa: E402
from niger import i as niger_i  # noqa: E402


# -----------------------------------------------------------------------
# ECVMA panel: 2011-12 ↔ 2014-15
# -----------------------------------------------------------------------

# 2011-12 baseline: hid is a scalar hid = grappe*100 + menage.  We read
# the individual file (ecvmaind_p1p2_en.dta) since the household
# identifier lives there as `hid`.
df11 = get_dataframe(
    '../2011-12/Data/NER_2011_ECVMA_v01_M_Stata8/ecvmaind_p1p2_en.dta'
)
hid11_set = {str(int(h)) for h in df11['hid'].dropna().unique()}

# 2014-15 cover sheet
cover14 = get_dataframe(
    '../2014-15/Data/NER_2014_ECVMA-II_v02_M_STATA8/ECVMA2_MS00P1.dta'
)

ecvma_14_to_11: dict[str, str] = {}
# Iterate over unique (GRAPPE, MENAGE) pairs. The 2014-15 roster
# declares idxvars as [GRAPPE, MENAGE] only (extensions are collapsed
# with their parent), so the linkage is at the (grappe, menage) level.
uniq_gm = cover14[['GRAPPE', 'MENAGE']].dropna().drop_duplicates()

# First pass: build the set of 2014-15 current IDs (post-niger.i).
# We need this to detect transitive-chain collisions in the second pass.
cur_ids_14: set[str] = set()
for _, row in uniq_gm.iterrows():
    g, m = int(row['GRAPPE']), int(row['MENAGE'])
    cur_i = niger_i(pd.Series([g, m], index=['GRAPPE', 'MENAGE']))
    if cur_i is not None:
        cur_ids_14.add(cur_i)

# Second pass: build the linkage, skipping transitive-chain collisions.
# niger.i(g, m) = format_id(g) + '0' + format_id(m, zp=2) and the
# theoretical 2011-12 hid = g*100+m share a namespace: e.g.
# (g=10, m=2) gives cur_i='10002' and prev_i='1002', and (g=1, m=2)
# gives cur_i='1002'. If we add both linkages, updated_ids['2014-15']
# would form a transitive chain '10002'->'1002'->'102' — and id_walk's
# single-pass .replace() applied twice would collapse both households
# into the single canonical '102', destroying one of them. We skip
# any linkage whose prev_i also appears as another HH's cur_i.
for _, row in uniq_gm.iterrows():
    g, m = int(row['GRAPPE']), int(row['MENAGE'])
    cur_i = niger_i(pd.Series([g, m], index=['GRAPPE', 'MENAGE']))
    if cur_i is None:
        continue
    prev_i = str(g * 100 + m)
    if prev_i not in hid11_set:
        continue
    if prev_i in cur_ids_14:
        # prev_i collides with another HH's cur_i — ambiguous. Leave
        # this household unlinked rather than produce a phantom chain.
        continue
    ecvma_14_to_11[cur_i] = prev_i


# -----------------------------------------------------------------------
# EHCVM panel: 2018-19 ↔ 2021-22
# -----------------------------------------------------------------------

# 2018-19 baseline IDs: derive from the individual file's unique
# (grappe, menage) pairs, passed through niger.i() to get the canonical
# 'E_...' form.
df18 = get_dataframe('../2018-19/Data/ehcvm_individu_ner2018.dta')
pairs18 = df18[['grappe', 'menage']].dropna().drop_duplicates()
ehcvm_18_set: set[str] = set()
for _, row in pairs18.iterrows():
    i_value = niger_i(
        pd.Series(
            [int(row['grappe']), int(row['menage'])],
            index=['grappe', 'menage'],
        )
    )
    if i_value is not None:
        ehcvm_18_set.add(i_value)

# 2021-22 cover sheet
cover21 = get_dataframe('../2021-22/Data/s00_me_ner2021.dta')
panel_mask = cover21['PanelHH'].astype(str).str.strip() == 'Ménage panel'
linked_mask = (
    panel_mask
    & cover21['s00q07f1'].notna()
    & cover21['s00q07f2'].notna()
)
panel21 = cover21[linked_mask]

ehcvm_21_to_18: dict[str, str] = {}
for _, row in panel21.iterrows():
    g = int(row['grappe'])
    m = int(row['menage'])
    pg = int(row['s00q07f1'])
    pm = int(row['s00q07f2'])
    cur_i = niger_i(pd.Series([g, m], index=['grappe', 'menage']))
    prev_i = niger_i(pd.Series([pg, pm], index=['grappe', 'menage']))
    if cur_i is None or prev_i is None:
        continue
    if prev_i in ehcvm_18_set:
        ehcvm_21_to_18[cur_i] = prev_i


# -----------------------------------------------------------------------
# Assemble outputs
# -----------------------------------------------------------------------

# updated_ids: one mapping per wave.  Baseline waves get empty dicts so
# id_walk() leaves their IDs alone.  Non-baseline waves get
# {current_id: canonical_id}, where canonical_id is the baseline's ID.
updated_ids = {
    '2011-12': {},
    '2014-15': ecvma_14_to_11,
    '2018-19': {},
    '2021-22': ehcvm_21_to_18,
}

# RecursiveDict: two disjoint chains, one per program.
recursive_D: RecursiveDict = RecursiveDict()
for cur, prev in ecvma_14_to_11.items():
    recursive_D[('2014-15', cur)] = ('2011-12', prev)
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


print(f"ECVMA 2014-15 → 2011-12: {len(ecvma_14_to_11)} households linked")
print(f"EHCVM 2021-22 → 2018-19: {len(ehcvm_21_to_18)} households linked")
