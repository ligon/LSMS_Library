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
menage``. The 2014-15 household ID uses ``niger.i()`` on the FULL
ECVMA2 key ``(GRAPPE, MENAGE, EXTENSION)`` -- every 2014-15 table
declares ``idxvars: i: [GRAPPE, MENAGE, EXTENSION]`` since GH #679 --
so the current ID is ``format_id(grappe) + '0' + format_id(menage, zp=2)
+ str(extension)``, e.g. ``'10010'`` for ``(g=1, m=1, e=0)``.

``EXTENSION`` says what became of the 2011 household (ECVMA2 cover
sheet): 0 = found in place; 1 = the WHOLE household, found after
moving; 2 = a SPLIT-OFF -- members who left the 2011 household and were
interviewed as a household of their own.  Measured on the cover sheet,
a ``(GRAPPE, MENAGE)`` carries the extension sets ``{0}`` (3,421),
``{1}`` (75), ``{0, 2}`` (57), ``{1, 2}`` (2) or ``{2}`` (3) -- never
``{0, 1}``, so extensions 0 and 1 are never two 2014-15 households.

**DECISION (GH #679): extensions 0 and 1 link to the 2011-12 parent;
extension 2 does NOT.**  A split-off is a new household in the panel
sense -- its members were enumerated in 2011 inside the parent, but the
2014-15 record is the first observation of THIS household.  Linking it
to the parent's hid would make ``id_walk`` rename both the parent and
the split-off to the same 2011-12 id and re-merge them -- the very
collision #679 removes, under another name.  A user who wants the
split-off's origin can recover it from the key itself: the parent is
the same ``(GRAPPE, MENAGE)`` with extension 0 or 1.

Empirical counts (2026-09-24): 3,617 ``(GRAPPE, MENAGE, EXTENSION)``
households in the 2014-15 cover (3,478 / 77 / 62 by extension);
3,529 linked to a 2011-12 hid (3,457 ext-0 + 77 ext-1, minus 5 skipped
by the namespace guard below); 88 unlinked (62 split-offs, 21 ext-0
households whose reconstructed hid is not in the 2011-12 individual
file, 5 guard-skipped).  Under the previous two-column key the guard
had to skip 286 households, so only 3,251 were linked.

Five of those 3,251 old links are NOT in the new crosswalk (measured
2026-10-14 against the merge-base crosswalk; 3,246 carry over with
identical targets, 0 change target, the other three waves' maps are
byte-identical).  Three go by the DECISION above -- old keys
``89005``, ``95001``, ``128011``, pairs (89,5) / (95,1) / (128,11),
are ext-2-ONLY on the cover sheet (EXTENSION set {2}, no ext-0/1
sibling): split-offs whose parent was not re-interviewed, for which
the collision argument does not bite; delinking them is a uniform
"split-off = new household" choice, reversible by admitting ext 2 for
pairs with no ext-0/1 sibling.  Two go by the namespace guard below --
old keys ``101010`` and ``201010`` (ext-0 households (101,10) and
(201,10)) reconstruct hids ``10110`` / ``20110``, which under the
three-column key are the ids of (1,11,0) / (2,11,0).  The other three
guard-skipped households (prev_i ``10010``, ``20010``, ``20012``) were
skipped under the old key too.

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
from pathlib import Path as _Path
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
# Iterate over unique (GRAPPE, MENAGE, EXTENSION) triples -- the full ECVMA2
# household key, which every 2014-15 table now declares (GH #679).
KEY14 = ['GRAPPE', 'MENAGE', 'EXTENSION']
uniq_gme = cover14[KEY14].dropna().drop_duplicates()

# Extensions that link to the 2011-12 parent.  0 = found in place, 1 = the
# whole household found after moving.  2 = SPLIT-OFF, deliberately NOT here:
# see the module docstring (DECISION, GH #679).
LINKED_EXTENSIONS = {0, 1}

# First pass: build the set of 2014-15 current IDs (post-niger.i).
# We need this to detect transitive-chain collisions in the second pass.
cur_ids_14: set[str] = set()
for _, row in uniq_gme.iterrows():
    g, m, e = int(row['GRAPPE']), int(row['MENAGE']), int(row['EXTENSION'])
    cur_i = niger_i(pd.Series([g, m, e], index=KEY14))
    if cur_i is not None:
        cur_ids_14.add(cur_i)

# Second pass: build the linkage, skipping transitive-chain collisions.
# niger.i(g, m, e) = format_id(g) + '0' + format_id(m, zp=2) + str(e) and
# the 2011-12 hid = g*100+m are both digit strings and share a namespace:
# (g=1, m=1, e=0) gives cur_i='10010', which is also the hid of the 2011-12
# household (g=100, m=10).  If (100, 10, 0) were linked to '10010',
# updated_ids['2014-15'] would hold the chain '1000100'->'10010'->'101' and
# id_walk (closure-resolved, local_tools._close_id_map) would send BOTH
# 2014-15 households to '101'.  We skip any linkage whose prev_i is also
# some household's cur_i.  Measured: 5 households skipped under the
# three-column key ('10010', '10110', '20010', '20012', '20110' as prev_i);
# under the old two-column key this guard skipped 286.
#
# The guard cannot cover the OTHER direction -- an UNLINKED 2014-15 id that
# equals a 2011-12 hid stays as it is and reads as that 2011-12 household
# in any cross-wave join.  Measured: exactly one, the split-off
# (g=2, m=1, e=2) = '20012', which is also hid 20012 = (g=200, m=12).
# A digit-only id format cannot exclude this; recorded in CONTENTS.org.
for _, row in uniq_gme.iterrows():
    g, m, e = int(row['GRAPPE']), int(row['MENAGE']), int(row['EXTENSION'])
    if e not in LINKED_EXTENSIONS:
        continue
    cur_i = niger_i(pd.Series([g, m, e], index=KEY14))
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

# GH #914: write beside THIS script, never into the process's cwd.  These two
# files are committed sources; `make panel-ids` regenerates them in place for a
# maintainer to review as a diff.  Resolving off __file__ means a maintainer who
# runs the script from somewhere other than `_/` updates the tracked files
# rather than scattering copies.
_HERE = _Path(__file__).resolve().parent

with open(_HERE / 'panel_ids.json', 'w') as f:
    json_ready = {','.join(k): ','.join(v) for k, v in recursive_D.data.items()}
    json.dump(json_ready, f)

with open(_HERE / 'updated_ids.json', 'w') as f:
    json.dump(updated_ids, f)


print(f"ECVMA 2014-15 → 2011-12: {len(ecvma_14_to_11)} households linked")
print(f"EHCVM 2021-22 → 2018-19: {len(ehcvm_21_to_18)} households linked")
