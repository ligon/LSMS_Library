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
menage``. The 2014-15 household ID is built by ``niger.i()`` from the
FULL ECVMA-II household key ``(GRAPPE, MENAGE, EXTENSION)`` (GH #323),
giving ``format_id(grappe) + '0' + format_id(menage, zp=2) +
str(extension)`` — e.g. ``'10010'`` for ``(g=1, m=1, ext=0)``.

**Split-offs do NOT inherit the parent's panel link — and this is load
bearing.** 59 ``(grappe, menage)`` pairs host TWO distinct 2014-15
households (extension compositions: 57 x (0, 2), 2 x (1, 2)). All
households at a given ``(grappe, menage)`` descend from the SAME
2011-12 household ``hid = grappe*100 + menage``, so a naive linkage
would map two distinct current IDs onto ONE canonical baseline ID.
``updated_ids`` is an identity-REWRITE map consumed by ``id_walk()``:
mapping two households to the same canonical ID MERGES them, silently
destroying one — which is precisely the GH #323 collapse this wave's
key fix removes, reintroduced one layer up. The rewrite map must
therefore be INJECTIVE.

So exactly one household per ``(grappe, menage)`` may inherit the
baseline identity: the BASE household, i.e. the one with the lowest
EXTENSION present (normally 0). Households with a higher EXTENSION are
post-baseline split-offs — genuinely NEW households — and are left
UNLINKED (new entrants) rather than merged into their parent. Leaving
them unlinked is class-2 (silently missing from the panel); merging
them would be class-1 (silently wrong), and class-2 is strictly safer.

This is enforced, not merely described: ``_assert_injective()`` below
raises if any two current IDs ever map to the same canonical ID.

Empirical counts (2026-07, GH #323): 3617 households in the 2014-15
cover across 3558 ``(GRAPPE, MENAGE)`` pairs; 3537 base households link
to a 2011-12 hid; 59 split-offs are deliberately left unlinked.

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

def _assert_injective(mapping: dict[str, str], label: str) -> None:
    """A rewrite map MUST be injective.

    ``updated_ids`` is consumed by ``id_walk()``, which REWRITES each
    current household ID to its canonical (baseline) ID.  If two distinct
    households share a canonical ID they become one household, and the
    framework's duplicate-index collapse (_normalize_dataframe_index's
    groupby().first()) then silently discards one of them -- the GH #323
    failure mode.  This assertion is the enforcement: prose in a docstring
    is not.
    """
    seen: dict[str, str] = {}
    for cur, prev in mapping.items():
        if prev in seen:
            raise AssertionError(
                f'{label}: NON-INJECTIVE panel rewrite -- {seen[prev]!r} and '
                f'{cur!r} both map to canonical {prev!r}.  id_walk() would '
                f'MERGE these two distinct households (GH #323).'
            )
        seen[prev] = cur


# The full ECVMA-II household key is the TRIPLE (GRAPPE, MENAGE, EXTENSION);
# 59 (GRAPPE, MENAGE) pairs host two distinct households.
uniq_gme = (cover14[['GRAPPE', 'MENAGE', 'EXTENSION']]
            .dropna().drop_duplicates())

# The BASE household at each (grappe, menage) is the one with the lowest
# EXTENSION present (normally 0).  Only it inherits the 2011-12 identity;
# higher extensions are post-baseline split-offs -- new households, left
# unlinked.  See the module docstring for why this is not optional.
base_ext = uniq_gme.groupby(['GRAPPE', 'MENAGE'], observed=True)['EXTENSION'].transform('min')
uniq_gme = uniq_gme.assign(is_base=uniq_gme['EXTENSION'] == base_ext)


def _cur_id(g: int, m: int, e: int) -> str | None:
    return niger_i(pd.Series([g, m, e], index=['GRAPPE', 'MENAGE', 'EXTENSION']))


# First pass: every 2014-15 current ID (post-niger.i), used to detect
# transitive-chain collisions in the second pass.
cur_ids_14: set[str] = set()
for _, row in uniq_gme.iterrows():
    cur_i = _cur_id(int(row['GRAPPE']), int(row['MENAGE']), int(row['EXTENSION']))
    if cur_i is not None:
        cur_ids_14.add(cur_i)

# Second pass: build the linkage.
# niger.i() and the theoretical 2011-12 hid = g*100+m share a digit
# namespace, so a prev_i can coincide with some OTHER household's cur_i.
# Adding both linkages would form a transitive chain (cur -> prev -> prev')
# and id_walk's single-pass .replace() applied twice would collapse two
# households into one.  Skip any linkage whose prev_i is also a cur_i.
ecvma_14_to_11: dict[str, str] = {}
n_splitoff_unlinked = 0
n_chain_skipped = 0
for _, row in uniq_gme.iterrows():
    g, m, e = int(row['GRAPPE']), int(row['MENAGE']), int(row['EXTENSION'])
    cur_i = _cur_id(g, m, e)
    if cur_i is None:
        continue
    prev_i = str(g * 100 + m)
    if prev_i not in hid11_set:
        continue
    if not row['is_base']:
        # Split-off household.  Its parent (the base household at this same
        # (grappe, menage)) already claims prev_i; linking this one too would
        # make the rewrite non-injective and MERGE the two.  It is a new
        # household -- leave it unlinked.
        n_splitoff_unlinked += 1
        continue
    if prev_i in cur_ids_14:
        # prev_i collides with another HH's cur_i -- ambiguous. Leave this
        # household unlinked rather than produce a phantom chain.
        n_chain_skipped += 1
        continue
    ecvma_14_to_11[cur_i] = prev_i

_assert_injective(ecvma_14_to_11, 'ECVMA 2014-15 -> 2011-12')


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

_assert_injective(ehcvm_21_to_18, 'EHCVM 2021-22 -> 2018-19')


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
print(f"  split-off households left UNLINKED (new entrants, GH #323): "
      f"{n_splitoff_unlinked}")
print(f"  linkages skipped to avoid a transitive ID chain: {n_chain_skipped}")
print(f"EHCVM 2021-22 → 2018-19: {len(ehcvm_21_to_18)} households linked")
