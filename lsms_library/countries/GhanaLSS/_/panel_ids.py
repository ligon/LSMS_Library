#!/usr/bin/env python3
"""Build GhanaLSS panel_ids for the GLSS1 ↔ GLSS2 panel (1987-88 ↔ 1988-89).

GhanaLSS has seven survey rounds spanning 1987-88 through 2016-17, but
**only the first two (GLSS1 and GLSS2) are panel-linked**. Subsequent
rounds used different cluster schemes (2xxxx → 3xxxx → 4xxxx → …) and
have no household-level linkage.

The panel linkage file is ``PANELC.DAT`` in the 1988-89 Data directory.
It is **person-level** (3370 rows) with columns:

- ``HID1`` / ``PID1``: 1987-88 household + person ID
- ``HID2`` / ``PID2``: 1988-89 household + person ID
- ``CLUST``: cluster identifier

Because it is person-level it records household **splits**: a single GLSS1
household whose members ended up in *two* GLSS2 households.  There are two
such splits (716 unique ``(HID2, HID1)`` pairs over 714 current households):

===========  =========================================================
GLSS1 HID1   GLSS2 HID2
===========  =========================================================
101332       204932 (persons 1, 2, 4) and 204922 (person 3)
114008       255728 (11 persons)      and 255718 (person 11)
===========  =========================================================

GH #548 / GH #504 — why this script routes through ``local_tools.panel_ids``
---------------------------------------------------------------------------
The previous version built the rename map with a bare
``dict(zip(cur_i, prev_i))``.  That silently encodes a **many-to-one** map:
*both* split-off households are told to rename onto the same 1987-88 id.
``id_walk`` duly renames both, and
``Country._normalize_dataframe_index``'s ``groupby(...).first()`` /
additive ``sum()`` then collapses the two distinct households into one —
**inside** ``load_from_waves``, i.e. before the L2-country parquet is
written, so the loss is baked into the cache and warm reads are silent.
Measured damage: household 101332's 1988-89 food expenditure was 15,850
(= 5,570 for HH 204932 **plus** 10,280 for HH 204922 — two different
households' food summed into one) and its roster lost 4 of 10 persons.

The library already has the guard: ``local_tools.update_id`` mints the
``_N`` split suffix (``101332`` / ``101332_1``) — the same convention
Malawi's IHPS panel uses — and since GH #504 refuses to mint a suffix that
collides with a live id.  We reach it by handing a ``(t, i) -> previous_i``
frame to ``local_tools.panel_ids()``, exactly as Burkina Faso does, instead
of hand-rolling the maps.  ``panel_ids()`` needs a prior wave in the frame
to build the ``RecursiveDict`` chain at all, so a dummy 1987-88 row with a
null ``previous_i`` is prepended; it is dropped inside the function.

The second, complementary guard (GH #536, Mali) is applied here too: skip a
linkage whose ``previous_i`` is itself a **live current-wave household id**
(rename-onto-occupied).  Zero GhanaLSS entries qualify — GLSS1 ids are
1xxxxx and GLSS2 ids are 2xxxxx — but the two guards cover different
failure modes and a future wave should not have to rediscover that.
"""

import json
import sys

import pandas as pd

sys.path.append('../../_')
from lsms_library.local_tools import (  # noqa: E402
    format_id,
    get_dataframe,
    panel_ids as build_panel_ids,
)

CUR_WAVE = '1988-89'
PREV_WAVE = '1987-88'

# All seven rounds.  Only the first two are panel-linked; the rest ship
# empty maps.
WAVES = [
    '1987-88', '1988-89', '1991-92', '1998-99',
    '2005-06', '2012-13', '2016-17',
]


# -----------------------------------------------------------------------
# Load the panel linkage file
# -----------------------------------------------------------------------

panel_df = get_dataframe(f'../{CUR_WAVE}/Data/PANELC.DAT')

# Household-level (HID2 -> HID1) pairs.  716 unique pairs / 714 unique
# HID2: the surplus is the two splits documented above (one HID1 -> two
# HID2) plus two merges (two HID1 -> one HID2, HID2 in {204933, 249301}).
# A merged household has two ancestors and the id model can keep only one;
# last-seen wins, which is what the shipped map already encodes.
hh_map = (
    panel_df[['HID2', 'HID1']]
    .drop_duplicates()
    .dropna()
)
hh_map['cur_i'] = hh_map['HID2'].apply(format_id)
hh_map['prev_i'] = hh_map['HID1'].apply(format_id)
hh_map = hh_map.dropna(subset=['cur_i', 'prev_i'])


# -----------------------------------------------------------------------
# Guard 1 (GH #536): never rename onto an *occupied* current-wave slot
# -----------------------------------------------------------------------

# The "occupied" set must be EVERY 1988-89 household id, not just the
# panel-linked ones (that panel-only mistake is precisely what #536 fixed
# in Mali).  Y00A.DAT is the 1988-89 cover page — the same file the wave's
# ``sample`` table is built from (1988-89/_/data_info.yml).
cover = get_dataframe(f'../{CUR_WAVE}/Data/Y00A.DAT')
cur_set = {format_id(h) for h in cover['HID']}
cur_set.discard(None)

kept: list[tuple[str, str]] = []
skipped_onto_occupied = 0
for cur, prev in zip(hh_map['cur_i'], hh_map['prev_i']):
    if prev in cur_set and cur != prev:
        # Renaming ``cur`` -> ``prev`` would land on a household that is
        # itself alive in this wave: id_walk's single-pass rename would
        # produce a duplicate (i, t) and groupby().first() would drop one.
        # Forgo the cross-wave link rather than lose a household.
        skipped_onto_occupied += 1
        continue
    kept.append((cur, prev))


# -----------------------------------------------------------------------
# Guard 2 (GH #504/#548): split households get the ``_N`` suffix
# -----------------------------------------------------------------------

# Hand the linkage to the framework as a (t, i) -> previous_i frame.  The
# dummy PREV_WAVE row (null previous_i, dropped inside panel_ids()) is what
# makes ``sorted_waves`` two-long so the RecursiveDict chain is built.
linkage_df = pd.DataFrame(
    {'previous_i': [prev for _, prev in kept]},
    index=pd.MultiIndex.from_tuples(
        [(CUR_WAVE, cur) for cur, _ in kept], names=['t', 'i']
    ),
)
dummy = pd.DataFrame(
    {'previous_i': [pd.NA]},
    index=pd.MultiIndex.from_tuples([(PREV_WAVE, '__dummy__')], names=['t', 'i']),
)
recursive_D, updated_wave = build_panel_ids(pd.concat([dummy, linkage_df]))

linkage: dict[str, str] = updated_wave.get(CUR_WAVE, {})

# Only 1987-88 and 1988-89 participate; every other round is
# cross-sectional and gets an empty map.
updated_ids: dict[str, dict[str, str]] = {w: {} for w in WAVES}
updated_ids[CUR_WAVE] = linkage


# -----------------------------------------------------------------------
# Post-conditions.  A violation here is a *class-1* defect (a silently
# wrong number downstream); raise instead of writing a bad map.
# -----------------------------------------------------------------------

# (a) The rename map must be injective: two live households must never end
#     up sharing one canonical id (that is the #548 collapse).
targets = list(linkage.values())
if len(set(targets)) != len(targets):
    dupes = sorted({t for t in targets if targets.count(t) > 1})
    raise ValueError(
        f'{CUR_WAVE}: rename map is not injective; {len(dupes)} target id(s) '
        f'claimed by more than one household: {dupes[:5]}'
    )

# (b) No canonical id may collide with a household that is alive in this
#     wave and is *not* the one being renamed (rename-onto-occupied).
collisions = sorted(
    {v for k, v in linkage.items() if v in cur_set and v != k}
)
if collisions:
    raise ValueError(
        f'{CUR_WAVE}: {len(collisions)} rename target(s) collide with live '
        f'{CUR_WAVE} household ids: {collisions[:5]}'
    )


# -----------------------------------------------------------------------
# Write JSON
# -----------------------------------------------------------------------

with open('panel_ids.json', 'w') as f:
    json_ready = {','.join(k): ','.join(v) for k, v in recursive_D.data.items()}
    json.dump(json_ready, f)

with open('updated_ids.json', 'w') as f:
    json.dump(updated_ids, f)


n_split = sum(1 for v in linkage.values() if '_' in v)
print(
    f'GLSS1 ↔ GLSS2: {len(linkage)} households linked ({PREV_WAVE} → {CUR_WAVE}); '
    f'{n_split} split-off household(s) suffixed _N; '
    f'{skipped_onto_occupied} rename-onto-occupied entr(y|ies) skipped'
)
