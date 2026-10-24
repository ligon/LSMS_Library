#!/usr/bin/env python3
"""
Build panel_ids mapping for Burkina Faso.

Panel linkage exists only between the two EHCVM waves (2018-19 and 2021-22).
The 2014 EMC is a different survey program with no panel linkage.

The 2021-22 cover sheet (s00_me_bfa2021.dta) contains 3,227 panel households
with linkage variables:
  - hhid       : 2021-22 household ID (current wave)
  - hhid_EHCVM1: 2018-19 household ID (previous wave), in the SURVEY's format
  - grappe_EHCVM1, menage_EHCVM1: cluster/household from 2018-19

THE LIBRARY'S HOUSEHOLD ID DIFFERS BY WAVE (GH #460, #1240).  Each wave's
tables build ``i`` from ``(grappe, menage)`` with a different formatter:

  - 2018-19: ``2018-19/_/mapping.py:i`` == ``burkina_faso.ehcvm_i``
             = format_id(grappe) + '0' + format_id(menage, zeropadding=2)
  - 2021-22: no wave override, so the country-level ``burkina_faso.i``
             = format_id(grappe) + format_id(menage, zeropadding=3)

The two agree for menage <= 99 and diverge for a three-digit menage
(grappe=2, menage=126: ``'20126'`` in 2018-19, ``'2126'`` in 2021-22).

The survey's own ``hhid_EHCVM1`` (and ``hhid``) are in the 2021-22 form,
``grappe + menage.zfill(3)``, for all 3,227 rows.  So ``hhid_EHCVM1`` is NOT
the id the 2018-19 tables serve, and neither is any 2018-19 id built with
``zfill(3)``.  Until GH #1240 this script did exactly that: 1,108 of the 3,227
households have a three-digit 2018-19 menage, and their targets were ids that
exist in no 2018-19 table (1,088) or, worse, the id of a DIFFERENT 2018-19
household (3: target ``'90102'`` for 2018-19 household (90, 102), which is the
served id of (9, 102)); the other 17 were chain-skipped.

Both ids are therefore built here with the country's own formatters -- the
target with ``ehcvm_i``, the source with ``i`` (byte-identical to ``hhid`` for
all 3,227 rows) -- and every target is checked against the 2018-19 cover's
``ehcvm_i`` ids, as Mali's and Senegal's ``panel_ids.py`` check theirs.
"""

import json
from pathlib import Path as _Path
import sys

import pandas as pd

sys.path.append('../../_')
from lsms_library.local_tools import panel_ids, get_dataframe
# The script's own directory (``_/``) is sys.path[0] when run as
# ``python panel_ids.py``; Mali's script imports ``from mali import i`` the
# same way.  Reuse the country's formatters rather than re-implementing them.
from burkina_faso import ehcvm_i, i as country_i, cross_section_i  # noqa: E402


def _id_2018_19(grappe, menage):
    """2018-19 household id, as ``2018-19/_/mapping.py:i`` builds it."""
    if pd.isna(grappe) or pd.isna(menage):
        return None
    return ehcvm_i(int(grappe), int(menage))


def _id_2021_22(grappe, menage):
    """2021-22 household id, as the country-level ``i`` builds it."""
    if pd.isna(grappe) or pd.isna(menage):
        return None
    return country_i(pd.Series([int(grappe), int(menage)]))


# 2018-19 baseline ids, in the 2018-19 tables' format.  A target outside this
# set would be a phantom (Mali / Senegal precedent); dropped and counted.
cover_18 = get_dataframe('../2018-19/Data/s00_me_bfa2018.dta')
baseline_ids = {
    _id_2018_19(g, m)
    for g, m in cover_18[['grappe', 'menage']].dropna().drop_duplicates()
    .itertuples(index=False)
}

# Read the 2021-22 cover sheets.  The round has TWO components (GH #1276):
# Data/Panel/ (WB 6224, 3,227 panel households, all with a non-null
# grappe_EHCVM1) and Data/Cross_Section/ (WB 6277, 7,176 households of a new
# frame, no 2018-19 antecedent and no linkage columns).  Only the panel cover
# yields links; BOTH covers are "occupied" 2021-22 ids for the collision guard
# below -- the Mali GH #536 rule: the occupied set is EVERY 2021-22 household,
# not just the panel-linked ones.
df = get_dataframe('../2021-22/Data/Panel/s00_me_bfa2021.dta', convert_categoricals=True)
cross = get_dataframe('../2021-22/Data/Cross_Section/s00_me_bfa2021.dta',
                      convert_categoricals=True)
# The cross-section's ids, as its tables serve them: namespaced 'CS_' (GH
# #1244; data_info.yml's per-file `cs_i`).  Unprefixed, 9 of them equal a
# 2018-19 id and 2 equal a panel household's crosswalk target -- a rename
# onto a household that keeps its id, i.e. a merge.  With the prefix neither
# can happen, and the asserts below say so rather than assume it.
cross_ids = {cross_section_i(g, m) for g, m in
             cross[['grappe', 'menage']].itertuples(index=False)}
cross_ids.discard(None)
assert len(cross_ids) == len(cross), "cross-section (grappe, menage) not unique"
assert not (cross_ids & baseline_ids), "a cross-section id equals a 2018-19 id"

# Current wave ID, in the 2021-22 tables' format.  The survey's hhid carries
# the same digits; assert it so a future release that changes either fails
# here rather than producing sources no 2021-22 table serves.
df['i'] = [_id_2021_22(g, m) for g, m in
           df[['grappe', 'menage']].itertuples(index=False)]
_hhid = df['hhid'].astype(int).astype(str)
assert (df['i'] == _hhid).all(), (
    "2021-22 country i(grappe, menage) no longer equals hhid for "
    f"{int((df['i'] != _hhid).sum())} rows")

# Previous wave ID, in the 2018-19 tables' format, left null for any household
# without a 2018-19 antecedent.  Such rows are dropped inside panel_ids(); they
# still count as *occupied* 2021-22 slots for the collision guard below.
_prev = [_id_2018_19(g, m) for g, m in
         df[['grappe_EHCVM1', 'menage_EHCVM1']].itertuples(index=False)]
orphans = sum(p is not None and p not in baseline_ids for p in _prev)
df['previous_i'] = pd.Series(
    [p if p in baseline_ids else pd.NA for p in _prev],
    index=df.index, dtype=object)

df['t'] = '2021-22'

# Build the panel_ids DataFrame expected by local_tools.panel_ids()
panel_df = df[['t', 'i', 'previous_i']].drop_duplicates().set_index(['t', 'i'])

# Include a dummy 2018-19 entry so that panel_ids() recognises two waves.
# Without this, sorted_waves=['2021-22'] and the RecursiveDict stays empty.
# The 2018-19 row has NaN previous_i and is dropped inside the function.
dummy = pd.DataFrame({'previous_i': [pd.NA]}, index=pd.MultiIndex.from_tuples(
    [('2018-19', '__dummy__')], names=['t', 'i']))
panel_df = pd.concat([dummy, panel_df])

D, updated_ids = panel_ids(panel_df)

# No chain guard (GH #1245).  The 2021-22 ids and the 2018-19 ids are built
# from (grappe, menage) in one numeric-string namespace, and 2021-22 renumbered
# menage within the grappe (only 477 of 3,227 keep their 2018-19 menage), so a
# household's 2018-19 id can be ANOTHER 2021-22 household's current id: 4021
# -> 4065, where 4065 is itself a 2021-22 household that moves on.  80 of the
# 3,227 entries are such links.  While id_walk closure-resolved a wave's map,
# {A: B, B: C} served both A and B as C, so this script dropped all 80 (and 8
# of those households were then served under a DIFFERENT 2018-19 household's
# id, GH #1240).  id_walk is now a single-pass rename -- every id to its own
# target -- and refuses to walk a wave twice, so every entry is kept.
#
# What a single pass CANNOT survive is a target held by a 2021-22 household
# that keeps its id: renaming onto it would merge two households.  That is
# impossible today (every 2021-22 household is a panel household, and every
# occupied target's occupant moves on), so assert it rather than drop links
# silently: a future cover with refreshment households fails here, at
# regeneration, instead of colliding at read time.  The "occupied" set is the
# FULL cover, not just panel-linked rows (GH #548 / #536).
cur_set = set(df['i'].dropna()) | cross_ids
assert not (set(df['i'].dropna()) & cross_ids), (
    "a panel household's 2021-22 id equals a cross-section household's")
_w21 = updated_ids.get('2021-22', {})
_held = sorted(prev for cur, prev in _w21.items()
               if prev != cur and prev in cur_set and _w21.get(prev, prev) == prev)
assert not _held, (
    f"{len(_held)} 2021-22 crosswalk target(s) are held by a 2021-22 household "
    f"that keeps its id; a single-pass rename would merge them: {_held[:5]}")
chained = sum(1 for cur, prev in _w21.items() if prev != cur and prev in cur_set)

# GH #914: write beside THIS script, never into the process's cwd.  These two
# files are committed sources; `make panel-ids` regenerates them in place for a
# maintainer to review as a diff.  Resolving off __file__ means a maintainer who
# runs the script from somewhere other than `_/` updates the tracked files
# rather than scattering copies.
_HERE = _Path(__file__).resolve().parent

with open(_HERE / 'panel_ids.json', 'w') as f:
    json_ready = {','.join(k): ','.join(v) for k, v in D.data.items()}
    json.dump(json_ready, f)

with open(_HERE / 'updated_ids.json', 'w') as f:
    json.dump(updated_ids, f)

print(f"Burkina Faso 2021-22 -> 2018-19: {len(_w21)} linked "
      f"({chained} onto an id another 2021-22 household moves off, "
      f"{orphans} targets absent from the 2018-19 cover; "
      f"{len(cross_ids)} cross-section households, none linked, all occupied)")
