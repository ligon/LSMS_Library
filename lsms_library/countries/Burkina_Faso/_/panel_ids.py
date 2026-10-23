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
from burkina_faso import ehcvm_i, i as country_i  # noqa: E402


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

# Read the 2021-22 cover sheet.  Today every row is a panel household
# (3,227 rows, all with a non-null grappe_EHCVM1), but do not *assume* that:
# see the full-cover cur_set below (GH #548).
df = get_dataframe('../2021-22/Data/s00_me_bfa2021.dta', convert_categoricals=True)

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

# Skip transitive-chain collisions.  The 2021-22 ids and the 2018-19 ids are
# built from (grappe, menage) in one numeric-string namespace, and 2021-22
# renumbered menage within the grappe (only 477 of 3,227 keep their 2018-19
# menage), so a household's 2018-19 id can be ANOTHER 2021-22 household's
# current id (e.g. 4021 -> 4065, where 4065 is also a 2021-22 id).
# id_walk closure-resolves each wave's map (local_tools._close_id_map), so
# {A: B, B: C} would serve both A and B as C.  Drop every entry whose target
# is an occupied 2021-22 id: 80 of 3,227 entries since GH #1240 (95 before,
# when the targets were in the wrong format).  Caught by
# check_panel_consistency's id_walk_idempotent check (diagnostics.py).
#
# A dropped household keeps its 2021-22 id, which can coincide with the id of
# a DIFFERENT 2018-19 household (8 of the 80 since GH #1240): for these two
# waves an ``i`` match on an unlinked household is not evidence that it is the
# same household.  See CONTENTS.org (GH #1240).
#
# GH #548 (hardening; byte-identical today): the "occupied" set must be
# EVERY 2021-22 household id, not just the panel-linked candidates.  Built
# from updated_ids.keys() it saw only panel households -- the exact
# panel-only pattern GH #536 had to fix in Mali, where a household renaming
# onto a slot held by a NON-panel current household collided under id_walk
# and groupby().first() silently dropped one of each pair.  Burkina's
# 2021-22 cover happens to be 100% panel (3,227/3,227), so panel-only and
# full-cover coincide *today* and this edit changes nothing; a future cover
# with refreshment households would otherwise resurface the Mali bug
# silently.  Build cur_set from the full cover.
cur_set = set(df['i'].dropna())
filtered_21 = {}
skipped_chains = 0
for cur, prev in updated_ids.get('2021-22', {}).items():
    if prev in cur_set and cur != prev:
        skipped_chains += 1
        continue
    filtered_21[cur] = prev
updated_ids['2021-22'] = filtered_21

# Rebuild the RecursiveDict with the skipped entries removed.
from lsms_library.local_tools import RecursiveDict  # noqa: E402
D_filtered = RecursiveDict()
for k, v in D.data.items():
    # k = (wave, cur_id), v = (prev_wave, prev_id)
    if k[0] == '2021-22' and k[1] not in filtered_21:
        continue
    D_filtered[k] = v
D = D_filtered

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

print(f"Burkina Faso 2021-22 -> 2018-19: {len(filtered_21)} linked "
      f"({skipped_chains} transitive-chain entries skipped, "
      f"{orphans} targets absent from the 2018-19 cover)")
