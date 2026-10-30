#!/usr/bin/env python3
"""Build panel_ids for Ethiopia ESS W1-W3.

W2 (2013-14) cover file maps household_id2 (18-char, W2 native)
to household_id (14-char, W1 baseline).  100% of non-empty links
resolve to W1 roster IDs.

W3 (2015-16) re-uses the same household_id2 values as W2 for
continuing households (98.6% overlap).  W3's household_id column
is *supposed* to be a W1 baseline link, but 25% of entries point
at IDs that don't exist in W1 -- fabricated IDs for W2 refreshment
households that continued into W3.

Fix: map W3 household_id2 to itself (identity) and let the chain
resolver in panel_ids() walk back through W2 to reach W1.  This
gives correct linkage for all three categories:
  - W1 panel HH (in all three waves): chain resolves to W1 ID
  - W2 refreshment (in W2+W3 only): keeps household_id2 as canonical
  - W3-only new HH: keeps household_id2 as canonical

W4 (2018-19) and W5 (2021-22) draw entirely new samples with no
backward link.
"""
import json
from pathlib import Path as _Path
import sys

import pandas as pd

sys.path.append('../../_')
sys.path.append(str(_Path(__file__).resolve().parent))
from lsms_library.local_tools import format_id, get_dataframe, panel_ids

from ethiopia import _agriculture_only_households, ea_id2_crosswalk

# D1: HH remains primary; selected holder rows must agree before reduction.
# W3 PP household_id is deliberately not linkage authority (ledger §4).
hh1 = get_dataframe('../2011-12/Data/sect_cover_hh_w1.dta', convert_categoricals=False)
pp1 = get_dataframe('../2011-12/Data/sect_cover_pp_w1.dta', convert_categoricals=False)
hh2 = get_dataframe('../2013-14/Data/sect_cover_hh_w2.dta', convert_categoricals=False)
pp2 = get_dataframe('../2013-14/Data/sect_cover_pp_w2.dta', convert_categoricals=False)
hh3 = get_dataframe('../2015-16/Data/sect_cover_hh_w3.dta', convert_categoricals=False)
pp3 = get_dataframe('../2015-16/Data/sect_cover_pp_w3.dta', convert_categoricals=False)
xw = ea_id2_crosswalk(hh2, hh3)
a1 = _agriculture_only_households('2011-12', hh1, pp1,
    dict(hhid='household_id', ea='ea_id'), [])
a2 = _agriculture_only_households('2013-14', hh2, pp2,
    dict(hhid='household_id2', ea='ea_id2'), [])
a3 = _agriculture_only_households('2015-16', hh3, pp3,
    dict(hhid='household_id2', ea='ea_id2', pp_ea='ea_id'), [], ea_crosswalk=xw)

baseline = dict(zip(hh1.household_id.map(format_id), hh1.ea_id.map(format_id)))
if a1 is not None:
    baseline.update(zip(a1.i, a1.v))
if a2 is not None:
    for row in a2.itertuples():
        if row.household_id == '':
            raise ValueError('W2 selected baseline is formatted empty; unresolved authority')
        if row.household_id is not None:
            if row.household_id not in baseline:
                raise ValueError('W2 selected baseline absent from HH/PP baseline union')
            if baseline[row.household_id] != row.ea_id:
                raise ValueError('W2 selected baseline EA disagreement')

df2 = hh2[['household_id2', 'household_id']].map(format_id)
df2 = df2.rename(columns={'household_id2': 'i', 'household_id': 'previous_i'})
if df2.previous_i.eq('').any():
    raise ValueError('W2 HH baseline is formatted empty; unresolved authority')
if a2 is not None:
    extra = a2[['i', 'household_id']].rename(columns={'household_id': 'previous_i'})
    df2 = pd.concat([df2, extra], ignore_index=True)
df2['t'] = '2013-14'
df2 = df2.set_index(['t', 'i'])[['previous_i']]

df3 = pd.DataFrame({'i': hh3.household_id2.map(format_id)})
if a3 is not None:
    df3 = pd.concat([df3, a3[['i']]], ignore_index=True)
df3['t'] = '2015-16'
df3['previous_i'] = df3['i']
df3 = df3.set_index(['t', 'i'])[['previous_i']]

# Refuse collisions before the general resolver can choose a split suffix.
w2 = df2.previous_i.droplevel('t').dropna().to_dict()
w3 = {i: w2.get(i, i) for i in df3.index.get_level_values('i')}
for frame, mapping in ((df2, w2), (df3, w3)):
    ids = frame.index.get_level_values('i')
    if not ids.is_unique or ids.isna().any() or (ids == '').any():
        raise ValueError('Missing or duplicate native panel household key')
    if len(mapping) != len(set(mapping.values())):
        raise ValueError('Panel target collision requires source review')
    staying = {i for i in ids if mapping.get(i, i) == i}
    if any(i != target and target in staying for i, target in mapping.items()):
        raise ValueError('Panel rename onto occupied target requires source review')
if a3 is not None:
    for row in a3.itertuples():
        if row.i in w2:
            if w2[row.i] not in baseline:
                raise ValueError('W3 linked baseline absent from HH/PP baseline union')
            # Crosswalk is not inverted: check the selected source EA directly.
            old = pp3.loc[pp3.household_id2.map(format_id).eq(row.i), 'ea_id'].map(format_id)
            if not old.eq(baseline[w2[row.i]]).all():
                raise ValueError('W3 linked baseline EA disagreement')

panel_ids_df = pd.concat([df2, df3], axis=0)
D, updated_ids = panel_ids(panel_ids_df)

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
