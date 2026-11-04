#!/usr/bin/env python3
"""Build panel_ids for Ethiopia: Panel I (W1-W3) and Panel II (W4-W5).

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

W3 households that W2's map does not hold (GH #1467, ruling 2026-11-04)
are the one place W3 reaches W1 without W2.  The ESS3 BID counts ESS1
households through its own rural plus small-town total: "the total number
of ESS1 households with complete interviews in ESS3 is 3,699" (PDF p16).
So a rural or small-town W3 household off W2's map is linked to W1 by
rule: its W1 id is ``household_id2`` less characters 7-10, which is W3's
own ``household_id`` and is the target of every W3 link through W2.  Large
towns entered at ESS2 and are never linked this way, including the three
whose truncated id happens to be a W1 household.  This rests on the BID's
accounting, NOT on the rosters, which only weakly agree: see
``CONTENTS.org``, "W3 households the survey counts as ESS1".

W1 (2011-12) and W4 (2018-19) are the two panels' baselines: every row
has a missing ``previous_i``, so the resolver records nothing for them and
nothing crosses from W4 to W3.  ESS4 "is not a follow-up of the previous
ESS waves" (WB catalog 3823).

W5 (2021-22) keeps W4's ids, so its links are identities and move no served
id (GH #1467).  A W5 household links where its id is on W4's household or
agriculture cover: 4,906, the ESPS-5 BID's "balanced panel" (PDF p43).  The
other 93 are the BID's "new EAs" (8 of them, PDF p32), drawn from a fresh
listing in EAs ESPS-4 selected and could not survey (PDF p30-31).
"""
import json
from pathlib import Path as _Path
import sys

import pandas as pd

sys.path.append('../../_')
sys.path.append(str(_Path(__file__).resolve().parent))
from lsms_library.local_tools import format_id, get_dataframe, panel_ids

from ethiopia import _agriculture_only_households, ea_id2_crosswalk

# W3's `rural`: 1 RURAL, 2 SMALL TOWN, 3 "Meduim and large town" (sic).
ESS1_LOCALITIES = (1, 2)


def w1_id(household_id2):
    """A Panel I household's W1 id: ``household_id2`` less characters 7-10."""
    return household_id2[:6] + household_id2[10:]


# D1: HH remains primary; selected holder rows must agree before reduction.
# W3 PP household_id is deliberately not linkage authority (ledger §4).
hh1 = get_dataframe('../2011-12/Data/sect_cover_hh_w1.dta', convert_categoricals=False)
pp1 = get_dataframe('../2011-12/Data/sect_cover_pp_w1.dta', convert_categoricals=False)
hh2 = get_dataframe('../2013-14/Data/sect_cover_hh_w2.dta', convert_categoricals=False)
pp2 = get_dataframe('../2013-14/Data/sect_cover_pp_w2.dta', convert_categoricals=False)
hh3 = get_dataframe('../2015-16/Data/sect_cover_hh_w3.dta', convert_categoricals=False)
pp3 = get_dataframe('../2015-16/Data/sect_cover_pp_w3.dta', convert_categoricals=False)
hh4 = get_dataframe('../2018-19/Data/sect_cover_hh_w4.dta', convert_categoricals=False)
pp4 = get_dataframe('../2018-19/Data/sect_cover_pp_w4.dta', convert_categoricals=False)
hh5 = get_dataframe('../2021-22/Data/sect_cover_hh_w5.dta', convert_categoricals=False)
pp5 = get_dataframe('../2021-22/Data/sect_cover_pp_w5.dta', convert_categoricals=False)
xw = ea_id2_crosswalk(hh2, hh3)
a1 = _agriculture_only_households('2011-12', hh1, pp1,
    dict(hhid='household_id', ea='ea_id'), [])
a2 = _agriculture_only_households('2013-14', hh2, pp2,
    dict(hhid='household_id2', ea='ea_id2'), [])
a3 = _agriculture_only_households('2015-16', hh3, pp3,
    dict(hhid='household_id2', ea='ea_id2', pp_ea='ea_id'), [], ea_crosswalk=xw)
a4 = _agriculture_only_households('2018-19', hh4, pp4,
    dict(hhid='household_id', ea='ea_id'), [])
a5 = _agriculture_only_households('2021-22', hh5, pp5,
    dict(hhid='household_id', ea='ea_id'), [])

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

df1 = pd.DataFrame({'i': list(baseline), 't': '2011-12', 'previous_i': None})
df1 = df1.set_index(['t', 'i'])[['previous_i']]

df2 = hh2[['household_id2', 'household_id']].map(format_id)
df2 = df2.rename(columns={'household_id2': 'i', 'household_id': 'previous_i'})
if df2.previous_i.eq('').any():
    raise ValueError('W2 HH baseline is formatted empty; unresolved authority')
if a2 is not None:
    extra = a2[['i', 'household_id']].rename(columns={'household_id': 'previous_i'})
    df2 = pd.concat([df2, extra], ignore_index=True)
df2['t'] = '2013-14'
df2 = df2.set_index(['t', 'i'])[['previous_i']]

w2 = df2.previous_i.droplevel('t').dropna().to_dict()

# W3's locality and W1-space EA, one row per household: the household cover,
# then the agriculture cover for the households only it holds.
w3_source = pd.DataFrame({'i': hh3.household_id2.map(format_id).values,
                          'rural': hh3.rural.values,
                          'ea_id': hh3.ea_id.map(format_id).values})
if a3 is not None:
    holders = pd.DataFrame({'i': pp3.household_id2.map(format_id).values,
                            'rural': pp3.rural.values,
                            'ea_id': pp3.ea_id.map(format_id).values})
    holders = holders[holders.i.isin(a3.i)]
    if holders.groupby('i')[['rural', 'ea_id']].nunique(dropna=False).gt(1).any(axis=None):
        raise ValueError('W3 agriculture-only holder rows disagree on locality or EA')
    w3_source = pd.concat([w3_source, holders.drop_duplicates('i')], ignore_index=True)
if w3_source.rural.isna().any():
    raise ValueError('W3 household without a locality; the ESS1 rule cannot place it')

# The rule rests on this: a W3 household on W2's map has its truncated id as target.
on_w2 = w3_source.i.isin(w2)
if not w3_source.i[on_w2].map(lambda i: w2[i] == w1_id(i)).all():
    raise ValueError('W3 link through W2 is not the truncated id; the ESS1 rule fails')

ess1 = w3_source[~on_w2 & w3_source.rural.isin(ESS1_LOCALITIES)]
for row in ess1.itertuples():
    if baseline.get(w1_id(row.i)) != row.ea_id:
        raise ValueError('ESS1 rule target is not a W1 household of the recorded EA')
ess1_target = {i: w1_id(i) for i in ess1.i}

df3 = pd.DataFrame({'i': w3_source.i})
df3['t'] = '2015-16'
df3['previous_i'] = df3['i'].map(lambda i: ess1_target.get(i, i))
df3 = df3.set_index(['t', 'i'])[['previous_i']]

# Panel II: household cover, then the households only the agriculture cover holds.
ids4 = pd.concat([hh4.household_id.map(format_id), a4.i], ignore_index=True)
ids5 = pd.concat([hh5.household_id.map(format_id), a5.i], ignore_index=True)
df4 = pd.DataFrame({'i': ids4, 't': '2018-19', 'previous_i': None})
df4 = df4.set_index(['t', 'i'])[['previous_i']]
df5 = pd.DataFrame({'i': ids5, 't': '2021-22'})
df5['previous_i'] = df5['i'].where(df5['i'].isin(ids4))
df5 = df5.set_index(['t', 'i'])[['previous_i']]

# Refuse collisions before the general resolver can choose a split suffix.
w3 = {i: w2.get(p, p) for i, p in df3.previous_i.droplevel('t').items()}
w5 = df5.previous_i.droplevel('t').dropna().to_dict()
for frame, mapping in ((df1, {}), (df2, w2), (df3, w3), (df4, {}), (df5, w5)):
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

panel_ids_df = pd.concat([df1, df2, df3, df4, df5], axis=0)
D, updated_ids = panel_ids(panel_ids_df)

# GH #914: write beside THIS script, never into the process's cwd.  These two
# files are committed sources; `make panel-ids` regenerates them in place for a
# maintainer to review as a diff.  Resolving off __file__ means a maintainer who
# runs the script from somewhere other than `_/` updates the tracked files
# rather than scattering copies.
_HERE = _Path(__file__).resolve().parent

# Under a name other than __main__ (the regeneration test) nothing is written.
if __name__ == '__main__':
    with open(_HERE / 'panel_ids.json', 'w') as f:
        json_ready = {','.join(k): ','.join(v) for k, v in D.data.items()}
        json.dump(json_ready, f)

    with open(_HERE / 'updated_ids.json', 'w') as f:
        json.dump(updated_ids, f)
