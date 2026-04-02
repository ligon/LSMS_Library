#!/usr/bin/env python3
"""Build panel IDs for Tanzania using head-tracking for household splits.

This script bypasses the generic local_tools.panel_ids() function because
Tanzania needs bespoke handling:

1. The 2008-15 UPD has no household splits (UPHI maps 1:1 per round).
2. The 2019-20 and 2020-21 waves have ~200 splits each.  When a household
   divides, the split containing the original head (hh_b06 == 1) inherits
   the canonical ID.  Split-offs are new households starting from that wave.
3. 2020-21 links to 2014-15 (refresh panel), not to 2019-20 (extended panel).
"""
import json
import sys
import pandas as pd
import dvc.api

sys.path.append('../../_')
from lsms_library.local_tools import format_id, RecursiveDict
from ligonlibrary.dataframes import from_dta
from tanzania import map_08_15, _map_with_head_tracking

# --- Phase 1: Build raw linkage per wave ---

# 2008-15: UPHI-based with composite IDs for household splits (#114)
with dvc.api.open('../2008-15/Data/upd4_hh_a.dta', mode='rb') as dta:
    cover_08 = from_dta(dta)
linkage_08 = map_08_15(cover_08[['r_hhid', 'round', 'UPHI']], ['r_hhid', 'round', 'UPHI'])

# 2019-20: head-tracking for splits
with dvc.api.open('../2019-20/Data/HH_SEC_A.dta', mode='rb') as dta:
    cover_19 = from_dta(dta)
with dvc.api.open('../2019-20/Data/HH_SEC_B.dta', mode='rb') as dta:
    roster_19 = from_dta(dta)
linkage_19 = _map_with_head_tracking(
    cover_19, roster_19, 'sdd_hhid', 'y4_hhid')
linkage_19['t'] = '2019-20'

# 2020-21: head-tracking for splits
with dvc.api.open('../2020-21/Data/hh_sec_a.dta', mode='rb') as dta:
    cover_20 = from_dta(dta)
with dvc.api.open('../2020-21/Data/hh_sec_b.dta', mode='rb') as dta:
    roster_20 = from_dta(dta)
linkage_20 = _map_with_head_tracking(
    cover_20, roster_20, 'y5_hhid', 'y4_hhid')
linkage_20['t'] = '2020-21'


# --- Phase 2: Build updated_ids by chaining backward to canonical IDs ---
# We do this ourselves instead of calling local_tools.panel_ids() to avoid
# the update_id() function which creates phantom retroactive entries.

all_linkage = pd.concat([
    linkage_08.reset_index(),
    linkage_19,
    linkage_20,
])

sorted_waves = sorted(all_linkage['t'].unique())
updated_ids = {}
recursive_D = RecursiveDict()

for wave in sorted_waves:
    wave_df = all_linkage[all_linkage['t'] == wave][['i', 'previous_i']].dropna()
    wave_matches = dict(zip(wave_df['i'], wave_df['previous_i']))

    # Determine the previous wave for chaining
    idx = sorted_waves.index(wave)
    if wave == '2020-21':
        prev_wave = '2014-15'  # refresh panel links to round 4
    elif idx > 0:
        prev_wave = sorted_waves[idx - 1]
    else:
        prev_wave = None

    if prev_wave and prev_wave in updated_ids:
        prev_matches = updated_ids[prev_wave]
        # Chase each previous_i back to its canonical form
        wave_matches = {k: prev_matches.get(v, v)
                        for k, v in wave_matches.items()}
        recursive_D.update({(wave, k): (prev_wave, v)
                            for k, v in wave_matches.items()})

    # Store as-is — no update_id(), no phantom creation
    updated_ids[wave] = wave_matches


# --- Phase 3: Write JSON outputs ---

with open('panel_ids.json', 'w') as f:
    json_ready = {','.join(k): ','.join(v) for k, v in recursive_D.data.items()}
    json.dump(json_ready, f)

with open('updated_ids.json', 'w') as f:
    json.dump(updated_ids, f)
