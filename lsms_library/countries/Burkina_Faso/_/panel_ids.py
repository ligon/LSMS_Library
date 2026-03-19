#!/usr/bin/env python3
"""
Build panel_ids mapping for Burkina Faso.

Panel linkage exists only between the two EHCVM waves (2018-19 and 2021-22).
The 2014 EMC is a different survey program with no panel linkage.

The 2021-22 cover sheet (s00_me_bfa2021.dta) contains 3,227 panel households
with linkage variables:
  - hhid       : 2021-22 household ID (current wave)
  - hhid_EHCVM1: 2018-19 household ID (previous wave)
  - grappe_EHCVM1, menage_EHCVM1: cluster/household from 2018-19

ID construction differs across waves:
  - 2018-19: j = str(grappe) + str(menage).zfill(3)
  - 2021-22: j = str(hhid)

Since hhid_EHCVM1 == str(grappe_EHCVM1) + str(menage_EHCVM1).zfill(3),
we use hhid_EHCVM1 directly as previous_i.
"""

import json
import sys

import pandas as pd
import dvc.api
from ligonlibrary.dataframes import from_dta

sys.path.append('../../_')
from lsms_library.local_tools import panel_ids

# Read the 2021-22 cover sheet (already filtered to panel households)
with dvc.api.open('../2021-22/Data/s00_me_bfa2021.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

# Build current wave ID (2021-22 style)
df['i'] = df['hhid'].astype(int).astype(str)

# Build previous wave ID (2018-19 style: grappe + menage zero-padded to 3)
df['previous_i'] = (df['grappe_EHCVM1'].astype(int).astype(str)
                     + df['menage_EHCVM1'].astype(int).astype(str).str.rjust(3, '0'))

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

with open('panel_ids.json', 'w') as f:
    json_ready = {','.join(k): ','.join(v) for k, v in D.data.items()}
    json.dump(json_ready, f)

with open('updated_ids.json', 'w') as f:
    json.dump(updated_ids, f)
