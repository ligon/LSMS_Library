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
import sys

import pandas as pd

sys.path.append('../../_')
from lsms_library.local_tools import format_id, get_dataframe, panel_ids

# --- W2: household_id2 -> household_id (W1 baseline) ---
df2 = get_dataframe('../2013-14/Data/sect_cover_hh_w2.dta')[
    ['household_id2', 'household_id']
]
df2['household_id2'] = df2['household_id2'].apply(format_id)
df2['household_id'] = df2['household_id'].apply(format_id)
df2['t'] = '2013-14'
df2 = df2.rename(columns={'household_id2': 'i', 'household_id': 'previous_i'})
df2 = df2.set_index(['t', 'i'])[['previous_i']]

# --- W3: identity mapping on household_id2 ---
# The chain resolver in panel_ids() looks up each W3 household_id2
# in W2's mapping, walking it back to W1 when the link exists.
df3 = get_dataframe('../2015-16/Data/sect_cover_hh_w3.dta')[['household_id2']]
df3['household_id2'] = df3['household_id2'].apply(format_id)
df3['t'] = '2015-16'
df3['previous_i'] = df3['household_id2']  # identity -- chain does the work
df3 = df3.rename(columns={'household_id2': 'i'})
df3 = df3.set_index(['t', 'i'])[['previous_i']]

panel_ids_df = pd.concat([df2, df3], axis=0)
D, updated_ids = panel_ids(panel_ids_df)

with open('panel_ids.json', 'w') as f:
    json_ready = {','.join(k): ','.join(v) for k, v in D.data.items()}
    json.dump(json_ready, f)

with open('updated_ids.json', 'w') as f:
    json.dump(updated_ids, f)
