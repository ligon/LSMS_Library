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

GH #1120: W2/W3 households recorded ONLY in the agriculture cover
(sect_cover_pp_w2/w3) are linked by the same two rules -- see the block
below the W3 household-cover mapping.
"""
import json
from pathlib import Path as _Path
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

# --- GH #1120: households recorded ONLY in the AGRICULTURE cover ---
# sample() takes these households from sect_cover_pp_w{N} (GH #1120 D1), so
# the crosswalk must walk them as it walks the household cover's.  Evidence
# (CONTENTS.org "sample(): the agriculture cover's households"): every one of
# W2's 25 and W3's 27 carries a W1 baseline household_id that IS in the W1
# household cover, in the same EA, and none is already another household's
# target here; 25 of W3's 27 were walked in W2 (from the W2 household cover)
# to exactly the baseline the W3 agriculture cover records, and were left
# unwalked in W3 -- a panel break.  The household-cover rows above are first,
# so no household-cover link is renumbered (no new target collides with an
# existing one; update_id mints no split suffix).  The regenerated JSON ADDS
# 25 W2 and 27 W3 entries (26 linked, 1 identity: no W2 record of any kind),
# and MOVES 12 existing W3 entries from identity to their W1 baseline: those
# households were in W2's agriculture cover only and in W3's household cover,
# so W2's missing link broke their chain.  For all 12 the new target is in the
# W1 household cover, has the same EA, and equals W3's own household_id
# column.  Counts are re-derived in tests/test_gh1120_ethiopia_agriculture_cover.py.
pp2 = get_dataframe('../2013-14/Data/sect_cover_pp_w2.dta')[
    ['household_id2', 'household_id']
]
pp2['household_id2'] = pp2['household_id2'].apply(format_id)
pp2['household_id'] = pp2['household_id'].apply(format_id)
pp2 = pp2[~pp2['household_id2'].isin(set(df2.index.get_level_values('i')))]
pp2 = pp2.drop_duplicates('household_id2')
pp2['t'] = '2013-14'
pp2 = pp2.rename(columns={'household_id2': 'i', 'household_id': 'previous_i'})
df2 = pd.concat([df2, pp2.set_index(['t', 'i'])[['previous_i']]], axis=0)

# W3: the same identity rule as the household cover (the chain resolver walks
# each id back through W2); W3's own household_id column is not trusted, as
# documented above.
pp3 = get_dataframe('../2015-16/Data/sect_cover_pp_w3.dta')[['household_id2']]
pp3['household_id2'] = pp3['household_id2'].apply(format_id)
pp3 = pp3[~pp3['household_id2'].isin(set(df3.index.get_level_values('i')))]
pp3 = pp3.drop_duplicates('household_id2')
pp3['t'] = '2015-16'
pp3['previous_i'] = pp3['household_id2']
pp3 = pp3.rename(columns={'household_id2': 'i'})
df3 = pd.concat([df3, pp3.set_index(['t', 'i'])[['previous_i']]], axis=0)

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
