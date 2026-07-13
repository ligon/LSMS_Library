"""Build sample table for Albania 2004.

Sources:
  - w3_weights.dta: individual-level file with HH-level design weight wt_des
    and household id chid.  wt_des is constant within household.
  - w3_hh_basic.dta: cover page with cluster id m0_q01 and district m0_distr.

No strata or Urban/Rural variables available for this wave.
"""
import sys
from pathlib import Path

import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

sys.path.append(str(Path(__file__).parent.parent.parent / '_'))
from albania import ALBANIA_2004_SENTINEL_PSUS  # noqa: E402

# Get HH-level weight from individual weights file
df_w = get_dataframe('../Data/w3_weights.dta')
hh_w = df_w.drop_duplicates(subset='chid')[['chid', 'wt_des']].copy()
hh_w = hh_w.dropna(subset=['wt_des'])

# Get cluster (v) from cover page
df_c = get_dataframe('../Data/w3_hh_basic.dta')
hh_c = df_c[['chid', 'm0_q01']].copy()

# Merge
hh = hh_w.merge(hh_c, on='chid', how='left')

# GH #323: m0_q01 in {995, 999} are ADMINISTRATIVE sentinels, not clusters --
# 995 = split-off/new households, 999 = original households that moved or could
# not be traced.  None of the 83 keys into 2002's (psu, hh).  Emitting them as
# cluster ids invents two clusters that pool 83 households across 23 districts.
# The households are KEPT (they are real, weighted households); only their
# cluster id is set to <NA>, because we do not know which cluster they belong
# to.  Silently MISSING beats silently WRONG.
psu = hh['m0_q01'].astype('Int64')
v = hh['m0_q01'].apply(format_id)
v = v.where(~psu.isin(ALBANIA_2004_SENTINEL_PSUS), pd.NA)

sample = pd.DataFrame({
    'i': hh['chid'].apply(format_id),
    'v': v,
    'weight': hh['wt_des'].astype(float),
    'panel_weight': hh['wt_des'].astype(float),  # cross-section: same as weight
})

sample = sample.set_index('i')
to_parquet(sample, 'sample.parquet')
