"""Build sample table for Albania 2004.

Sources:
  - w3_weights.dta: individual-level file with HH-level design weight wt_des
    and household id chid.  wt_des is constant within household.
  - w3_hh_basic.dta: cover page with cluster id m0_q01 and district m0_distr.

No strata or Urban/Rural variables available for this wave.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

# Get HH-level weight from individual weights file
df_w = get_dataframe('../Data/w3_weights.dta')
hh_w = df_w.drop_duplicates(subset='chid')[['chid', 'wt_des']].copy()
hh_w = hh_w.dropna(subset=['wt_des'])

# Get cluster (v) from cover page
df_c = get_dataframe('../Data/w3_hh_basic.dta')
hh_c = df_c[['chid', 'm0_q01']].copy()

# Merge
hh = hh_w.merge(hh_c, on='chid', how='left')

sample = pd.DataFrame({
    'i': hh['chid'].apply(format_id),
    'v': hh['m0_q01'].apply(format_id),
    'weight': hh['wt_des'].astype(float),
    'panel_weight': hh['wt_des'].astype(float),  # cross-section: same as weight
})

sample = sample.set_index('i')
to_parquet(sample, 'sample.parquet')
