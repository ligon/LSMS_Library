"""Build sample table for Albania 2005.

Sources:
  - identification_cl.dta: cover page with m0_q00 (PSU), m0_q01 (HH within PSU),
    m0_stratum (Central/Coastal/Mountain/Tirana), m0_ur (Rural/Urban).
  - weights_cl.dta: PSU-level weights with m0_q00 and weight.
    All HHs in the same PSU share the same weight.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

# Cover page: HH-level with stratum, urban/rural, cluster
df_id = get_dataframe('../Data/identification_cl.dta')

# PSU-level weights
df_wt = get_dataframe('../Data/weights_cl.dta')

# Merge weights onto cover page by PSU (m0_q00)
df = df_id.merge(df_wt, on='m0_q00', how='left')

sample = pd.DataFrame({
    'i': df[['m0_q00', 'm0_q01']].apply(
        lambda r: format_id(r.iloc[0]) + '-' + format_id(r.iloc[1]), axis=1),
    'v': df['m0_q00'].apply(format_id),
    'weight': df['weight'].astype(float),
    'panel_weight': df['weight'].astype(float),  # cross-section: same as weight
    'strata': df['m0_stratum'].astype(str),
    'Rural': df['m0_ur'].astype(str),
})

sample = sample.set_index('i')
to_parquet(sample, 'sample.parquet')
