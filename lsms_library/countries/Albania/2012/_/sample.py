"""Build sample table for Albania 2012.

Sources:
  - Weight_lsms2012_retro.sav: HH-level weight (psu, hh, pesha10tetor).
  - poverty.sav: region (Central/Coastal/Mountains/Tirana) and urban (Rural/Urban).
    Also has hhid (globally unique HH id).
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

# Weights
df_w = get_dataframe('../Data/Weight_lsms2012_retro.sav')

# Stratum and Urban/Rural from poverty file
df_p = get_dataframe('../Data/poverty.sav')

# Merge on (psu, hh)
df = df_p.merge(df_w, on=['psu', 'hh'], how='left')

sample = pd.DataFrame({
    'i': df['hhid'].apply(format_id),
    'v': df['psu'].apply(format_id),
    'weight': df['pesha10tetor'].astype(float),
    'panel_weight': df['pesha10tetor'].astype(float),  # cross-section: same as weight
    'strata': df['region'].astype(str).replace('Mountains', 'Mountain'),
    'Rural': df['urban'].astype(str),
})

sample = sample.set_index('i')
to_parquet(sample, 'sample.parquet')
