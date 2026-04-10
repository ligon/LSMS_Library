"""Build sample table for Albania 2008.

Sources:
  - Weight_retro_2008.sav: HH-level weight (psu, hh, Weight_retro).
  - poverty.sav: stratum (central/coastal/mountain/tirana) and urbrur (rural/urban).
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

# Weights
df_w = get_dataframe('../Data/Weight_retro_2008.sav')

# Stratum and Urban/Rural from poverty file
df_p = get_dataframe('../Data/poverty.sav')

# Merge on (psu, hh)
df = df_p.merge(df_w, on=['psu', 'hh'], how='left')

sample = pd.DataFrame({
    'i': df[['psu', 'hh']].apply(
        lambda r: format_id(r.iloc[0]) + '-' + format_id(r.iloc[1]), axis=1),
    'v': df['psu'].apply(format_id),
    'weight': df['Weight_retro'].astype(float),
    'panel_weight': df['Weight_retro'].astype(float),  # cross-section: same as weight
    'strata': df['stratum'].astype(str).str.title(),
    'Rural': df['urbrur'].astype(str).str.title(),
})

sample = sample.set_index('i')
to_parquet(sample, 'sample.parquet')
