"""Build sample table for Albania 2002.

Source: weights.dta — contains psu, hh, weight, stratum, ur.
Stratum takes 4 values: Central, Coastal, Mountain, Tirana.
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

df = get_dataframe('../Data/weights.dta')

sample = pd.DataFrame({
    'i': df[['psu', 'hh']].apply(lambda r: format_id(r.iloc[0]) + '-' + format_id(r.iloc[1]), axis=1),
    'v': df['psu'].apply(format_id),
    'weight': df['weight'],
    'panel_weight': df['weight'],  # cross-section: same as weight
    'strata': df['stratum'].astype(str),
    'Rural': df['ur'].astype(str),
})

sample = sample.set_index('i')
to_parquet(sample, 'sample.parquet')
