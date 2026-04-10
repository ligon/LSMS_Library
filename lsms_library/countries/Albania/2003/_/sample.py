"""Build sample table for Albania 2003.

Source: w2_ind_all.dta — individual-level file with HH-level design weight WT_DES,
PSU, and STRATA (numeric 1-16).  No Urban/Rural variable available in wave 2 data.
WT_DES is constant within household (BHID).
"""
import pandas as pd
from lsms_library.local_tools import get_dataframe, format_id, to_parquet

df = get_dataframe('../Data/w2_ind_all.dta')

# Take one row per household (WT_DES is constant within HH)
hh = df.drop_duplicates(subset='BHID').copy()

# Drop households with missing weight (non-response)
hh = hh.dropna(subset=['WT_DES'])

sample = pd.DataFrame({
    'i': hh['BHID'].apply(format_id),
    'v': hh['PSU'].apply(format_id),
    'weight': hh['WT_DES'].astype(float),
    'panel_weight': hh['WT_DES'].astype(float),  # cross-section: same as weight
    'strata': hh['STRATA'].apply(format_id),
})

sample = sample.set_index('i')
to_parquet(sample, 'sample.parquet')
