#!/usr/bin/env python
"""Extract sampling design variables for Ethiopia ESS Wave 5 (2021-22).

Source: sect_cover_hh_w5.dta (HH cover page)
Weight: pw_w5 (cross-sectional, one weight only)
Cluster: ea_id
Strata: constructed from saq01 (region) x saq14 (rural/urban)
Rural: saq14 mapped to Rural/Urban
"""
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, format_id, to_parquet
import pandas as pd

df = get_dataframe('../Data/sect_cover_hh_w5.dta')

# Rural mapping: wave 5 uses saq14 with 'RURAL'/'URBAN'
# Also handles '1. RURAL' / '2. URBAN' variants
rural_map = {
    'rural': 'Rural',
    'Rural': 'Rural',
    'RURAL': 'Rural',
    '1. RURAL': 'Rural',
    'urban': 'Urban',
    'Urban': 'Urban',
    'URBAN': 'Urban',
    '2. URBAN': 'Urban',
}

sample = pd.DataFrame({
    'i': df['household_id'].apply(format_id),
    'v': df['ea_id'].apply(format_id),
    'weight': df['pw_w5'].astype(float),
    'Rural': df['saq14'].map(rural_map),
})

# Construct strata from region x Rural
region = df['saq01'].astype(str).str.strip()
sample['strata'] = region + ' ' + sample['Rural'].fillna('')
sample['strata'] = sample['strata'].str.strip()

# Panel weight = cross-sectional weight (single weight)
sample['panel_weight'] = sample['weight']

sample['t'] = '2021-22'
sample = sample.set_index(['i', 't'])

to_parquet(sample, 'sample.parquet')
