#!/usr/bin/env python
"""Extract sampling design variables for Ethiopia ESS Wave 4 (2018-19).

Source: sect_cover_hh_w4.dta (HH cover page)
Weight: pw_w4 (cross-sectional, one weight only)
Cluster: ea_id
Strata: constructed from saq01 (region) x saq14 (rural/urban)
Rural: saq14 mapped to Rural/Urban
"""
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, format_id, to_parquet
import pandas as pd

df = get_dataframe('../Data/sect_cover_hh_w4.dta')

# Rural mapping: wave 4 uses saq14 with 'RURAL'/'URBAN'
rural_map = {
    'rural': 'Rural',
    'Rural': 'Rural',
    'RURAL': 'Rural',
    'urban': 'Urban',
    'Urban': 'Urban',
    'URBAN': 'Urban',
}

sample = pd.DataFrame({
    'i': df['household_id'].apply(format_id),
    'v': df['ea_id'].apply(format_id),
    'weight': df['pw_w4'].astype(float),
    'Rural': df['saq14'].map(rural_map),
})

# Construct strata from region x Rural
region = df['saq01'].astype(str).str.strip()
sample['strata'] = region + ' ' + sample['Rural'].fillna('')
sample['strata'] = sample['strata'].str.strip()

# Panel weight = cross-sectional weight (single weight)
sample['panel_weight'] = sample['weight']

sample['t'] = '2018-19'
sample = sample.set_index(['i', 't'])

to_parquet(sample, 'sample.parquet')
