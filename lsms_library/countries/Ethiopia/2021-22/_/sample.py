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
sys.path.append('../../_/')
from ethiopia import with_agriculture_cover_households, ea_id2_crosswalk

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

# D1: append recorded agriculture-only households; HH rows retain precedence.
hh_codes = get_dataframe('../Data/sect_cover_hh_w5.dta', convert_categoricals=False)
pp = get_dataframe('../Data/sect_cover_pp_w5.dta', convert_categoricals=False)
sample = with_agriculture_cover_households(
    sample, '2021-22', hh_codes, df, pp,
    {'hhid': 'household_id', 'ea': 'ea_id', 'weight': 'pw_w5', 'rural': 'saq14'}, rural_map, ea_crosswalk=None)
sample = sample.set_index(['i', 't'])

to_parquet(sample, 'sample.parquet')
