#!/usr/bin/env python
"""Extract sampling design variables for Ethiopia ESS Wave 5 (2021-22).

Source: sect_cover_hh_w5.dta (HH cover page), plus the households
        recorded ONLY in sect_cover_pp_w5.dta (agriculture cover; GH #1120 D1)
Weight: pw_w5 (cross-sectional, one weight only)
Cluster: ea_id
Strata: constructed from saq01 (region) x saq14 (rural/urban)
Rural: saq14 mapped to Rural/Urban
"""
import sys
sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, format_id, to_parquet
import pandas as pd
from ethiopia import with_agriculture_cover_households

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

# GH #1120 D1 (@ligon, 2026-10-23): households the survey recorded ONLY in
# the agriculture (post-planting) cover enter the sample from that cover,
# with that cover's weight.  See ethiopia.with_agriculture_cover_households
# and CONTENTS.org "sample(): the agriculture cover's households".
# W5: 40 households, 29 of them in 3 EAs with no household-cover row.
hh_codes = get_dataframe('../Data/sect_cover_hh_w5.dta', convert_categoricals=False)
pp = get_dataframe('../Data/sect_cover_pp_w5.dta', convert_categoricals=False)
n0 = len(sample)
sample = with_agriculture_cover_households(
    sample, '2021-22', hh_codes, df, pp,
    dict(hhid='household_id', ea='ea_id', weight='pw_w5', rural='saq14'),
    rural_map)
sample = sample.set_index(['i', 't'])

to_parquet(sample, 'sample.parquet')
