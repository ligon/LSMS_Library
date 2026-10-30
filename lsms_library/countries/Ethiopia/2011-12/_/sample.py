#!/usr/bin/env python
"""Extract sampling design variables for Ethiopia ESS Wave 1 (2011-12).

Source: sect_cover_hh_w1.dta (HH cover page)
Weight: pw (cross-sectional, one weight only)
Cluster: ea_id
Strata: constructed from saq01 (region) x rural
Rural: rural column mapped to Rural/Urban
"""
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, format_id, to_parquet
import pandas as pd
sys.path.append('../../_/')
from ethiopia import with_agriculture_cover_households, ea_id2_crosswalk

df = get_dataframe('../Data/sect_cover_hh_w1.dta')

# Rural mapping: wave 1 has 'rural' and 'small town'
rural_map = {
    'rural': 'Rural',
    'Rural': 'Rural',
    'RURAL': 'Rural',
    'small town': 'Urban',
    'Small town': 'Urban',
    'Small Town': 'Urban',
    'SMALL TOWN': 'Urban',
    'urban': 'Urban',
    'Urban': 'Urban',
    'URBAN': 'Urban',
}

sample = pd.DataFrame({
    'i': df['household_id'].apply(format_id),
    'v': df['ea_id'].apply(format_id),
    'weight': df['pw'].astype(float),
    'Rural': df['rural'].map(rural_map),
})

# Construct strata from region x Rural
region = df['saq01'].astype(str).str.strip()
sample['strata'] = region + ' ' + sample['Rural'].fillna('')
sample['strata'] = sample['strata'].str.strip()

# Panel weight = cross-sectional weight (single weight, pre-rotating panel)
sample['panel_weight'] = sample['weight']

sample['t'] = '2011-12'

# D1: append recorded agriculture-only households; HH rows retain precedence.
hh_codes = get_dataframe('../Data/sect_cover_hh_w1.dta', convert_categoricals=False)
pp = get_dataframe('../Data/sect_cover_pp_w1.dta', convert_categoricals=False)
sample = with_agriculture_cover_households(
    sample, '2011-12', hh_codes, df, pp,
    {'hhid': 'household_id', 'ea': 'ea_id', 'weight': 'pw', 'rural': 'rural'}, rural_map, ea_crosswalk=None)
sample = sample.set_index(['i', 't'])

to_parquet(sample, 'sample.parquet')
