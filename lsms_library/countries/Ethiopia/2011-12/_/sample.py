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
sample = sample.set_index(['i', 't'])

to_parquet(sample, 'sample.parquet')
