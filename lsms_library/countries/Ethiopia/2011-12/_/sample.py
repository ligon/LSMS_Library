#!/usr/bin/env python
"""Extract sampling design variables for Ethiopia ESS Wave 1 (2011-12).

Source: sect_cover_hh_w1.dta (HH cover page), plus the households
        recorded ONLY in sect_cover_pp_w1.dta (agriculture cover; GH #1120 D1)
Weight: pw (cross-sectional, one weight only)
Cluster: ea_id
Strata: constructed from saq01 (region) x rural
Rural: rural column mapped to Rural/Urban
"""
import sys
sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, format_id, to_parquet
import pandas as pd
from ethiopia import with_agriculture_cover_households

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

# GH #1120 D1 (@ligon, 2026-10-23): households the survey recorded ONLY in
# the agriculture (post-planting) cover enter the sample from that cover,
# with that cover's weight.  See ethiopia.with_agriculture_cover_households
# and CONTENTS.org "sample(): the agriculture cover's households".
# W1: every agriculture-cover household is in the household cover, so this
# adds no row; the rule is the same in every wave.
hh_codes = get_dataframe('../Data/sect_cover_hh_w1.dta', convert_categoricals=False)
pp = get_dataframe('../Data/sect_cover_pp_w1.dta', convert_categoricals=False)
n0 = len(sample)
sample = with_agriculture_cover_households(
    sample, '2011-12', hh_codes, df, pp,
    dict(hhid='household_id', ea='ea_id', weight='pw', rural='rural'),
    rural_map)
sample = sample.set_index(['i', 't'])

to_parquet(sample, 'sample.parquet')
