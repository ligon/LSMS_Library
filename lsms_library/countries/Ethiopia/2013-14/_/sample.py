#!/usr/bin/env python
"""Extract sampling design variables for Ethiopia ESS Wave 2 (2013-14).

Source: sect_cover_hh_w2.dta (HH cover page), plus the households
        recorded ONLY in sect_cover_pp_w2.dta (agriculture cover; GH #1120 D1)
Weight: pw2 (cross-sectional, one weight only)
Cluster: ea_id2
Strata: constructed from saq01 (region) x rural
Rural: rural column mapped to Rural/Urban
"""
import sys
sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, format_id, to_parquet
import pandas as pd
from ethiopia import with_agriculture_cover_households

df = get_dataframe('../Data/sect_cover_hh_w2.dta')

# Rural mapping: wave 2 has 'Rural', 'Small town (urban)', 'Large town (urban)'
rural_map = {
    'rural': 'Rural',
    'Rural': 'Rural',
    'RURAL': 'Rural',
    'small town': 'Urban',
    'Small town': 'Urban',
    'Small Town': 'Urban',
    'SMALL TOWN': 'Urban',
    'Small town (urban)': 'Urban',
    'urban': 'Urban',
    'Urban': 'Urban',
    'URBAN': 'Urban',
    'Large town (urban)': 'Urban',
    'Meduim and large town': 'Urban',
}

sample = pd.DataFrame({
    'i': df['household_id2'].apply(format_id),
    'v': df['ea_id2'].apply(format_id),
    'weight': df['pw2'].astype(float),
    'Rural': df['rural'].map(rural_map),
})

# Construct strata from region x Rural
region = df['saq01'].astype(str).str.strip()
sample['strata'] = region + ' ' + sample['Rural'].fillna('')
sample['strata'] = sample['strata'].str.strip()

# Panel weight = cross-sectional weight (single weight)
sample['panel_weight'] = sample['weight']

sample['t'] = '2013-14'

# GH #1120 D1 (@ligon, 2026-10-23): households the survey recorded ONLY in
# the agriculture (post-planting) cover enter the sample from that cover,
# with that cover's weight.  See ethiopia.with_agriculture_cover_households
# and CONTENTS.org "sample(): the agriculture cover's households".
# W2: 25 households, every one a 2011-12 sample household (its W1
# household_id is in the W1 household cover, same EA); the agriculture
# cover carries ea_id2 itself.
hh_codes = get_dataframe('../Data/sect_cover_hh_w2.dta', convert_categoricals=False)
pp = get_dataframe('../Data/sect_cover_pp_w2.dta', convert_categoricals=False)
n0 = len(sample)
sample = with_agriculture_cover_households(
    sample, '2013-14', hh_codes, df, pp,
    dict(hhid='household_id2', ea='ea_id2', weight='pw2', rural='rural'),
    rural_map)
sample = sample.set_index(['i', 't'])

to_parquet(sample, 'sample.parquet')
