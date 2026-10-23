#!/usr/bin/env python
"""Extract sampling design variables for Ethiopia ESS Wave 3 (2015-16).

Source: sect_cover_hh_w3.dta (HH cover page), plus the households
        recorded ONLY in sect_cover_pp_w3.dta (agriculture cover; GH #1120 D1)
Weight: pw_w3 (cross-sectional, one weight only)
Cluster: ea_id2
Strata: constructed from saq01 (region) x rural
Rural: rural column mapped to Rural/Urban
"""
import sys
sys.path.append('../../_/')
sys.path.append('../../../_/')
from lsms_library.local_tools import get_dataframe, format_id, to_parquet
import pandas as pd
from ethiopia import with_agriculture_cover_households, ea_id2_crosswalk

df = get_dataframe('../Data/sect_cover_hh_w3.dta')

# Rural mapping: wave 3 has 'RURAL', 'SMALL TOWN', 'Meduim and large town'
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
    'weight': df['pw_w3'].astype(float),
    'Rural': df['rural'].map(rural_map),
})

# Construct strata from region x Rural
region = df['saq01'].astype(str).str.strip()
sample['strata'] = region + ' ' + sample['Rural'].fillna('')
sample['strata'] = sample['strata'].str.strip()

# Panel weight = cross-sectional weight (single weight)
sample['panel_weight'] = sample['weight']

sample['t'] = '2015-16'

# GH #1120 D1 (@ligon, 2026-10-23): households the survey recorded ONLY in
# the agriculture (post-planting) cover enter the sample from that cover,
# with that cover's weight.  See ethiopia.with_agriculture_cover_households
# and CONTENTS.org "sample(): the agriculture cover's households".
# W3: 27 households.  The agriculture cover carries only the W1-style ea_id,
# so v (ea_id2) comes through the survey's own crosswalk -- the (ea_id,
# ea_id2) pairs of the W3 household cover, plus W2's for the one EA (10
# households) with no W3 household-cover row.  Cross-check: ESS2/ESS3
# build household_id2 as ea_id2 + a 3-digit serial (true of every row of
# both covers that carry both), so the crosswalked v must be its prefix.
hh_codes = get_dataframe('../Data/sect_cover_hh_w3.dta', convert_categoricals=False)
pp = get_dataframe('../Data/sect_cover_pp_w3.dta', convert_categoricals=False)
xw = ea_id2_crosswalk(hh_codes, get_dataframe('../../2013-14/Data/sect_cover_hh_w2.dta',
                                                convert_categoricals=False))
n0 = len(sample)
sample = with_agriculture_cover_households(
    sample, '2015-16', hh_codes, df, pp,
    dict(hhid='household_id2', ea='ea_id2', weight='pw_w3', rural='rural', pp_ea='ea_id'),
    rural_map, ea_crosswalk=xw)
added = sample.iloc[n0:]
assert (added['v'] == added['i'].str[:15]).all(), \
    "2015-16: a crosswalked ea_id2 is not its household_id2's EA prefix"
sample = sample.set_index(['i', 't'])

to_parquet(sample, 'sample.parquet')
