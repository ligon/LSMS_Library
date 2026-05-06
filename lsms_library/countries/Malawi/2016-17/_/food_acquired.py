#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_dataframe

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
from malawi import (handling_unusual_units, conversion_table_matching,
                    food_acquired_to_canonical, normalize_food_label)

wave = '2016-17'

df = get_dataframe('../Data/Cross_Sectional/hh_mod_g1.dta', convert_categoricals=True)

panel_df = get_dataframe('../Data/Panel/hh_mod_g1_16.dta',convert_categoricals=True)
conversions = pd.read_csv('../../2010-11/_/ihs3_conversions.csv')

# Read region directly from household modules for conversion table merge
hh_cs = get_dataframe('../Data/Cross_Sectional/hh_mod_a_filt.dta', convert_categoricals=True)
hh_pn = get_dataframe('../Data/Panel/hh_mod_a_filt_16.dta', convert_categoricals=True)
regions_cs = hh_cs[['case_id', 'region']].rename(columns={'case_id': 'j'})
regions_pn = hh_pn[['y3_hhid', 'region']].rename(columns={'y3_hhid': 'j'})
regions = pd.concat([regions_cs, regions_pn]).drop_duplicates().set_index('j')['region']
regions = regions.replace({'South': 'Southern'})
regions.name = 'm'

columns_dict = {'case_id': 'j', 'y3_hhid':'j', 'hh_g02' : 'i', 'hh_g03a': 'quantity_consumed', 'hh_g03b' : 'unitcode_consumed', 'hh_g03b_label': 'units_consumed', 'hh_g03b_oth': 'unitsdetail_consumed',
                'hh_g05': 'expenditure', 'hh_g04a': 'quantity_bought', 'hh_g04b': 'unitcode_bought', 'hh_g04b_label': 'units_bought', 'hh_g04b_oth': 'unitsdetail_bought',
                'hh_g06a': 'quantity_produced', 'hh_g06b': 'unitcode_produced', 'hh_g06b_label': 'units_produced', 'hh_g06b_oth': 'unitsdetail_produced',
                'hh_g07a': 'quantity_gifted', 'hh_g07b': 'unitcode_gifted', 'hh_g07b_label': 'units_gifted', 'hh_g07b_oth': 'unitsdetail_gifted',
                }
df = df.rename(columns_dict, axis=1)
panel_df = panel_df.rename(columns_dict, axis=1)
df = df.loc[:, list(set(columns_dict.values()))]
panel_df = panel_df.loc[:, list(set(columns_dict.values()))]
df = pd.concat([df, panel_df], axis=0)
df['i'] = normalize_food_label(df['i'].astype(str).str.capitalize())

cols = df.loc[:, ['quantity_consumed', 'expenditure', 'quantity_bought',
                  'quantity_produced', 'quantity_gifted']].columns
df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

match_df, D = conversion_table_matching(df, conversions, conversion_label_name = 'item_name')
conversions['item_name'] = conversions['item_name'].map(D)
# conversion_table_matching is non-injective for 2016-17 (e.g. two
# distinct ihs3 entries both fuzzy-match to 'Groundnut flour'); after
# map(D) the conversions table acquires duplicate (region, item_name,
# unit_code) triples that would multiply rows in each per-source merge
# below.  Collapse to one factor per triple before the merge.
conversions = (conversions.dropna(subset=['item_name'])
               .groupby(['region', 'item_name', 'unit_code'], as_index=False)['factor']
               .mean())

df = df.set_index(['j', 'i'])
df = df.join(regions).replace(r'^\s*$', pd.NA, regex=True)

# Deal with some problematic units which are floats
for src in ('consumed', 'bought', 'produced', 'gifted'):
    df[f'units_{src}'] = df[f'units_{src}'].astype(str).str.upper()
    df[f'unitcode_{src}'] = df[f'unitcode_{src}'].astype(str).str.upper()

# handling conversion table
conversions = conversions.set_index(['region', 'item_name', 'unit_code'])
df = df.reset_index()
for src in ('consumed', 'bought', 'produced', 'gifted'):
    df = df.merge(
        conversions, how='left',
        left_on=['i', 'm', f'unitcode_{src}'],
        right_on=['item_name', 'region', 'unit_code'],
    ).rename({'factor': f'cfactor_{src}'}, axis=1)
df = df.set_index(['j', 'i'])
df = handling_unusual_units(df, suffixes=['consumed', 'bought', 'produced', 'gifted'])

df['t'] = wave
df = df.reset_index()
out = food_acquired_to_canonical(df.set_index(['j', 't', 'i']), wave=wave)
to_parquet(out, 'food_acquired.parquet')
