#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
from malawi import (handling_unusual_units, conversion_table_matching,
                    food_acquired_to_canonical, normalize_food_label,
                    is_other_specify_code)
from lsms_library.transformations import U_UNKNOWN

wave = '2019-20'

df = get_dataframe('../Data/Cross_Sectional/HH_MOD_G1.dta', convert_categoricals=True)

conversions = get_dataframe('../Data/Cross_Sectional/ihs_foodconversion_factor_2020.dta', convert_categoricals=True)

panel_df = get_dataframe('../Data/Panel/hh_mod_g1_19.dta',convert_categoricals=True)

# Read region directly from household modules for conversion table merge
hh_cs = get_dataframe('../Data/Cross_Sectional/hh_mod_a_filt.dta', convert_categoricals=True)
hh_pn = get_dataframe('../Data/Panel/hh_mod_a_filt_19.dta', convert_categoricals=True)
regions_cs = hh_cs[['case_id', 'region']].rename(columns={'case_id': 'j'})
regions_pn = hh_pn[['y4_hhid', 'region']].rename(columns={'y4_hhid': 'j'})
regions = pd.concat([regions_cs, regions_pn]).drop_duplicates().set_index('j')['region']
regions = regions.replace({'South': 'Southern'})
regions.name = 'm'

columns_dict = {'case_id': 'j', 'y4_hhid': 'j',  'hh_g02' : 'i', 'hh_g03a': 'quantity_consumed', 'hh_g03b' : 'unitcode_consumed', 'hh_g03b_label': 'units_consumed', 'hh_g03b_oth': 'unitsdetail_consumed',
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

df = df.set_index(['j', 'i'])
df = df.join(regions).replace(r'^\s*$', pd.NA, regex=True)
df['unitcode_consumed'] = df['unitcode_consumed'].str.upper()
df['unitcode_bought'] = df['unitcode_bought'].str.upper()
df['unitcode_produced'] = df['unitcode_produced'].str.upper()
df['unitcode_gifted'] = df['unitcode_gifted'].str.upper()

#handling conversion table
conversions = conversions.replace({'South': 'Southern'}).groupby(['region', 'item_name', 'unit_code']).agg({'factor': 'mean'})
df = df.reset_index().merge(conversions, how='left',
                            left_on=['i', 'm', 'unitcode_consumed'],
                            right_on=['item_name', 'region', 'unit_code']).rename({'factor' : 'cfactor_consumed'}, axis=1)
df = df.merge(conversions, how='left',
              left_on=['i', 'm', 'unitcode_bought'],
              right_on=['item_name', 'region', 'unit_code']).rename({'factor' : 'cfactor_bought'}, axis = 1)
df = df.merge(conversions, how='left',
              left_on=['i', 'm', 'unitcode_produced'],
              right_on=['item_name', 'region', 'unit_code']).rename({'factor' : 'cfactor_produced'}, axis = 1)
df = df.merge(conversions, how='left',
              left_on=['i', 'm', 'unitcode_gifted'],
              right_on=['item_name', 'region', 'unit_code']).rename({'factor' : 'cfactor_gifted'}, axis = 1)

df = df.set_index(['j', 'i'])
df = handling_unusual_units(df, suffixes=['consumed','bought','produced','gifted'])

# GH #1074 ruling (@ligon 2026-11-03): a row served u = 'Unknown' ("no unit
# recorded") does NOT keep the conversion table's code-23 OTHER (SPECIFY)
# factor.  The factor was merged above on the RAW code, so a code-23 row
# whose specify text is absent, 'N/A' or names a food (handling_unusual_units)
# still carried that table's factor for an unnamed unit -- e.g. 10.99 kg per
# mango.  Code-23 rows whose text names a unit keep their factor.
for s in ['consumed', 'bought', 'produced', 'gifted']:
    no_unit = (df[f'u_{s}'].eq(U_UNKNOWN).fillna(False).to_numpy()
               & is_other_specify_code(df[f'unitcode_{s}']).to_numpy())
    df[f'cfactor_{s}'] = df[f'cfactor_{s}'].mask(no_unit)

df['t'] = wave
df = df.reset_index()
out = food_acquired_to_canonical(df.set_index(['j', 't', 'i']), wave=wave)
to_parquet(out, 'food_acquired.parquet')
