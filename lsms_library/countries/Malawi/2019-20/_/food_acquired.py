#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_categorical_mapping
from lsms_library.local_tools import get_dataframe

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
from malawi import handling_unusual_units, conversion_table_matching

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
df['i'] = df['i'].astype(str).str.capitalize()

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
df = handling_unusual_units(df)

df['price per unit'] = df['expenditure']/df['quantity_bought']

df['t'] = wave
df = df.reset_index().drop(columns=['m']).set_index(['j','t','i']).dropna(how='all')

final = df.loc[:, ['quantity_consumed', 'u_consumed', 'quantity_bought',
                   'u_bought', 'price per unit', 'expenditure',
                   'quantity_produced',
                   'cfactor_consumed', 'cfactor_bought']]
final['u_bought'] = final.u_bought.astype(str)

# Fix food labels via the cross-wave union helper (GH #216).  See
# malawi.harmonize_food_labels() for rationale and the full derivation.
from malawi import harmonize_food_labels
final = harmonize_food_labels(final, level='i')

final = final.dropna(how='all')
final = final.reorder_levels(['j','t','i']).sort_index()
to_parquet(final, "food_acquired.parquet")
