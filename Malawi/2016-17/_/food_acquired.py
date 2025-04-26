#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_categorical_mapping, get_dataframe
from lsms_library.local_tools import get_dataframe

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from malawi import handling_unusual_units, conversion_table_matching

wave = '2016-17'

with dvc.api.open('../Data/Cross_Sectional/hh_mod_g1.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

panel_df = get_dataframe('../Data/Panel/hh_mod_g1_16.dta',convert_categoricals=True)
conversions = pd.read_csv('../../2010-11/_/ihs3_conversions.csv')

regions = get_dataframe('other_features.parquet').reset_index().set_index(['j'])['m']

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
df['i'] = df['i'].astype(str).str.capitalize()

cols = df.loc[:, ['quantity_consumed', 'expenditure', 'quantity_bought',
                  'quantity_produced', 'quantity_gifted']].columns
df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')

match_df, D = conversion_table_matching(df, conversions, conversion_label_name = 'item_name')
conversions['item_name'] = conversions['item_name'].map(D)

df = df.set_index(['j', 'i'])
df = df.join(regions).set_index('m', append=True).replace(r'^\s*$', np.nan, regex=True)

# Deal with some problematic units which are floats
df['units_consumed'] = df.units_consumed.astype(str).str.upper()
df['units_bought'] = df.units_bought.astype(str).str.upper()

# handling conversion table
conversions = conversions.set_index(['region', 'item_name', 'unit_code'])
df = df.reset_index().merge(conversions, how='left', left_on=['i', 'm', 'unitcode_consumed'], right_on=['item_name', 'region', 'unit_code']).rename({'factor' : 'cfactor_consumed'}, axis=1)
df = df.merge(conversions, how='left', left_on=['i', 'm', 'unitcode_bought'], right_on=['item_name', 'region', 'unit_code']).rename({'factor' : 'cfactor_bought'}, axis = 1)
df = df.set_index(['j', 'm', 'i'])
df = handling_unusual_units(df)

df['price per unit'] = df['expenditure']/df['quantity_bought']
df['t'] = wave
df = df.reset_index().set_index(['j','t', 'i']).dropna(how='all')

final = df.loc[:, ['quantity_consumed', 'u_consumed', 'quantity_bought', 'u_bought', 'price per unit', 'expenditure', 'cfactor_consumed', 'cfactor_bought']]

labelsd = get_categorical_mapping(tablename='harmonize_food',
                                  idxvars={'j':wave},
                                  **{'Label':'Preferred Label'})

final = final.rename(index=labelsd,level='i')
final = final.dropna(how='all')
to_parquet(final, "food_acquired.parquet")
