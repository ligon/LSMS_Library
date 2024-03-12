#!/usr/bin/env python

import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster
import sys
sys.path.append('../../../_/')
from local_tools import conversion_table_matching_global

def age_sex_composition(df, sex, sex_converter, age, age_converter, hhid):
    Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
    df = get_household_roster(df, sex=sex,  sex_converter=sex_converter,
                                  age=age, age_converter=age_converter, HHID= hhid,
                                  convert_categoricals=True,Age_ints=Age_ints,fn_type=None)
    df['log HSize'] = np.log(df[['girls', 'boys', 'men', 'women']].sum(axis=1))
    df.index.name = 'j'
    return df

def sex_conv(x):
    if str.lower(x) == 'female':
        return 'f'
    elif str.lower(x) == 'male':
        return 'm'

#household characteristic code for 2010-11, 2016-17, 2019-20
def get_household_characteristics(df, year):
    df = age_sex_composition(df, sex='hh_b03', sex_converter=sex_conv,
                                age='hh_b05a', age_converter=None, hhid='case_id')

    df = df.reset_index()
    df['t'] = year
    df = df.set_index(['j','t'])
    df.columns.name = 'k'
    return df

#other features code for 2010-11, 2016-17, 2019-20
def get_other_features(df, year, reside):
    df = df.loc[:,[ "case_id", "region", reside]]
    df['region'] = df['region'].map({1: 'North', 2: 'Central', 3: 'Southern'})
    df =  df.rename({'case_id': 'j', 'region' : 'm', reside: 'Rural'}, axis = 1)
    df['t'] = year
    df = df.set_index(['j','t'])
    df.columns.name = 'k'
    return df

#food_acquired code; not comprehensive of all the units, but can handle some [insert number] kilograms and grams formatting
def handling_unusual_units(df):
    grams = r'(\d+)\s*g(?:\s+|r)'
    kgs =r'(\d+)\s*k(?:g|ilo)'

    conv_kgrams_consumed = pd.concat([df['unitsdetail_consumed'].str.lower().str.extract(grams).astype(float)*0.01,
                                  df['unitsdetail_consumed'].str.lower().str.extract(kgs).astype(float)], axis= 0).dropna()
    conv_kgrams_bought = pd.concat([df['unitsdetail_bought'].str.lower().str.extract(grams).astype(float)*0.01,
                                df['unitsdetail_bought'].str.lower().str.extract(kgs).astype(float)], axis=0).dropna()

    df['cfactor_consumed'] = df.apply(lambda x: x['cfactor_consumed'] or conv_kgrams_consumed, axis = 1)
    df['cfactor_bought'] = df.apply(lambda x: x['cfactor_bought'] or conv_kgrams_bought, axis = 1)

    df["quantity_consumed"] = df['quantity_consumed'].mul(df['cfactor_consumed'].fillna(1))
    df["quantity_bought"] = df['quantity_bought'].mul(df['cfactor_bought'].fillna(1))

    df['u_consumed'] = np.where(~df['cfactor_consumed'].isna(), 'kg', df['unitsdetail_consumed'])
    df['u_consumed'] = df['u_consumed'].replace('nan', np.NaN).fillna(df['units_consumed'])
    df['u_bought'] = np.where(~df['cfactor_bought'].isna(), 'kg', df['unitsdetail_bought'])
    df['u_bought'] = df['u_bought'].replace('nan', np.NaN).fillna(df['units_bought'])
    return df

def conversion_table_matching(df, conversions, conversion_label_name, num_matches=3, cutoff = 0.6):
    return conversion_table_matching_global(df, conversions, conversion_label_name, num_matches=num_matches, cutoff = cutoff)
