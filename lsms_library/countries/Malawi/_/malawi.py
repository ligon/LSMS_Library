#!/usr/bin/env python

import pandas as pd
import numpy as np
import json
import dvc.api
from ligonlibrary.dataframes import from_dta
from lsms.tools import get_household_roster
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import conversion_table_matching_global

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
def get_household_characteristics(df, year, hhid = 'case_id'):
    df = age_sex_composition(df, sex='hh_b03', sex_converter=sex_conv,
                                age='hh_b05a', age_converter=None, hhid=hhid)

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

def _extract_kg_conversion(series):
    """Extract kilogram conversion factors from a unit-detail string series.

    Parses patterns like '300 grams', '1kg', '2 kilo' and returns
    a Series of conversion factors in kilograms.
    """
    grams = r'(\d+)\s*g(?:\s+|r)'
    kgs = r'(\d+)\s*k(?:g|ilo)'

    lower = series.str.lower()
    conv = pd.concat([lower.str.extract(grams).astype(float) * 0.01,
                      lower.str.extract(kgs).astype(float)], axis=0).dropna()
    return conv


def handling_unusual_units(df, suffixes=None):
    """Convert unusual unit descriptions to kg-based quantities.

    Parameters
    ----------
    df : DataFrame
    suffixes : list[str], optional
        Column suffixes to process (e.g. ``['consumed', 'bought']``).
        For each suffix, expects columns ``unitsdetail_{suffix}``,
        ``cfactor_{suffix}``, ``quantity_{suffix}``, and ``units_{suffix}``.
        Defaults to ``['consumed', 'bought']`` for backward compatibility.
    """
    if suffixes is None:
        suffixes = ['consumed', 'bought']

    for suffix in suffixes:
        detail_col = f'unitsdetail_{suffix}'
        cfactor_col = f'cfactor_{suffix}'
        quantity_col = f'quantity_{suffix}'
        units_col = f'units_{suffix}'
        u_col = f'u_{suffix}'

        if detail_col not in df.columns:
            continue

        conv_kg = _extract_kg_conversion(df[detail_col])

        df[cfactor_col] = df.apply(lambda x, c=cfactor_col: x[c] or conv_kg, axis=1)
        df[quantity_col] = df[quantity_col].mul(df[cfactor_col].fillna(1))
        df[u_col] = np.where(~df[cfactor_col].isna(), 'kg', df[detail_col])
        df[u_col] = df[u_col].replace('nan', pd.NA).fillna(df[units_col])

    return df

def conversion_table_matching(df, conversions, conversion_label_name, num_matches=3, cutoff = 0.6):
    return conversion_table_matching_global(df, conversions, conversion_label_name, num_matches=num_matches, cutoff = cutoff)

def Sex(value):
    if isinstance(value, str) and value.strip():
        return value.strip().upper()[0]
    else:
        return np.nan
