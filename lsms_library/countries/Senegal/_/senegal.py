#!/usr/bin/env python

import pandas as pd
import numpy as np
import json
from ligonlibrary.dataframes import from_dta

waves = ['2018-19', '2021-22']


def _household_roster_from_df(df, sex, age, HHID, sex_converter=None, age_converter=None,
                               months_spent='months_spent', Age_ints=None):
    """Inline replacement for lsms.tools.get_household_roster(fn_type=None)."""
    cols = [c for c in [HHID, sex, age, months_spent] if c in df.columns]
    df = df.loc[:, cols].rename(columns={HHID: 'HHID', sex: 'sex', age: 'age',
                                          months_spent: 'months_spent'})
    if sex_converter is not None:
        df['sex'] = df['sex'].apply(sex_converter)
    df = df.dropna(how='any')
    df['sex'] = df['sex'].apply(lambda s: str(s[0]).lower())
    if age_converter is not None:
        df['age'] = df['age'].apply(age_converter)
    df['boys']  = (df['sex'] == 'm') & (df['age'] < 18)
    df['girls'] = (df['sex'] == 'f') & (df['age'] < 18)
    df['men']   = (df['sex'] == 'm') & (df['age'] >= 18)
    df['women'] = (df['sex'] == 'f') & (df['age'] >= 18)
    if Age_ints is None:
        Age_ints = ((0,1),(1,5),(5,10),(10,15),(15,20),(20,30),(30,50),(50,60),(60,100))
    valvars = list({'HHID','girls','boys','men','women'}.intersection(df.columns))
    for lo, hi in Age_ints:
        s, e = lo, hi - 1
        df['Males %02d-%02d' % (s, e)]   = (df['sex'] == 'm') & (df['age'] >= lo) & (df['age'] < hi)
        df['Females %02d-%02d' % (s, e)] = (df['sex'] == 'f') & (df['age'] >= lo) & (df['age'] < hi)
        valvars += ['Males %02d-%02d' % (s, e), 'Females %02d-%02d' % (s, e)]
    try:
        if df['HHID'].iloc[0].split('.')[-1] == '0':
            df['HHID'] = df['HHID'].apply(lambda x: '%d' % int(float(x)))
    except (ValueError, AttributeError):
        pass
    if 'months_spent' in df.columns and df['months_spent'].count() > 0:
        g = df.loc[df['months_spent'] > 0, valvars].groupby('HHID')
    else:
        g = df[valvars].groupby('HHID')
    return g.sum()


def age_sex_composition(df, sex, sex_converter, age, age_converter, hhid):
    Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
    testdf = _household_roster_from_df(df, sex=sex, age=age, HHID=hhid,
                                       sex_converter=sex_converter,
                                       age_converter=age_converter,
                                       Age_ints=Age_ints)
    testdf['log HSize'] = np.log(testdf[['girls', 'boys', 'men', 'women']].sum(axis=1))
    testdf.index.name = 'j'
    return testdf


def panel_ids(df):
    """Construct previous_i for Senegal EHCVM panel linkage.

    The same grappe+menage identifies the same household across rounds.
    For panel households (in_panel == 1), previous_i equals the current i
    (same vague+grappe+menage composite ID from the prior wave).
    New replacement households (Nouveau Menage) are filtered out.
    """
    if 'in_panel' in df.columns:
        df = df[df['in_panel'] == 1]
        df = df.drop(columns=['in_panel'])

    df = df[df['previous_i'].notna()]
    return df[['previous_i']]
