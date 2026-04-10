#!/usr/bin/env python

import pandas as pd
import numpy as np
import json
import dvc.api
from ligonlibrary.dataframes import from_dta
from lsms.tools import get_household_roster
import lsms_library.local_tools as tools


def i(value):
    '''
    Formatting household id from composite (grappe/zd, menage).
    Matches existing convention: str(grappe) + str(menage).rjust(3, '0')
    '''
    return tools.format_id(value.iloc[0]) + tools.format_id(value.iloc[1], zeropadding=3)

def age_sex_composition(df, sex, sex_converter, age, age_converter, hhid):
    Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
    testdf = get_household_roster(df, sex=sex, sex_converter=sex_converter,
                                  age=age, age_converter=age_converter, HHID=hhid,
                                  convert_categoricals=True,Age_ints=Age_ints,fn_type=None)
    testdf['log HSize'] = np.log(testdf[['girls', 'boys', 'men', 'women']].sum(axis=1))
    testdf.index.name = 'j'
    return testdf

def panel_ids(df):
    """Construct previous_i from previous_v (grappe) and previous_hid (menage).

    Must match the i() format above: format_id(grappe) + format_id(menage, zeropadding=3).
    """
    grappe = df['previous_v'].apply(tools.format_id)
    menage = df['previous_hid'].apply(lambda x: tools.format_id(x, zeropadding=3))
    df['previous_i'] = grappe + menage
    return df[['previous_i']]
