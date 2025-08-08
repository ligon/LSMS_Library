#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe

"""Pull region identifier from other_features and use as markets;
   concatenate other material in other features with
   household_characteristics.
"""

import pandas as pd
from cfe.df_utils import use_indices

DFs = ['food_expenditures','household_characteristics','food_quantities']

dir = '../var/%s.parquet'

oc = get_dataframe(dir % 'other_features')

def m_regions(df,oc):

    try:
        df = df.droplevel('m')
    except KeyError:
        pass
    oc = oc.reset_index('m')
    try:
        colnames = df.columns.names
        df = df.join(oc)
        df = df.reset_index().set_index(['j','t','m'])
        df.columns.names = colnames
    except ValueError: # Already has variable?
        pass

    if not df.columns.name=='k':
        # Drop other characteristics
        oc = oc.reset_index().set_index(['j','t','m'])
        df = df.drop(oc.columns,axis=1)

    return df

for fn in DFs:
    df = get_dataframe(dir % fn)
    to_parquet(m_regions(df,oc), dir % fn)

    

