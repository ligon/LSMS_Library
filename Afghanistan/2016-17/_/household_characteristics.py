#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster
from afghanistan import age_sex_composition

with dvc.api.open('../Data/H_03.dta', mode='rb') as dta:
    df_orig = from_dta(dta, convert_categoricals=True)

df = age_sex_composition(df_orig)
df1  = df_orig.groupby("hh_id").agg({'q_1_1a' : 'first'}) #getting regions for each hhid
df1.index = df1.index.str.replace(r'^(0)', '', regex=True) #adjusting for dropped 0
df = pd.merge(left = df, right = df1, how = 'left', left_index = True, right_index = True)
df['t'] = "2016-17"
df = df.rename(columns = {'q_1_1a' : 'm'})
df = df.set_index(['t', 'm'], append = True)
df.columns.name = 'k'

to_parquet(df, 'household_characteristics.parquet')
