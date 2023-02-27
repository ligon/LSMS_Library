#!/usr/bin/env python

import sys
sys.path.append('../../_/')
from burkina_faso import age_sex_composition
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster

with dvc.api.open('../Data/emc2014_p1_individu_27022015.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

df["hhid"]  = df["zd"].astype(str) + '-'  + df["menage"].astype(int).astype(str)
regions  = df.groupby('hhid').agg({'region' : 'first'})

df = age_sex_composition(df, sex='B2', sex_converter=lambda x:['m','f'][x=='Feminin'], age='B4', age_converter=None, hhid='hhid')
df = pd.merge(left = df, right = regions, how = 'left', left_index = True, right_index = True)

df = df.rename(columns = {'region' : 'm'})
df['t'] = '2014'
df = df.set_index(['t', 'm'], append = True)
df.columns.name = 'k'

df.to_parquet('household_characteristics.parquet')
