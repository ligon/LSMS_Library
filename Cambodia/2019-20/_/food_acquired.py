#!/usr/bin/env python

import sys
sys.path.append('../../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from local_tools import to_parquet

with dvc.api.open('../Data/hh_sec_5.dta', mode='rb') as dta:
    foods = from_dta(dta, convert_categoricals=True)

with dvc.api.open('../Data/hh_sec_5.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=False)

df = df.rename({'HHID': 'j', 'food_consumption_roster_1__id' : 'i', 's05q03' : 'units','s05q04' : 'quantity', 's05q05' : 'total spent', 's05q06': 'value obtained'}, axis=1)
df['i'] = foods['food_consumption_roster_1__id']

cols = df.loc[:, ['quantity', 'total spent', 'value obtained']].columns
df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
df = df.fillna(0)

df['price per unit'] = (df['total spent']+ df['value obtained'])/df['quantity']
df = df.drop({'s05_start_time', 's05_end_time', 's05_respondent'}, axis=1)
df = df.set_index(['j', 'i'])

to_parquet(df, "food_acquired.parquet")
