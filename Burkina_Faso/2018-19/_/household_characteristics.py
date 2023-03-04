#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster
from burkina_faso import age_sex_composition

with dvc.api.open('../Data/s01_me_bfa2018.dta', mode='rb') as dta:
    df_orig = from_dta(dta, convert_categoricals=False)

with dvc.api.open('../Data/s00_me_bfa2018.dta', mode='rb') as dta:
    regional_info  = from_dta(dta, convert_categoricals=True)

df_orig["age"] = df_orig['s01q04a'].fillna(2019-df_orig['s01q03c'])
df_orig["hhid"]  = df_orig["grappe"].astype(int).astype(str) + '-'  + df_orig["menage"].astype(int).astype(str) #concatenate menage and grappe
regional_info['hhid'] =  regional_info["grappe"].astype(int).astype(str) + '-'  + regional_info["menage"].astype(int).astype(str)

def waves(df):
    wave_dict = dict()
    for i in df["vague"].unique():
        wave_dict[i]=df[df["vague"]==i]
    return wave_dict

wave_dict=waves(df_orig)

for i in wave_dict:
    df = age_sex_composition(wave_dict[i], sex='s01q01', sex_converter=lambda x:['m','f'][x==2], age='age', age_converter=None, hhid='hhid')
    if i == 1.0:
        df['t'] = '2018'
    if i == 2.0:
        df['t'] = '2019'
    wave_dict[i] = df

final = pd.concat([wave_dict[1.0], wave_dict[2.0]])

regions  = regional_info.groupby('hhid').agg({'s00q01' : 'first'})
final = pd.merge(left = final, right = regions, how = 'left', left_index = True, right_index = True)

final = final.rename(columns = {'s00q01' : 'm'})
final = final.set_index(['t', 'm'], append = True)
final.columns.name = 'k'

final.to_parquet('household_characteristics.parquet')
