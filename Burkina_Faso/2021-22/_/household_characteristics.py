#!/usr/bin/env python

import sys
sys.path.append('../../_/')
sys.path.append('../../../_')
import pandas as pd
import dvc.api
from lsms import from_dta
from burkina_faso import age_sex_composition
from local_tools import to_parquet


with dvc.api.open('../Data/s01_me_bfa2021.dta', mode='rb') as dta:
    df_orig = from_dta(dta, convert_categoricals=False)
with dvc.api.open('../Data/s00_me_bfa2021.dta', mode='rb') as dta:
    regions = from_dta(dta, convert_categoricals=True)

regions['j'] =  regions["hhid"].astype(int).astype(str)
regions  = regions.groupby('j').agg({'s00q01' : 'first'}).rename({'s00q01': 'm'}, axis =1)

df_orig["age"] = df_orig['s01q04a'].fillna(2022-df_orig['s01q03c'])
df_orig["hhid"]  = df_orig["hhid"].astype(int).astype(str)

def waves(df):
    wave_dict = dict()
    for i in df["vague"].unique():
        wave_dict[i]=df[df["vague"]==i]
    return wave_dict

wave_dict=waves(df_orig)

for i in wave_dict:
    df = age_sex_composition(wave_dict[i], sex='s01q01', sex_converter=lambda x:['m','f'][x==2], age='age', age_converter=None, hhid='hhid')
    if i == 1.0:
        df['t'] = '2021'
    if i == 2.0:
        df['t'] = '2022'
    wave_dict[i] = df

final = pd.concat([wave_dict[1.0], wave_dict[2.0]])

final = pd.merge(left = final, right = regions, how = 'left', left_index = True, right_index = True)
final = final.set_index(['t', 'm'], append=True)
final.columns.name = 'k'

to_parquet(final, 'household_characteristics.parquet')
