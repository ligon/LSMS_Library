import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet
import dvc.api
from lsms import from_dta

##Harvest 2016Q1
fn='../Data/secta_harvestw3.csv'
with dvc.api.open(fn,mode='rb') as csv:
    df =  pd.read_csv(csv)
vars = {'hhid': 'j', 'saq13y': 'year', 'saq13m': 'month', 'saq13d': 'day'}
t = '2016Q1'
df=df[['hhid','saq13y', 'saq13m', 'saq13d']]
df['t']=t
df=df.rename(columns=vars)
df=df.set_index(['j','t'])
df['date'] = pd.to_datetime(df[['year', 'month', 'day']], errors='coerce')
df=df.drop(columns=['year','month','day'])


##Planting(2015Q3)
fn='../Data/secta_plantingw3.csv'
with dvc.api.open(fn,mode='rb') as csv:
    df_1 =  pd.read_csv(csv)
t = '2015Q3'
df_1=df_1[['hhid','saq13y', 'saq13m', 'saq13d']]
df_1['t']=t
df_1=df_1.rename(columns=vars)
df_1=df_1.set_index(['j','t'])
df_1['date'] = pd.to_datetime(df_1[['year', 'month', 'day']], errors='coerce')
df_1=df_1.drop(columns=['year','month','day'])

df = pd.concat([df,df_1])
to_parquet(df,'interview_date.parquet')

##Harvest 2016Q1 (201602-201604)
# idxvars = dict(j='hhid',
#                 t=('sector', lambda x: "2018Q3"))
# myvars = dict(year='saq13y',
#                 month='saq13m',
#                 day='saq13d')
# df = df_data_grabber('../Data/secta_harvestw3.csv',idxvars,**myvars)
# df['date'] = pd.to_datetime(df[['year', 'month', 'day']], errors='coerce')
# df=df.drop(columns=['year','month','day'])
##Planting(2015Q3 201509-201511)
# idxvars = dict(j='hhid',
#                 t=('sector', lambda x: "2015Q3"))
# myvars = dict(year='saq13y',
#                 month='saq13m',
#                 day='saq13d')
# df_1 = df_data_grabber('../Data/secta_plantingw3.csv',idxvars,**myvars)
# df_1['date'] = pd.to_datetime(df_1[['year', 'month', 'day']], errors='coerce')
# df_1=df_1.drop(columns=['year','month','day'])