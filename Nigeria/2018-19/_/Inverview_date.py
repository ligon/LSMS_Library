import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet


##Harvest （2018Q3)
idxvars = dict(j='hhid',
                t=('sector', lambda x: "2018Q3"))
myvars = dict(date='InterviewDate')

df = df_data_grabber('../Data/secta_harvestw4.csv',idxvars,**myvars)
df['date'] = pd.to_datetime(df['date'])
df['date'] = pd.to_datetime(df['date']).dt.date


##Planting(2019Q1)
idxvars = dict(j='hhid',
                t=('sector', lambda x: "2019Q1"))
myvars = dict(date='InterviewDate')
df_1 = df_data_grabber('../Data/secta_plantingw4.csv',idxvars,**myvars)
df_1['date'] = pd.to_datetime(df['date'])
df_1['date'] = pd.to_datetime(df['date']).dt.date

df = pd.concat([df,df_1])
to_parquet(df,'interview_date.parquet')