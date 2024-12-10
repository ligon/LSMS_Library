import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_')
from local_tools import df_data_grabber, to_parquet


#Harvest '2011Q1'
idxvars = dict(j='hhid',
                t=('sector', lambda x: "2011Q1"))
myvars = dict(year='saq13y',
                month='saq13m',
                day='saq13d')
df = df_data_grabber('../Data/Post Harvest Wave 1/Household/secta_harvestw1.dta',idxvars,**myvars)
df['date'] = pd.to_datetime(df[['year', 'month', 'day']], errors='coerce')
df=df.drop(columns=['year','month','day'])

#Planting(2010Q3)
idxvars = dict(j='hhid',
                t=('sector', lambda x: "2010Q3"))
myvars = dict(year='saq13y',
                month='saq13m',
                day='saq13d')
df_1 = df_data_grabber('../Data/Post Planting Wave 1/Household/secta_plantingw1.dta',idxvars,**myvars)
df_1['date'] = pd.to_datetime(df_1[['year', 'month', 'day']], errors='coerce')
df_1=df_1.drop(columns=['year','month','day'])

df = pd.concat([df,df_1])
to_parquet(df,'interview_date.parquet')