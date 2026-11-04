import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
sys.path.append('../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet
from nigeria import interview_result_label


##Harvest '2013Q1'
idxvars = dict(i='hhid',
                t=('sector', lambda x: "2013Q1"))
myvars = dict(year='saq13y',
                month='saq13m',
                day='saq13d',
                InterviewResult=('saq1', interview_result_label))
df = df_data_grabber('../Data/Post Harvest Wave 2/Household/secta_harvestw2.dta',idxvars,**myvars)
df['int_t'] = pd.to_datetime(df[['year', 'month', 'day']], errors='coerce')
df=df.drop(columns=['year','month','day'])

#Planting(2012Q3)
idxvars = dict(i='hhid',
                t=('sector', lambda x: "2012Q3"))
myvars = dict(year='saq13y',
                month='saq13m',
                day='saq13d',
                InterviewResult=('saq22a', interview_result_label))
df_1 = df_data_grabber('../Data/Post Planting Wave 2/Household/secta_plantingw2.dta',idxvars,**myvars)
df_1['int_t'] = pd.to_datetime(df_1[['year', 'month', 'day']], errors='coerce')
df_1=df_1.drop(columns=['year','month','day'])

df=pd.concat([df,df_1],axis=0)
# A date is what this table serves: InterviewResult must not carry an undated
# household past the framework's dropna(how='all') (GH #1096).
df = df.dropna(subset=['int_t'])[['int_t', 'InterviewResult']]
to_parquet(df,'interview_date.parquet')