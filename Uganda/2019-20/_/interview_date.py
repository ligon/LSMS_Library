import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

idxvars = dict(j='hhid',
               t=('batch', lambda x: "2019-20"))

myvars = dict(year='year',  
                month='month',
                day='day')

df = df_data_grabber('../Data1/HH/gsec1.dta',idxvars,**myvars)
df['interview_date'] = pd.to_datetime(df[['year', 'month', 'day']])
df=df.drop(columns=['year','month','day'])


to_parquet(df,'interview_date.parquet')
