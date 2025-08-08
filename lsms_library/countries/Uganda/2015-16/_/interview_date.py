import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

idxvars = dict(j='HHID',
                t=('district', lambda x: "2015-16"))

myvars = dict(year='year',  
                month='month',
                day='day')

df = df_data_grabber('../Data/gsec1.dta',idxvars,**myvars)
df['date'] = pd.to_datetime(df[['year', 'month', 'day']])
df=df.drop(columns=['year','month','day'])

to_parquet(df,'interview_date.parquet')
