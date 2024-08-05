import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet

idxvars = dict(j='sdd_hhid',
                t=('sdd_rural', lambda x: "2019-20"))
#myvars = dict(date='Int_End')
myvar=dict(date='hh_a18') #start date

df = df_data_grabber('../Data/HH_SEC_A.dta',idxvars,**myvars)
df['date'] = pd.to_datetime(df['date'])
df['date'] = pd.to_datetime(df['date']).dt.date

to_parquet(df,'interview_date.parquet')

