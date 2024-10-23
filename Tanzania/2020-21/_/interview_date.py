import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from local_tools import df_data_grabber, to_parquet

idxvars = dict(j='y5_hhid',
                t=('y5_rural', lambda x: "2020-21"))
#myvars = dict(date='Int_End')
myvars=dict(date='hh_a18') #start date


df = df_data_grabber('../Data/hh_sec_a.dta',idxvars,**myvars)
df['date'] = pd.to_datetime(df['date'])
df['date'] = pd.to_datetime(df['date']).dt.date

to_parquet(df,'interview_date.parquet')