#!/usr/bin/env python3
import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet
from datetime import datetime

idxvars = dict(j = 'HHID',
               t=('region',lambda x:'2011-12'),  # Note trivial mapping
               )
myvars = dict(day = 'day',
              month= 'month',
              year = 'year',
              )

df = df_data_grabber('../Data/GSEC1.dta',idxvars,**myvars)
# error = coerce returns NaT when encountering NaN vals in the 3 coloumns or when day/month is out of range
# 3 instances in April have day as 31
df['date'] = pd.to_datetime(df[['year', 'month', 'day']], errors='coerce')
df = df.drop(columns = list( myvars.keys()))
to_parquet(df,'interview_date.parquet')