#!/usr/bin/env python3
import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

idxvars = dict(j = 'HHID',
               t=('region',lambda x:'2009-10'),  # Note trivial mapping
)

myvars = dict(day = 'h1bq2a',
              month= 'h1bq2b',
              year = 'h1bq2c',
              #interview_date = (['h1bq2c','h1bq2b', 'h1bq2a'], lambda x: pd.to_datetime(x))
              )
df = df_data_grabber('../Data/GSEC1.dta',idxvars,**myvars)

df['date'] = pd.to_datetime(dict(year=df.year, month=df.month, day=df.day))
df = df.drop(columns = list( myvars.keys()))
to_parquet(df,'interview_date.parquet')
