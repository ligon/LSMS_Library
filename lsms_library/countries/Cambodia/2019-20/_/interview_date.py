#!/usr/bin/env python
"""Cambodia 2019-20 interview date.

Source: hh_sec_1.dta.  Date columns intvw_day / intvw_month / intvw_year
are STRING dtype (full 4-digit year), cast to int before to_datetime.
i = HHID (32-char hex hash, already string).  v is joined from sample()
at API time, so it is not baked into this parquet.
"""
import sys
sys.path.append('../../../_/')
import pandas as pd
from lsms_library.local_tools import df_data_grabber, to_parquet

idxvars = dict(i='HHID',
               t=('intvw_year', lambda x: "2019-20"))

myvars = dict(year='intvw_year',
              month='intvw_month',
              day='intvw_day')

df = df_data_grabber('../Data/hh_sec_1.dta', idxvars, **myvars)

for c in ['year', 'month', 'day']:
    df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')

df['Int_t'] = pd.to_datetime(dict(year=df['year'], month=df['month'], day=df['day']),
                             errors='coerce')
df = df.drop(columns=['year', 'month', 'day'])

to_parquet(df, 'interview_date.parquet')
