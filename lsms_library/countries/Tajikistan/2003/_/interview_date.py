#!/usr/bin/env python
"""Tajikistan 2003 interview dates.

Source: ../Data/interview.dta.  Records day_in/month_in but NO year column;
the survey was fielded in 2003, so the year is hardcoded.  Household id is
`hhid` (matches sample()'s `i`); `v` is joined from sample() at API time.
"""
import sys
sys.path.append('../../../_')
import pandas as pd
from lsms_library.local_tools import df_data_grabber, to_parquet

idxvars = dict(t=('hhid', lambda x: '2003'),
               i='hhid')
myvars = dict(day='day_in',
              month='month_in')

df = df_data_grabber('../Data/interview.dta', idxvars, **myvars)

# No year column in the source: the 2003 round was fielded in 2003.
df['Int_t'] = pd.to_datetime(
    dict(year=2003, month=df['month'], day=df['day']),
    errors='coerce',
)
df = df.drop(columns=['day', 'month']).dropna()

to_parquet(df, 'interview_date.parquet')
