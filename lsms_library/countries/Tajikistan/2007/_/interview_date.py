#!/usr/bin/env python
"""Tajikistan 2007 interview dates.

Source: ../Data/r1m0.dta.  Records dateday/datemont but NO year column; the
round was fielded in 2007, so the year is hardcoded.  The file covers 4644 of
the 4860 sample households; rows with an unparseable date are dropped.
Household id is `hhid` (matches sample()'s `i`).  timehour/timemin ignored.
"""
import sys
sys.path.append('../../../_')
import pandas as pd
from lsms_library.local_tools import df_data_grabber, to_parquet

idxvars = dict(t=('hhid', lambda x: '2007'),
               i='hhid')
myvars = dict(day='dateday',
              month='datemont')

df = df_data_grabber('../Data/r1m0.dta', idxvars, **myvars)

# No year column in the source: the 2007 round was fielded in 2007.
df['Int_t'] = pd.to_datetime(
    dict(year=2007, month=df['month'], day=df['day']),
    errors='coerce',
)
df = df.drop(columns=['day', 'month']).dropna()

to_parquet(df, 'interview_date.parquet')
