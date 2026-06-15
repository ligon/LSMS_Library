#!/usr/bin/env python
"""Tajikistan 1999 interview dates.

Source: ../Data/SSEC1.DTA (one row per *person*; 14142 rows).  The interview
date (date_day/date_mth/date_yr) is recorded identically for every member of a
household, so we deduplicate to one row per household.  date_yr is a 2-digit
year (99 -> 1999).  The household id matches sample()'s `i`: 'pop_pt-hhid'.
"""
import sys
sys.path.append('../../../_')
sys.path.append('../_')
import pandas as pd
from lsms_library.local_tools import df_data_grabber, to_parquet
from mapping import i as format_i

idxvars = dict(t=('hhid', lambda x: '1999'),
               i=(['pop_pt', 'hhid'], format_i))
myvars = dict(day='date_day',
              month='date_mth',
              year='date_yr')

df = df_data_grabber('../Data/SSEC1.DTA', idxvars, **myvars)

# date_yr is 2-digit (99); map to 4-digit calendar year.
df['year'] = df['year'].astype(float) + 1900

df['Int_t'] = pd.to_datetime(
    dict(year=df['year'], month=df['month'], day=df['day']),
    errors='coerce',
)
df = df.drop(columns=['day', 'month', 'year'])

# One interview date per household.
df = df[~df.index.duplicated(keep='first')].dropna()

to_parquet(df, 'interview_date.parquet')
