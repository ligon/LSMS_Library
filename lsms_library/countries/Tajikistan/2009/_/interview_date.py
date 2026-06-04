#!/usr/bin/env python
"""Tajikistan 2009 interview dates.

Source: ../Data/m0.dta.  Records HH4_D (day) and HH4_M (month).  The recipe
expected a usable HH4_Y year column, but in this file HH4_Y is entirely blank
(every value is the string ' '), so the year is hardcoded to 2009 (the round's
fielding year; months present are 10/11, consistent with an Oct-Nov 2009
fieldwork window).  Household id is `HHID` (matches sample()'s `i`).
"""
import sys
sys.path.append('../../../_')
import pandas as pd
from lsms_library.local_tools import df_data_grabber, to_parquet

idxvars = dict(t=('HHID', lambda x: '2009'),
               i='HHID')
myvars = dict(day='HH4_D',
              month='HH4_M')

df = df_data_grabber('../Data/m0.dta', idxvars, **myvars)

# HH4_Y is blank in the source; the 2009 round was fielded in 2009.
df['Int_t'] = pd.to_datetime(
    dict(year=2009, month=df['month'], day=df['day']),
    errors='coerce',
)
df = df.drop(columns=['day', 'month']).dropna()

to_parquet(df, 'interview_date.parquet')
