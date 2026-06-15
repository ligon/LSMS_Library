#!/usr/bin/env python
"""Interview dates for Pakistan 1991.

Source: F00A.DTA, interview-1 date triple dayi1/moi1/yri1 (the first-visit
date; the return-visit *r/*i2/*en/*pr triples are deliberately ignored).
The year is recorded 2-digit (e.g. 91), so add 1900 to recover 1991.
"""
import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

idxvars = dict(i='hid',
               t=('hid', lambda x: "1991"))

myvars = dict(day='dayi1',
              month='moi1',
              year='yri1')

df = df_data_grabber('../Data/F00A.DTA', idxvars, **myvars)

# 2-digit year -> 4-digit (91 -> 1991)
df['year'] = df['year'] + 1900

df['Int_t'] = pd.to_datetime(
    dict(year=df['year'], month=df['month'], day=df['day']),
    errors='coerce')

df = df.drop(columns=['day', 'month', 'year'])

to_parquet(df.dropna(), 'interview_date.parquet')
