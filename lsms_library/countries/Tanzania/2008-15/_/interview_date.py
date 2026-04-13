import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

# Round-to-wave mapping matches the convention in 2008-15/_/shocks.py
# and 2008-15/_/sample.py: each integer round becomes the corresponding
# year-string wave label so that (i, t) is unique per (household, round).
# Without this map every round collapses onto t='2008-15' and 4 visits
# of the same household end up under one (i, t) key — a 68% duplicate
# rate, breaking test_no_duplicate_rows[Tanzania/interview_date].
round_match = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}

idxvars = dict(i='r_hhid',
                t=('round', lambda x: round_match.get(x, x)))


myvars = dict(year=('ha_18_3', lambda x: pd.to_numeric(x, errors='coerce')),
                month='ha_18_2',
                day=('ha_18_1',lambda x: pd.to_numeric(x, errors='coerce')))

df = df_data_grabber('../Data/upd4_hh_a.dta',idxvars,**myvars)

# Convert month names to month numbers, handling missing months by mapping to NaN
months_dict = {'JANUARY': 1, 'FEBRUARY': 2, 'MARCH': 3, 'APRIL': 4, 'MAY': 5, 'JUNE': 6,
               'JULY': 7, 'AUGUST': 8, 'SEPTEMBER': 9, 'OCTOBER': 10, 'NOVEMBER': 11, 'DECEMBER': 12, None: pd.NA}

df['month'] = df['month'].map(months_dict)
df['date'] = pd.to_datetime(df[['year', 'month', 'day']], errors='coerce')
df=df.drop(columns=['year','month','day'])

to_parquet(df,'interview_date.parquet')
