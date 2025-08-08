import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

idxvars = dict(j='r_hhid',
                t=('round', lambda x: "2008-15"))


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
