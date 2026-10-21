"""Interview date for Nigeria 2018-19 (GHS-Panel W4), both rounds.

W4 records the interview start as a full timestamp (``InterviewStart``,
``datetime64`` in the ``.dta``), where W1-W3 record a day/month/year triple
(``saq13d/m/y``).  The timestamp is stored as is -- a ``datetime64`` column,
time of day included -- so the wave concatenates with the other waves'
``datetime64`` ``int_t`` and ``_coerce_datetime_column`` passes it through.

GH #1037: this script used ``pd.to_datetime(x).date`` (no call), which stored
the TEXT of a bound method (``<bound method Timestamp.date of ...>``) in
every row; those strings parse to NaT at read time and ``dropna(how='all')``
deleted all 10,288 W4 rows.  The 52 rows whose ``InterviewStart`` is blank at
source are NaT here and are dropped like the other waves' blank dates.

The household id is the ``i`` level, as declared in ``data_scheme.yml``
(``(t, i)``) -- not ``j``, which the canonical schema reserves for the item.
"""
import numpy as np
import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

#Planting(2018Q3)
idxvars = dict(i='hhid',
               t=('hhid', lambda x: "2018Q3"),
               )

myvars = dict(int_t=('InterviewStart', lambda x: pd.to_datetime(x)))

df_planting = df_data_grabber('../Data/secta_plantingw4.dta',idxvars,**myvars)



#Harvest(2019Q1)
idxvars = dict(i='hhid',
               t=('hhid', lambda x: "2019Q1"),
               )

myvars = dict(int_t=('InterviewStart', lambda x: pd.to_datetime(x)))

df = df_data_grabber('../Data/secta_harvestw4.dta',idxvars,**myvars)


df=pd.concat([df_planting,df],axis=0)
df['int_t'] = pd.to_datetime(df['int_t'])
to_parquet(df,'interview_date.parquet')
