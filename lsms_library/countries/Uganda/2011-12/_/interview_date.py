#!/usr/bin/env python3
import sys
sys.path.append('../../../_/')
from lsms_library.datetime_parse import assign_datetime, wave_datetime_audit
from lsms_library.local_tools import df_data_grabber, to_parquet

idxvars = dict(j = 'HHID',
               t=('region',lambda x:'2011-12'),  # Note trivial mapping
               )
myvars = dict(day = 'day',
              month= 'month',
              year = 'year',
              )

df = df_data_grabber('../Data/GSEC1.dta', idxvars, **myvars)

# The original household cover records a Gregorian civil date (GH #1045).
components = df[list(myvars)].rename(columns={'year': 'y', 'month': 'm', 'day': 'd'})
df = assign_datetime(df.drop(columns=list(myvars)), 'Int_t', components,
                     calendar='gregorian', resolution='day', country='Uganda',
                     table='interview_date', wave_folder='2011-12')
to_parquet(df, 'interview_date.parquet',
           datetime_audit=wave_datetime_audit('Uganda', 'interview_date', '2011-12'))
