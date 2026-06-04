import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

# 2006-07 cover page: visit-1 interview date in q0035d/q0035m/q0035y
# (4-digit year). i = xhhkey.  v is joined from sample() at API time.
idxvars = dict(i='xhhkey',
               t=('xhhkey', lambda x: "2006-07"))
myvars = dict(day='q0035d',
              month='q0035m',
              year='q0035y')

df = df_data_grabber('../Data/2007ihses00_cover_page.dta', idxvars, **myvars)
df['Int_t'] = pd.to_datetime(df[['year', 'month', 'day']])
df = df.drop(columns=['year', 'month', 'day'])

to_parquet(df, 'interview_date.parquet')
