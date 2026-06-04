import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

# 2012 cover page: visit-1 interview date in q00_35_1d/m/y (4-digit year;
# preferred over the q00_31 interviewer date).  i = questid.
# v is joined from sample() at API time.
idxvars = dict(i='questid',
               t=('questid', lambda x: "2012"))
myvars = dict(day='q00_35_1d',
              month='q00_35_1m',
              year='q00_35_1y')

df = df_data_grabber('../Data/2012ihses00_cover_page.dta', idxvars, **myvars)
df['Int_t'] = pd.to_datetime(df[['year', 'month', 'day']])
df = df.drop(columns=['year', 'month', 'day'])

to_parquet(df, 'interview_date.parquet')
