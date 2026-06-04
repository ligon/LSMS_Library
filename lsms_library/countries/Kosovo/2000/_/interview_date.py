import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

idxvars = dict(t=('s0i_dat0', lambda x: "2000"),
               i='hhid')
myvars = dict(s0i_dat0='s0i_dat0')

df = df_data_grabber('../Data/ID.dta', idxvars, **myvars)
# A handful of records carry impossible calendar dates (e.g. 20001032,
# 20001200, 20001100 -- day/month out of range). Coerce those to NaT.
df['Int_t'] = pd.to_datetime(df['s0i_dat0'].astype('Int64').astype(str),
                             format='%Y%m%d', errors='coerce')
df = df.drop(columns=['s0i_dat0'])

to_parquet(df, 'interview_date.parquet')
