import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

# India 1997-98: interview date in SECT00.DTA.
# intdate=day, intmonth=month, intyear is 2-DIGIT (98 -> 1998).
# i=hhcode (matches sample 100%; the roster's i:hh is a latent bug -> use hhcode).
# v (village) is NOT baked in here; it is joined from sample() at API time by
# _join_v_from_sample (see CLAUDE.md: only cluster_features owns v).
idxvars = dict(i='hhcode',
               t=('hhcode', lambda x: "1997-98"))

myvars = dict(day='intdate',
              month='intmonth',
              year='intyear')

df = df_data_grabber('../Data/SECT00.DTA', idxvars, **myvars)

# 2-digit year -> 4-digit (97 -> 1997, 98 -> 1998).
df['year'] = df['year'].astype('Int64') + 1900
df['Int_t'] = pd.to_datetime(df[['year', 'month', 'day']], errors='coerce')
df = df.drop(columns=['year', 'month', 'day'])

to_parquet(df, 'interview_date.parquet')
