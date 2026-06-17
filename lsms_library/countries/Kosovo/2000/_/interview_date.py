import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, to_parquet

# Issue #475: s0i_dat0/s0i_dat1 are a CSPro case open/close MACHINE timestamp
# (the two are ~identical), NOT the fieldwork interview date.  The genuine
# questionnaire interview date is field s0i_q22, recorded as day (s0i_q22a),
# month (s0i_q22b) and year (s0i_q22c is the constant literal "Year"; the
# survey is single-round 2000, so the year is fixed at 2000).  The packed
# s0i_q22 integer (DDMMYYYY) drops a digit for a couple of records, so we
# build Int_t from the separate day/month components instead.
idxvars = dict(t=('s0i_q22a', lambda x: "2000"),
               i='hhid')
myvars = dict(s0i_q22a='s0i_q22a', s0i_q22b='s0i_q22b')

df = df_data_grabber('../Data/ID.dta', idxvars, **myvars)
# Out-of-range day/month components (day > 31, month > 12) coerce to NaT.
# A few records carry an in-range but implausible date (e.g. January in a
# Sep-Dec fieldwork survey -- a likely data-entry typo); these are VALID
# calendar dates, so they are preserved as recorded -- we never fabricate or
# silently "repair" a date.
df['Int_t'] = pd.to_datetime(
    dict(year=2000,
         month=df['s0i_q22b'].astype('Int64'),
         day=df['s0i_q22a'].astype('Int64')),
    errors='coerce')
df = df.drop(columns=['s0i_q22a', 's0i_q22b'])

to_parquet(df, 'interview_date.parquet')
