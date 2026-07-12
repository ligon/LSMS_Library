import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, format_id, to_parquet

# Guyana 1992 cover page (COVERN.dta) records the enumeration date as
# day/month/year-of-enumeration (DDE/MDE/YDE).  The household key is the
# composite (ED, SN, HH) -- NOT (ED, HH), which conflates distinct households
# (GH #503): COVERN's 1807 rows hold 1807 unique triples but only 1502 unique
# pairs, and the survey's own NEWID == ED*100000 + SN*100 + HH for all 1807.
# Under the old pair key this table silently picked ONE of two different
# households' enumeration dates for 305 households (e.g. i='1-1' was both
# 18-Jun-93 and 22-Mar-93).  We hyphen-join ED+SN+HH to match sample's
# "ED-SN-HH" form (e.g. "1-37-1").  YDE is stored 2-digit (uniformly 93 ->
# 1993): enumeration ran Mar-Aug 1993 even though the survey is nominally the
# 1992 round.
idxvars = dict(ED='ED', SN='SN', HH='HH')
myvars = dict(day='DDE', month='MDE', year='YDE')

df = df_data_grabber('../Data/COVERN.dta', idxvars, **myvars).reset_index()

# Composite household id "ED-SN-HH" (format_id each part).
df['i'] = (df['ED'].map(format_id) + '-' + df['SN'].map(format_id)
           + '-' + df['HH'].map(format_id))
df['t'] = '1992'

# Expand 2-digit year of enumeration to 4-digit (e.g. 93 -> 1993).
year = df['year'].astype(int)
year = year.where(year >= 100, year + 1900)
df['Int_t'] = pd.to_datetime(dict(year=year,
                                  month=df['month'].astype(int),
                                  day=df['day'].astype(int)))

# Do NOT emit `v` as a column: the framework joins it from sample() into
# the index at API time (_join_v_from_sample).  Emitting it as a column
# suppresses that join and leaves the index at (t, i) (GH #325).
df = df.set_index(['t', 'i'])[['Int_t']]

to_parquet(df, 'interview_date.parquet')
