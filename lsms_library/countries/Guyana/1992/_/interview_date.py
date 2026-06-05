import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, format_id, to_parquet

# Guyana 1992 cover page (COVERN.dta) records the enumeration date as
# day/month/year-of-enumeration (DDE/MDE/YDE).  The household key is the
# composite (ED, HH); we hyphen-join ED+HH to match sample's "ED-HH" form
# (e.g. "1-1").  v is the enumeration district ED.  YDE is stored 2-digit
# (uniformly 93 -> 1993): enumeration ran Mar-Aug 1993 even though the
# survey is nominally the 1992 round.
idxvars = dict(ED='ED', HH='HH')
myvars = dict(day='DDE', month='MDE', year='YDE')

df = df_data_grabber('../Data/COVERN.dta', idxvars, **myvars).reset_index()

# Composite household id "ED-HH" and cluster id v=ED (format_id each part).
df['i'] = df['ED'].map(format_id) + '-' + df['HH'].map(format_id)
df['v'] = df['ED'].map(format_id)
df['t'] = '1992'

# Expand 2-digit year of enumeration to 4-digit (e.g. 93 -> 1993).
year = df['year'].astype(int)
year = year.where(year >= 100, year + 1900)
df['Int_t'] = pd.to_datetime(dict(year=year,
                                  month=df['month'].astype(int),
                                  day=df['day'].astype(int)))

df = df.set_index(['t', 'i'])[['v', 'Int_t']]

to_parquet(df, 'interview_date.parquet')
