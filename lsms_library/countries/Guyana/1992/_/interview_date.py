import pandas as pd
import sys
sys.path.append('../../../_/')
from lsms_library.local_tools import df_data_grabber, format_id, to_parquet

# Guyana 1992 cover page (COVERN.dta) records the enumeration date as
# day/month/year-of-enumeration (DDE/MDE/YDE).  The household key is the
# THREE-level composite (ED, SN, HH) -- SN is the ED sample-segment serial, and
# without it distinct households collide: COVERN has 305 duplicate (ED,HH) pairs
# but ZERO duplicate (ED,SN,HH) triples (GH #323).  We hyphen-join ED+SN+HH to
# match the "ED-SN-HH" form sample/roster/housing use (e.g. "5-194-2").  YDE is
# stored 2-digit (uniformly 93 -> 1993): enumeration ran Mar-Aug 1993 even
# though the survey is nominally the 1992 round.
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

# The declared index MUST be unique.  A non-unique (t, i) here would be silently
# collapsed by _normalize_dataframe_index's groupby().first() (GH #323), so fail
# loudly instead of shipping fused households.
dups = int(df.index.duplicated().sum())
assert dups == 0, (
    f"Guyana 1992 interview_date: {dups} duplicate (t, i) tuple(s) -- the "
    f"household key is (ED, SN, HH); a duplicate means SN was dropped or the "
    f"source changed.")

to_parquet(df, 'interview_date.parquet')
