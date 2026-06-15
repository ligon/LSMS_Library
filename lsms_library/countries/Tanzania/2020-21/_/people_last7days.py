"""people_last7days for Tanzania NPS 2020-21 (NPS Y5 Refresh Panel;
parity-loop GAP 3).

Same HH_SEC_E1 module and variable names as 2019-20 (identical NPS
questionnaire); only the file name (lowercase) and the household / individual
id columns differ (y5_hhid / indidy5).  See
``tanzania.people_last7days_for_wave`` for the schema.  i/pid match the
household_roster keys; the country-level concatenator applies id_walk and the
framework joins ``v`` from sample().
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from tanzania import people_last7days_for_wave


sec = get_dataframe('../Data/hh_sec_e1.dta', convert_categoricals=False)

colmap = dict(hhid='y5_hhid', pid='indidy5')

df = people_last7days_for_wave('2020-21', sec, colmap)
assert df.index.is_unique, "people_last7days 2020-21: (t,i,pid) not unique"
assert len(df) > 0
to_parquet(df, 'people_last7days.parquet')
