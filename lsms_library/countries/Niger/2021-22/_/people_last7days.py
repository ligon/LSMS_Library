"""Build people_last7days for Niger EHCVM 2021-22 (GAP 3, item-level).

Two source files:
  s04a_me_ner2021.dta — the 7-day employment module (participation dummies).
  s01_me_ner2021.dta  — the individual roster (age for working_age).
Merged on (grappe, menage, membres__id) — the same person key
household_roster uses this wave.  Grain (t, i, pid); pid = membres__id;
i = EHCVM composite via niger.i.

As in 2018-19, EHCVM's 7-day module records only the three participation
DUMMIES (s04q06 farm, s04q07 own-business, s04q08 wage) and working_age
(roster Age >= 6); productive-work hours and the WB 7-day industry code are
not recorded, so farm_hrs / SB_hrs / wage_hrs and Industry are NA.  Build
shared with 2018-19 via niger.people_last7days_ehcvm (here the person key is
membres__id, not 2018-19's s01q00a).
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from niger import people_last7days_ehcvm, _finish_people_last7days


s04 = get_dataframe('../Data/s04a_me_ner2021.dta', convert_categoricals=False)
s01 = get_dataframe('../Data/s01_me_ner2021.dta', convert_categoricals=False)

df = people_last7days_ehcvm(s04, s01, '2021-22',
                            pid_col='membres__id', age_col='s01q04a',
                            survey_year=2021)
df = _finish_people_last7days(df, '2021-22')

assert len(df) > 0, 'people_last7days 2021-22 produced no rows'
to_parquet(df, 'people_last7days.parquet')
