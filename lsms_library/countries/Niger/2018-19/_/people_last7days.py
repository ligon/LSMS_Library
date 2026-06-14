"""Build people_last7days for Niger EHCVM 2018-19 (GAP 3, item-level).

Two source files:
  s04_me_ner2018.dta — the 7-day employment module (participation dummies).
  s01_me_ner2018.dta — the individual roster (age for working_age).
Merged on (grappe, menage, s01q00a) — the same person key household_roster
uses.  Grain (t, i, pid); pid = s01q00a; i = EHCVM composite via niger.i.

EHCVM's 7-day module records only the three participation DUMMIES in an
ECVMA-comparable form (s04q06 farm, s04q07 own-business, s04q08 wage) and
working_age (roster Age >= 6).  It does NOT record productive-work hours or
the WB 7-day industry section code, so farm_hrs / SB_hrs / wage_hrs and
Industry are NA (declared for cross-wave schema parity).  Build shared with
2021-22 via niger.people_last7days_ehcvm.
"""
import sys

sys.path.append('../../_/')
from lsms_library.local_tools import get_dataframe, to_parquet
from niger import people_last7days_ehcvm, _finish_people_last7days


s04 = get_dataframe('../Data/s04_me_ner2018.dta', convert_categoricals=False)
s01 = get_dataframe('../Data/s01_me_ner2018.dta', convert_categoricals=False)

df = people_last7days_ehcvm(s04, s01, '2018-19', survey_year=2018)
df = _finish_people_last7days(df, '2018-19')

assert len(df) > 0, 'people_last7days 2018-19 produced no rows'
to_parquet(df, 'people_last7days.parquet')
