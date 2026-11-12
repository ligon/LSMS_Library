"""Build people_last7days for CotedIvoire EHCVM 2021-22 (GH #1697).

s04a_me_civ2021.dta holds the 7-day answers and carries one row per member, where 2018-19's
s04 is on the same grain: 9,575 rows here have no answer, and they are served with the
dummies NA.  The person key is membres__id, as in this wave's roster; its Stata label in s04a
is mistyped ("Qui est le répondant"), and it joins s01 on all 64,491.  working_age = Age >= 6,
Age the stated years else 2021 less the birth year, which is below 6 for the 394 children
born in 2022 too.  The module asks no hours and no industry on this grain, so those columns
are NA, as in 2018-19.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet
from mapping import household_ids

KEY = ['grappe', 'menage', 'membres__id']

s04 = get_dataframe('../Data/s04a_me_civ2021.dta', convert_categoricals=False)
s01 = get_dataframe('../Data/s01_me_civ2021.dta', convert_categoricals=False)
src = s04[KEY + ['s04q06', 's04q07', 's04q08']].merge(
    s01[KEY + ['s01q04a', 's01q03c']], on=KEY, how='left', validate='one_to_one')


def _yes_no(series):
    code = pd.to_numeric(series, errors='coerce')
    out = pd.Series(pd.NA, index=code.index, dtype='boolean')
    return out.mask(code == 1, True).mask(code == 2, False)


age = pd.to_numeric(src['s01q04a'], errors='coerce')
birth = pd.to_numeric(src['s01q03c'], errors='coerce')
age = age.where(age.notna(), 2021 - birth.where(birth.between(1900, 2022)))

df = pd.DataFrame({
    't': '2021-22',
    'i': household_ids(src).astype('string').values,
    'pid': src['membres__id'].apply(tools.format_id).astype('string').values,
    'farm_work': _yes_no(src['s04q06']).values,
    'SOB_work': _yes_no(src['s04q07']).values,
    'wage_work': _yes_no(src['s04q08']).values,
    'farm_hrs': pd.array([pd.NA] * len(src), dtype='Float64'),
    'SB_hrs': pd.array([pd.NA] * len(src), dtype='Float64'),
    'wage_hrs': pd.array([pd.NA] * len(src), dtype='Float64'),
    'Industry': pd.array([pd.NA] * len(src), dtype='string'),
    'working_age': (age >= 6).astype('boolean').where(age.notna(), pd.NA).values,
}).set_index(['t', 'i', 'pid'])

assert df.index.is_unique, 'people_last7days 2021-22 repeats a person'
assert len(df) == len(s04), 'people_last7days 2021-22 lost a person'
to_parquet(df, 'people_last7days.parquet')
