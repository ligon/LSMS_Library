#!/usr/bin/env python
"""Build household_characteristics for Nepal 2010-11 (NLSS III).

Source: S01.dta (household roster)
Variables:
    xhpsu  - primary sampling unit (cluster)
    xhnum  - household number
    v01_02 - sex
    v01_03 - age
    v01_09 - months at home
"""
import numpy as np
from lsms.tools import get_household_roster
from lsms_library.local_tools import get_dataframe, to_parquet

df = get_dataframe('../Data/S01.dta')

Age_ints = ((0, 4), (4, 9), (9, 14), (14, 19), (19, 31), (31, 51), (51, 100))


def sex_converter(x):
    s = str(x).strip().lower()
    if s in ('male', '1', 'm'):
        return 'm'
    elif s in ('female', '2', 'f'):
        return 'f'
    return s


hh = get_household_roster(
    df,
    HHID='xhnum',
    sex='v01_02',
    age='v01_03',
    months_spent='v01_09',
    sex_converter=sex_converter,
    Age_ints=Age_ints,
    fn_type=None,
)

hh.index.name = 'i'
hh.columns.name = 'k'

hh = hh.filter(regex='ales ')
N = hh.sum(axis=1)
hh.loc[:, 'log HSize'] = np.log(N[N > 0])

to_parquet(hh, 'household_characteristics.parquet')
