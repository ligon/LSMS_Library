#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

import sys
sys.path.append('../../../_/')
import pandas as pd
import numpy as np
from lsms_library.local_tools import age_sex_composition

myvars = dict(fn='../Data/sect1_hh_w4.dta',
              HHID='household_id',
              sex='s1q02',
              age='s1q03a',
              sex_converter=lambda x: ['f','m'][x[0]=='1'])

df = age_sex_composition(**myvars)

mydf = df.copy()

df = df.filter(regex='ales ')

df['log HSize'] = np.log(df.sum(axis=1))

to_parquet(df, 'household_characteristics.parquet')
