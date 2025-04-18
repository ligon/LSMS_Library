#!/usr/bin/env python
from lsms_library.local_tools import to_parquet


import pandas as pd
import numpy as np
import sys
sys.path.append('../../_/')
from uganda import age_sex_composition

myvars = dict(fn='../Data/GSEC2.dta',
              HHID='HHID',
              sex='h2q4',
              age='h2q9',
              months_spent='h2q6')

df = age_sex_composition(**myvars)

mydf = df.copy()

df = df.filter(regex='ales ')

df['log HSize'] = np.log(df.sum(axis=1))

to_parquet(df, 'household_characteristics.parquet')
