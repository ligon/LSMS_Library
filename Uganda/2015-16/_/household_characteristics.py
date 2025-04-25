#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from uganda import age_sex_composition

myvars = dict(fn='../Data/gsec2.dta',
              HHID='hhid',
              sex='h2q3',
              age='h2q8',
              months_spent='h2q5')

df = age_sex_composition(**myvars)

df = df.filter(regex='ales ')

N = df.sum(axis=1)

df['log HSize'] = np.log(N[N>0])

to_parquet(df, 'household_characteristics.parquet')
