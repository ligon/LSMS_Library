#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from tanzania import age_sex_composition

myvars = dict(fn='../Data/SEC_1_ALL.dta',
              HHID='hhid',
              sex='s1q3',
              age='s1q2')

df = age_sex_composition(**myvars)

df = df.filter(regex='ales ')

df['log HSize'] = np.log(df.sum(axis=1))

# Drop any obs with infinities...
df = df.loc[np.isfinite(df.min(axis=1)),:]

df.to_parquet('household_characteristics.parquet')
