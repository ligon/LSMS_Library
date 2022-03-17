#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from uganda import age_sex_composition

myvars = dict(fn='Uganda/2005-06/Data/GSEC2.dta',
              HHID='HHID',
              sex='h2q4',
              age='h2q9',
              months_spent='h2q6')

df = age_sex_composition(**myvars)

mydf = df.copy()

df = df.filter(regex='ales ')

df['log HSize'] = np.log(df.sum(axis=1))

df.to_parquet('household_characteristics.parquet')
