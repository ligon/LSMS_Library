#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from uganda import age_sex_composition

myvars = dict(fn='Uganda/2015-16/Data/gsec2.dta',
              HHID='hhid',
              sex='h2q3',
              age='h2q8',
              months_spent='h2q5')

df = age_sex_composition(**myvars)

df = df.filter(regex='ales ')

df['log HSize'] = np.log(df.sum(axis=1))

df.to_parquet('household_characteristics.parquet')
