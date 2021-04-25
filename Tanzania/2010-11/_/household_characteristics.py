#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from tanzania import age_sex_composition

myvars = dict(fn='Tanzania/2010-11/Data/HH_SEC_B.dta',
              HHID='y2_hhid',
              sex='hh_b02',
              age='hh_b04')

df = age_sex_composition(**myvars)

df = df.filter(regex='ales ')

df['log HSize'] = np.log(df.sum(axis=1))

# Drop any obs with infinities...
df = df.loc[np.isfinite(df.min(axis=1)),:]

df.to_parquet('household_characteristics.parquet')
