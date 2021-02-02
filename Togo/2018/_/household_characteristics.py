#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from togo import age_sex_composition

myvars = dict(fn='Togo/2018/Data/Togo_survey2018_hhroster_forEthan.dta',
              HHID='hhid',
              sex='gender',
              sex_converter = lambda x:['m','f'][x=='woman'],
              age='age',
              months_spent=None)

df = age_sex_composition(**myvars)

df = df.filter(regex='ales ')

N = df.sum(axis=1)

df['log HSize'] = np.log(N[N>0])

df.to_parquet('household_characteristics.parquet')
