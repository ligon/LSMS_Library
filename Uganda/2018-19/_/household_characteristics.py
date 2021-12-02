#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from uganda import age_sex_composition
import dvc.api

myvars = dict(fn='Uganda/2018-19/Data/GSEC2.dta',
              HHID='hhid',
              sex='h2q3',
              age='h2q8',
              months_spent='h2q5')

df = age_sex_composition(**myvars)

df = df.filter(regex='ales ')

N = df.sum(axis=1)

df['log HSize'] = np.log(N[N>0])

# Switch to hhid that is constant across waves...
with dvc.api.open('Uganda/2018-19/Data/GSEC1.dta',mode='rb') as dta:
    id = pd.read_stata(dta,convert_categoricals=False)

df.to_parquet('household_characteristics.parquet')
