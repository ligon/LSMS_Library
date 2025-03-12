#!/usr/bin/env python

import sys
sys.path.append('../../../_/')
import pandas as pd
import numpy as np
from lsms_library.local_tools import age_sex_composition

myvars = dict(fn='../Data/sect1_hh_w3.dta',
              HHID='household_id',
              sex='hh_s1q03',
              age='hh_s1q04a')

df = age_sex_composition(**myvars)

mydf = df.copy()

df = df.filter(regex='ales ')

df['log HSize'] = np.log(df.sum(axis=1))

df.to_parquet('household_characteristics.parquet')
