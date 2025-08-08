#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
#2020
import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from tanzania import age_sex_composition

myvars = dict(fn='../Data/hh_sec_b.dta',
              HHID='y5_hhid',
              sex='hh_b02',
              age='hh_b04')

df = age_sex_composition(**myvars)

df = df.filter(regex='ales ')

df['log HSize'] = np.log(df.sum(axis=1))

# Drop any obs with infinities...
df = df.loc[np.isfinite(df.min(axis=1)),:]

#reformat
df = df.reset_index()
df.insert(1, 't', '2020-21')
df.set_index(['j','t'], inplace = True)

assert df.index.is_unique, "Non-unique index!  Fix me!"

to_parquet(df, 'household_characteristics.parquet')
