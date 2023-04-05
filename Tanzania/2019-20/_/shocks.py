#!/usr/bin/env python

from calendar import month
import sys
sys.path.append('../../_/')
import pandas as pd
import dvc.api
from datetime import datetime
from lsms import from_dta
import numpy as np 

#shock dataset
with dvc.api.open('../Data/HH_SEC_R.dta',mode='rb') as dta:
    df = from_dta(dta)
df = df[df['hh_r01'] == 'YES'] #filter for valid entry

#formatting
shocks = pd.DataFrame({"j": df.sdd_hhid.values.tolist(),
                       "Shock":df.shock_id.values.tolist(),
                       "EffectedIncome&/Assets":df.hh_r03.values.tolist(),
                       "HowCoped0":df.hh_r04_1.values.tolist(),
                       "HowCoped1":df.hh_r04_2.values.tolist()})

shocks.insert(1, 't', '2019-20')

#converting data types 
shocks = shocks.astype({
                       "j": 'object',
                       "t": 'object',
                       'Shock': 'category',
                       "HowCoped0": 'category',
                       "HowCoped1": 'category',
                       "EffectedIncome&/Assets": 'category',
                       })

shocks.set_index(['j','t','Shock'], inplace = True)

assert shocks.index.is_unique, "Non-unique index!  Fix me!"

shocks.to_parquet('shocks.parquet')
