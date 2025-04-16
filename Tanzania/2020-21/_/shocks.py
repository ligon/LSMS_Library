from lsms_library.local_tools import to_parquet
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
with dvc.api.open('../Data/hh_sec_r.dta',mode='rb') as dta:
    df = from_dta(dta)
df = df[df['hh_r01'] == 'yes'] #filter for valid entry

shocks = pd.DataFrame({"j": df.y5_hhid.values.tolist(),
                       "Shock":df.shockid.values.tolist(),
                       "EffectedIncome&/Assets":df.hh_r03.values.tolist(),
                       "HowCoped0":df.hh_r04_1.values.tolist(),
                       "HowCoped1":df.hh_r04_2.values.tolist()})

shocks.insert(1, 't', '2020-21')

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

assert df.index.is_unique, "Non-unique index!  Fix me!"

to_parquet(shocks, 'shocks.parquet')
