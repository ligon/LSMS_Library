#!/usr/bin/env python

from calendar import month
import sys
sys.path.append('../../_/')
import pandas as pd
import dvc.api
from datetime import datetime
from lsms import from_dta


#shock dataset
with dvc.api.open('../Data/sect9_hh_w4.dta',mode='rb') as dta:
    df = from_dta(dta)
df = df[df['s9q01'] == '1. YES'] #filter for valid entry 

shocks = pd.DataFrame({"j": df.household_id.values.tolist(),
                    "Shock":df.shock_type.values.tolist(), 
                    "EffectedIncome":df.s9q03a.values.tolist(), 
                    "EffectedAssets":df.s9q03b.values.tolist(), 
                    "EffectedProduction":df.s9q03c.values.tolist(), 
                    "EffectedConsumption":df.s9q03e.values.tolist(), 
                    "EffectedFoodStock":df.s9q03d.values.tolist(), 
                    "HowCoped0":df.s9q04_1.values.tolist(),
                    "HowCoped1":df.s9q04_2.values.tolist(),
                    "HowCoped2":df.s9q04_3.values.tolist(),
                    })

#converting data types 
for col in ["EffectedIncome",  "EffectedAssets",  "EffectedProduction", "EffectedConsumption", "EffectedFoodStock"]:
    shocks[col] = shocks[col].map({"2. DECREASED": True,"1. INCREASED": False, "3. DID NOT CHANGE": False})
    shocks[col] = shocks[col].fillna(False).astype('boolean')
shocks = shocks.astype({'Shock': 'category',
                        "HowCoped0": 'category',
                        "HowCoped1": 'category',
                        "HowCoped2": 'category'
                        }) 

shocks.insert(1, 't', '2018-19')
shocks.set_index(['j','t','Shock'], inplace = True)

shocks.to_parquet('shocks.parquet')
