#!/usr/bin/env python

from calendar import month
import sys
sys.path.append('../../_/')
import pandas as pd
import dvc.api
from datetime import datetime
from lsms import from_dta


#shock dataset
with dvc.api.open('../Data/sect8_hh_w1.dta',mode='rb') as dta:
    df = from_dta(dta)
df = df[df['hh_s8q01'] == 'Yes'] #filter for valid entry 

shocks = pd.DataFrame({"j": df.household_id.values.tolist(),
                    "Shock":df.hh_s8q00.values.tolist(), 
                    "EffectedIncome":df.hh_s8q03_a.values.tolist(), 
                    "EffectedAssets":df.hh_s8q03_b.values.tolist(), 
                    "EffectedProduction":df.hh_s8q03_c.values.tolist(), 
                    "EffectedConsumption":df.hh_s8q03_e.values.tolist(), 
                    "EffectedFoodStock":df.hh_s8q03_d.values.tolist(), 
                    "HowCoped0":df.hh_s8q04_a.values.tolist(),
                    "HowCoped1":df.hh_s8q04_b.values.tolist(),
                    "HowCoped2":df.hh_s8q04_c.values.tolist(),
                    "Occurrence":df.hh_s8q05.values.tolist() #how many times did shock occur in the last year
                    })

#converting data types 
for col in ["EffectedIncome",  "EffectedAssets",  "EffectedProduction", "EffectedConsumption", "EffectedFoodStock"]:
    shocks[col] = shocks[col].map({"Decrease": True,"Increase": False, "Did Not Change": False})
    shocks[col] = shocks[col].fillna(False).astype('boolean')
shocks = shocks.astype({'Shock': 'category',
                        "HowCoped0": 'category',
                        "HowCoped1": 'category',
                        "HowCoped2": 'category',
                        'Occurrence': 'Int64',
                        }) 

shocks.insert(1, 't', '2011-12')
shocks.set_index(['j','t','Shock'], inplace = True)

shocks.to_parquet('shocks.parquet')
