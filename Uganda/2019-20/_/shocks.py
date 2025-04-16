from lsms_library.local_tools import to_parquet
#!/usr/bin/env python

from calendar import month
import sys
sys.path.append('../../_/')
import pandas as pd
import dvc.api
from datetime import datetime

#shock dataset
with dvc.api.open('../Data/HH/gsec16.dta',mode='rb') as dta:
         df = pd.read_stata(dta)
df = df[df['s16q02y'].notna()] #filter for valid entry 

#general hh dataset 
with dvc.api.open('../Data/HH/gsec1.dta',mode='rb') as dta:
         date = pd.read_stata(dta)
#filter for hhs who have taken the shock questionnaire 
date = date[date.set_index('hhid').index.isin(df.set_index('hhid').index)]


#calculate shock onset 
df['s16q02a'] = pd.to_datetime(df.s16q02a, format='%B').dt.month
df['start_date'] = pd.to_datetime(df.rename(columns={'s16q02y': 'year', 's16q02a': 'month'})[['year', 'month']].assign(DAY=1)) #no day reported; assume 1st of the month 
date['end_date'] = pd.to_datetime(date[['year', 'month']].assign(DAY=1)) #round the interview date to 1st of the month to match shock date
date = date[["hhid", "end_date"]]
df = pd.merge(df, date, on='hhid')
df['Onset'] = (df.end_date.dt.to_period('M') - df.start_date.dt.to_period('M')).apply(lambda x: x.n)


shocks = pd.DataFrame({"j": df.hhid.values.tolist(),
                    "Shock":df.s16qa01.values.tolist(), 
                    "Year": df.s16q02y.values.tolist(), 
                    "Onset":df.Onset.values.tolist(), 
                    "Duration":df.s16q02b.values.tolist(),
                    "EffectedIncome":df.s10q03a.values.tolist(), 
                    "EffectedAssets":df.s16q03b.values.tolist(), 
                    "EffectedProduction":df.s16q03c.values.tolist(), 
                    "EffectedConsumption":df.s16q03d.values.tolist(), 
                    "HowCoped0":df.s16q04a.values.tolist(),
                    "HowCoped1":df.s16q04b.values.tolist(),
                    "HowCoped2":df.s16q04c.values.tolist()})

#converting data types 
for col in ["EffectedIncome",  "EffectedAssets",  "EffectedProduction", "EffectedConsumption"]:
    shocks[col] = shocks[col].map({"Yes": True,"No": False})
    shocks[col] = shocks[col].astype('boolean')
shocks = shocks.astype({'Shock': 'category',
                        'Year': 'Int64',
                        'Onset': 'Int64',
                        'Duration': 'float',
                        "HowCoped0": 'category',
                        "HowCoped1": 'category',
                        "HowCoped2": 'category'
                        }) 

shocks.insert(1, 't', '2019-20')

shocks.set_index(['j','t','Shock'], inplace = True)



to_parquet(shocks, 'shocks.parquet')