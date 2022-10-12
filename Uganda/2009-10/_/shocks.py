#!/usr/bin/env python

from calendar import month
import sys
sys.path.append('../../_/')
import pandas as pd
import dvc.api
from datetime import datetime


modelpkl = dvc.api.read(
    'GSEC16.dta.dvc',
    repo='../Data/',
    mode='rb'
)

#shock dataset
with dvc.api.open('../Data/GSEC16.dta.dvc',mode='rb') as dta:
         df = pd.read_stata(dta)
df = df[df['h16q02a'].notna()] #filter for valid entry 

#general hh dataset 
with dvc.api.open('../Data/GSEC1.dta.dvc',mode='rb') as dta:
         date = pd.read_stata(dta)
#filter for hhs who have taken the shock questionnaire 
date = date[date.set_index('HHID').index.isin(df.set_index('HHID').index)]

#calculate shock onset 
df['h16q02a'] = pd.to_datetime(df.h16q02a, format='%B').dt.month
df['start_date'] = pd.to_datetime(df.rename(columns={'???': 'year', 'h16q02a': 'month'})[['year', 'month']].assign(DAY=1))
date['end_date'] = pd.to_datetime(date[['h1bq2c', 'h1bq2b']].assign(DAY=1))
date = date[["HHID", "end_date"]]
df = pd.merge(df, date, on='HHID')
df['Onset'] = (df.end_date.dt.to_period('M') - df.start_date.dt.to_period('M')).apply(lambda x: x.n)

shocks = pd.DataFrame({"i": df.HHID.values.tolist(),
                    "Shock":df.h16q00.values.tolist(), 
                    "Onset":df.Onset.values.tolist(), 
                    "Duration":df.h16q02b.values.tolist(),
                    "EffectedIncome":df.h16q3a.values.tolist(), 
                    "EffectedAssets":df.h16q3b.values.tolist(), 
                    "EffectedProduction":df.h16q3c.values.tolist(), 
                    "EffectedConsumption":df.h16q3d.values.tolist(), 
                    "HowCoped0":df.h16q4a.values.tolist(),
                    "HowCoped1":df.h16q4b.values.tolist(),
                    "HowCoped2":df.h16q4c.values.tolist()})
shocks.insert(1, 't', '2009-10')
shocks.set_index(['i','t','Shock'])

shocks.to_parquet('shocks.parquet')