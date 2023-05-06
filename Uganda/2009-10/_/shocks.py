#!/usr/bin/env python

from calendar import month
from stat import SF_APPEND
import sys
sys.path.append('../../../_/')
from local_tools import to_parquet
import pandas as pd
import dvc.api
from datetime import datetime
from lsms import from_dta

#shock dataset
with dvc.api.open('../Data/GSEC16.dta',mode='rb') as dta:
    df = from_dta(dta)
df = df[df['h16q02a'].notna()] #filter for valid entry 

#general hh dataset 
with dvc.api.open('../Data/GSEC1.dta',mode='rb') as dta:
    date = from_dta(dta)

#filter for hhs who have taken the shock questionnaire 
date = date[date.set_index('HHID').index.isin(df.set_index('HHID').index)] 


#calculate shock onset 
df['h16q02a'] = pd.to_datetime(df.h16q02a.str[:3], format='%b').dt.month #convert categorical to numerical months 
date['end_date'] = pd.to_datetime(date.rename(columns={'h1bq2c': 'year', 'h1bq2b': 'month'})[['year', 'month']].assign(DAY=1)) #round the interview date to 1st of the month to match shock date
date = date[["HHID", "end_date"]]
date = date[date['end_date'].notna()]
df = pd.merge(df, date, on='HHID')

def shock_yr(row): #derive shock year based on assumptions 
    if row['h16q02b'] > 12: #if duration is longer than 12 month
        #suppose onset must >= duration
        if (row["end_date"].month - row['h16q02a']) < (row['h16q02b'] - 12): #shocks with +1yr duration but ended before interview date 
            return row["end_date"].year - (row['h16q02b'] // 12) + 1 #add only one additional year because otherwise the shock won't be still ongoing during the last 12 months 
        else: #shocks with +1yr duration and still ongoing 
            return row["end_date"].year - row['h16q02b'] // 12

    elif row['h16q02b'] == 12: #if duration is 12 month
        if (row['h16q02a'] - row["end_date"].month) > 1: #shocks with +1yr duration, ended in the year before interview year  
            return row["end_date"].year - 2
        else: #shocks with +1yr duration, ended/ongoing in the interview year
            return row["end_date"].year - 1

    else: #if duration is less than 12 month 
        if row['h16q02a'] < row["end_date"].month: 
            #if shock_month <= interview_month, then the shock must've happened within the interview year
            return row["end_date"].year
        elif row['h16q02a'] > row["end_date"].month:
            return row ["end_date"].year - 1
        elif row['h16q02a'] == row["end_date"].month:
            if row['h16q02b'] <=1:
                return row["end_date"].year
            else:
                return row ["end_date"].year - 1

df['shock_year'] = df.apply(shock_yr, axis=1)

df['start_date'] = pd.to_datetime(df.rename(columns={'shock_year': 'year', 'h16q02a': 'month'})[['year', 'month']].assign(DAY=1)) #no day reported; assume 1st of the month 
df['Onset'] = (df.end_date.dt.to_period('M') - df.start_date.dt.to_period('M')).apply(lambda x: x.n)


shocks = pd.DataFrame({"j": df.HHID.values.tolist(),
                    "Shock":df.h16q00.values.tolist(), 
                    "Year": df.shock_year.tolist(),
                    "Onset":df.Onset.values.tolist(), 
                    "Duration":df.h16q02b.values.tolist(),
                    "EffectedIncome":df.h16q3a.values.tolist(), 
                    "EffectedAssets":df.h16q3b.values.tolist(), 
                    "EffectedProduction":df.h16q3c.values.tolist(), 
                    "EffectedConsumption":df.h16q3d.values.tolist(), 
                    "HowCoped0":df.h16q4a.values.tolist(),
                    "HowCoped1":df.h16q4b.values.tolist(),
                    "HowCoped2":df.h16q4c.values.tolist()})

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

shocks.insert(1, 't', '2009-10')
shocks.set_index(['j','t','Shock'], inplace = True)

to_parquet(shocks,'shocks.parquet')
