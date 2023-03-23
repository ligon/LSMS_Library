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
with dvc.api.open('../Data/upd4_hh_r.dta',mode='rb') as dta: 
    df = from_dta(dta)
df = df[df['hr_01'] == 'YES'] #filter for valid entry

df.hr_05_2 = df.hr_05_2.replace("DON'T KNOW",np.NaN)

#general hh dataset 
with dvc.api.open('../Data/upd4_hh_a.dta',mode='rb') as dta: 
    date = from_dta(dta) 

#calculate shock onset 
df['hr_05_2'] = pd.to_datetime(df.hr_05_2, format='%B').dt.month
date['ha_18_2'] = pd.to_datetime(date.ha_18_2, format='%B').dt.month
df['start_date'] = pd.to_datetime(df.rename(columns={'hr_05_1': 'year', 'hr_05_2': 'month'})[['year', 'month']].assign(DAY=1)) #no day reported; assume 1st of the month 
date['end_date'] = pd.to_datetime(date.rename(columns={'ha_18_3': 'year', 'ha_18_2': 'month'})[['year', 'month']].assign(DAY=1)) #round the interview date to 1st of the month to match shock date


#merge 
date = date[["round", "UPHI", "end_date"]].drop_duplicates()
df = df.merge(date.drop_duplicates(), how = 'inner', on = ['UPHI', 'round'])
df['Onset'] = (df.end_date.dt.to_period('M') - df.start_date.dt.to_period('M')).apply(lambda x: x.n if pd.notnull(x) else np.nan)

#y4 = df.loc[df['round']==4, 'r_hhid'].to_frame().rename(columns ={'r_hhid':'y4_hhid'})
#df = df.join(y4)

#formatting
shocks = pd.DataFrame({"j": df.UPHI.values.tolist(),
                       #"hhid": df.r_hhid.values.tolist(), 
                       "t": df['round'].values.tolist(),
                       #'y4_hhid': df.y4_hhid.values.tolist(),
                       "Shock":df.hr_00.values.tolist(), 
                       "Year": df.hr_05_1.values.tolist(),
                       "Onset":df.Onset.values.tolist(), 
                       "EffectedIncome&/Assets":df.hr_03.values.tolist(), 
                       "Dispersion":df.hr_04.values.tolist(), 
                       "HowCoped0":df.hr_06_1.values.tolist(),
                       "HowCoped1":df.hr_06_2.values.tolist(),
                       "HowCoped2":df.hr_06_3.values.tolist()})

dict = {1:'2008-09', 2:'2010-11', 3:'2012-13', 4:'2014-15'}
shocks.replace({"t": dict},inplace=True)

#converting data types 
shocks = shocks.astype({"j": 'object',
                       #"hhid": 'object',
                       "t": 'object',
                       #"y4_hhid": 'object',
                       'Shock': 'category',
                       'Year': 'Int64',
                       'Onset': 'Int64',
                       "HowCoped0": 'category',
                       "HowCoped1": 'category',
                       "HowCoped2": 'category',
                       "EffectedIncome&/Assets": 'category', 
                       "Dispersion": 'category'
                       }) 

shocks['j'] = shocks['j'].astype(int).astype(str)
shocks.set_index(['j','t','Shock'], inplace = True)

assert df.index.is_unique, "Non-unique index!  Fix me!"

shocks.to_parquet('shocks.parquet')
