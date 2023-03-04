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

#df.hr_05_2 = df.hr_05_2.replace("DON'T KNOW",np.NaN)

#general hh dataset 
#with dvc.api.open('../Data/HH_SEC_A.dta',mode='rb') as dta:
    #date = from_dta(dta)

#calculate shock onset 
#df['hr_05_2'] = pd.to_datetime(df.hr_05_2, format='%B').dt.month
#date['ha_18_2'] = pd.to_datetime(date.ha_18_2, format='%B').dt.month
#df['start_date'] = pd.to_datetime(df.rename(columns={'hr_05_1': 'year', 'hr_05_2': 'month'})[['year', 'month']].assign(DAY=1)) #no day reported; assume 1st of the month
#date['end_date'] = pd.to_datetime(date.rename(columns={'ha_18_3': 'year', 'ha_18_2': 'month'})[['year', 'month']].assign(DAY=1)) #round the interview date to 1st of the month to match shock date


#merge 
#date = date[["round", "r_hhid", "end_date"]].drop_duplicates()
#df = df.merge(date.drop_duplicates(), how = 'inner', on = ['r_hhid', 'round'])
#df['Onset'] = (df.end_date.dt.to_period('M') - df.start_date.dt.to_period('M')).apply(lambda x: x.n if pd.notnull(x) else np.nan)

#y4 = df.loc[df['round']==4, 'r_hhid'].to_frame().rename(columns ={'r_hhid':'y4_hhid'})
#df = df.join(y4)
#formatting
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

shocks.to_parquet('shocks.parquet')
