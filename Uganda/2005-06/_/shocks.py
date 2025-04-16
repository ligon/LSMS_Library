from lsms_library.local_tools import to_parquet
#!/usr/bin/env python

from calendar import month
from stat import SF_APPEND
import sys
sys.path.append('../../_/')
import pandas as pd
import dvc.api
import numpy as np 
from datetime import datetime
from lsms import from_dta

#shock dataset
with dvc.api.open('../Data/GSEC16.dta',mode='rb') as dta:
    df = from_dta(dta)
df = df[df['h16q5'].notna()] #filter for valid entry 

#general hh dataset 
with dvc.api.open('../Data/GSEC1.dta',mode='rb') as dta:
    date = from_dta(dta)

#filter for hhs who have taken the shock questionnaire 
date = date[date.set_index('HHID').index.isin(df.set_index('HHID').index)] 


#calculate shock onset 
#date['end_date'] = pd.to_datetime(date.rename(columns={'h1bq2c': 'Year', 'h1bq2b': 'month'})[['year', 'month']].assign(DAY=1)) #round the interview date to 1st of the month to match shock date
#date = date[["HHID", "end_date"]]
#date = date[date['end_date'].notna()]
#df = pd.merge(df, date, on='HHID')

#rename shock labels to match those of other years 
s = {'drought': 'Drought',
    'floods/hailstorm': 'Floods', 
    'pest attack': 'Unusually High Level of Crop Pests &amp; Disease', 
    'bad seed quality': 'Unusually High Costs of Agricultural Inputs', 
    'livestock epidemic': 'Unusually High Level of Livestock Disease',
    'fire accident': 'Fire',
    'civil strife': 'Conflict/Violence',
    'robbery/theft': 'Theft',
    'death of head of the hh': 'Death of Income Earner(s)',
    'death of other hh members': 'Death of Other Household Member(s)',
    'injury from accident': 'Serious Illness or Accident of Income Earner(s)',
    'others': 'Other (Specify)'
     }
df.h16q2 = df.h16q2.map(s)

#rename coping strategy labels to match those of other years 
d = {'mortgage': 'Rented out land/building',
    'sell assets': 'Sold assets', 
    'use savings': 'Relied on savings', 
    'send children to live else where': 'Sent children to live elsewhere', 
    'migration': 'Household member(s) migrated',
    'formal borrowing': 'Obtained credit',
    'informal borrowing': 'Obtained credit',
    'reduce consumption': 'Reduced consumption',
    'help provided by relatives and friends': 'Unconditional help provided by relatives/friends',
    'help provided from local government': 'Unconditional help provided by local government',
    'more wage employment': 'More wage employment',
    'change crop choices to avoid bad weather or pest attack': 'Changed cropping practices (crop choices or technology)',
    'improve technology': 'Changed cropping practices (crop choices or technology)',
    'increased agriculture labour supply': 'Increased agriculture labour supply',
    'other': 'Other (specify)'
     }
df.h16q7a = df.h16q7a.map(d)
df.h16q7b = df.h16q7b.map(d)
df.h16q7c = df.h16q7c.map(d)


shocks = pd.DataFrame({"j": df.HHID.values.tolist(),
                    "Shock":df.h16q2.values.tolist(), 
                    "Year": df.h16q5.tolist(),
                    "Onset": np.nan, 
                    "Duration":df.h16q6.values.tolist(),
                    "EffectedIncome": np.nan, 
                    "EffectedAssets": np.nan, 
                    "EffectedProduction": np.nan, 
                    "EffectedConsumption": np.nan, 
                    "HowCoped0":df.h16q7a.values.tolist(),
                    "HowCoped1":df.h16q7b.values.tolist(),
                    "HowCoped2":df.h16q7c.values.tolist()})
shocks.insert(1, 't', '2005-06')
shocks.set_index(['j','t','Shock'], inplace = True)

to_parquet(shocks, 'shocks.parquet')