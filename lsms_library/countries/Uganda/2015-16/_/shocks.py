#!/usr/bin/env python
"""Uganda 2015-16 ``shocks``, from ``GSEC16``.

The complication is the household key.  ``GSEC16``'s one id column is NAMED
``HHID`` but holds ``gsec1.hh`` values (the namespace #1290 found in ``gsec9``), so
a row reaches the sample's ``HHID``, and the interview month its ``Onset`` needs,
only through a cover's ``hh -> HHID`` pair.

GH #1434: 2 rows belong to households only ``AGSEC1`` records (GH #1120 D1), and an
inner merge on ``gsec1.hh`` deleted them.  ``AGSEC1`` is now the fallback, on the
#1290 housing rules: the household cover stays primary, two recorded targets for
one ``hh`` refuse, and a key neither cover records keeps that key with a warning.
On all 2,622 households both covers hold, ``AGSEC1``'s ``HHID`` and interview date
equal ``gsec1``'s wherever both are recorded.
"""
from lsms_library.local_tools import to_parquet, get_dataframe

from calendar import month
import sys
import warnings
sys.path.append('../../_/')
import pandas as pd
from datetime import datetime

#shock dataset
df = get_dataframe('../Data/GSEC16.dta')
df = df[df['h16q2y'].notna()] #filter for valid entry 

#general hh dataset: the household cover, then AGSEC1 for the hh it does not hold
cover_columns = ['hh', 'HHID', 'year', 'month']
date = get_dataframe('../Data/gsec1.dta')[cover_columns].dropna(subset=['hh', 'HHID'])
ag = get_dataframe('../Data/AGSEC1.dta')[cover_columns].dropna(subset=['hh', 'HHID'])
both = date[['hh', 'HHID']].merge(ag[['hh', 'HHID']], on='hh', suffixes=('', '_ag'))
if both['HHID'].ne(both['HHID_ag']).any():
    raise ValueError('Uganda/2015-16 shocks: conflicting household/agriculture hh/HHID bridge '
                     f"({int(both['HHID'].ne(both['HHID_ag']).sum())} pairs)")
date = pd.concat([date, ag[~ag['hh'].isin(date['hh'])]], ignore_index=True).drop_duplicates()
if date['hh'].duplicated().any():
    raise ValueError('Uganda/2015-16 shocks: ambiguous hh/HHID bridge '
                     f"({int(date['hh'].duplicated().sum())} repeated hh)")

#filter for hhs who have taken the shock questionnaire 
date = date[date.set_index('hh').index.isin(df.set_index('HHID').index)] 


#calculate shock onset 
df['h16q02a'] = pd.to_datetime(df.h16q02a, format='%B').dt.month

df['start_date'] = pd.to_datetime(df.rename(columns={'h16q2y': 'year', 'h16q02a': 'month'})[['year', 'month']].assign(DAY=1)) #no day reported; assume 1st of the month 
date['end_date'] = pd.to_datetime(date[['year', 'month']].assign(DAY=1)) #round the interview date to 1st of the month to match shock date
date = date[["hh", "end_date","HHID"]].rename(columns={'hh': 'HHID', 'HHID': 'master_hhid'})
df = pd.merge(df, date, on='HHID', how='left')
unresolved = df['master_hhid'].isna()
if unresolved.any():
    warnings.warn('Uganda/2015-16 shocks: unresolved hh/HHID bridge for '
                  f'{int(unresolved.sum())} of {len(df)} shock rows; retaining recorded keys',
                  UserWarning, stacklevel=2)
    df['master_hhid'] = df['master_hhid'].where(~unresolved, df['HHID'])
df['Onset'] = (df.end_date.dt.to_period('M') - df.start_date.dt.to_period('M')).apply(
    lambda x: x.n if pd.notna(x) else pd.NA)  # no recorded interview month: no Onset

shocks = pd.DataFrame({"i": df.master_hhid.values.tolist(),
                    "Shock":df.h16q00.values.tolist(),
                    "Year": df.h16q2y.values.tolist(),
                    "Onset":df.Onset.values.tolist(),
                    "Duration":df.h16q02b.values.tolist(),
                    "AffectedIncome":df.h16q3a.values.tolist(),
                    "AffectedAssets":df.h16q3b.values.tolist(),
                    "AffectedProduction":df.h16q3c.values.tolist(),
                    "AffectedConsumption":df.h16q3d.values.tolist(),
                    "HowCoped0":df.h16q4a.values.tolist(),
                    "HowCoped1":df.h16q4b.values.tolist(),
                    "HowCoped2":df.h16q4c.values.tolist()})

#converting data types 
for col in ["AffectedIncome",  "AffectedAssets",  "AffectedProduction", "AffectedConsumption"]:
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

shocks.insert(1, 't', '2015-16')
shocks = shocks.set_index(['i','t','Shock'])

to_parquet(shocks, 'shocks.parquet')
