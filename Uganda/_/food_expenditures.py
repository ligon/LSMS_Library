#!/usr/bin/env python
"""
Read food expenditures; use harmonized food labels.
"""
import pandas as pd
from uganda import change_id

def id_walk(df,wave,waves):
    
    use_waves = list(waves.keys())
    T = use_waves.index(wave)
    for t in use_waves[T::-1]:
        if len(waves[t]):
            df = change_id(df,'Uganda/%s/Data/%s' % (t,waves[t][0]),*waves[t][1:])
        else:
            df = change_id(df)

    return df
    
# Data to link household ids across waves
Waves = {'2005-06':(),
         '2009-10':(),
         '2010-11':(),
         '2011-12':(),
         '2013-14':('GSEC1.dta','HHID','HHID_old'),
         '2015-16':('gsec1.dta','hh','HHID'),
         '2018-19':('GSEC1.dta','hhid','t0_hhid'),
         '2019-20':('HH/gsec1.dta','hhid','hhidold')}

x={}

for t in Waves.keys():
    print(t)
    x[t] = pd.read_parquet('../'+t+'/_/food_expenditures.parquet')
    x[t] = id_walk(x[t],t,Waves)
    x[t] = x[t].stack('i').dropna()
    x[t] = x[t].reset_index().set_index(['j','i']).squeeze()

df = pd.DataFrame(x)
df.columns.name = 't'

x = df.stack().unstack('i')

x['m'] = 'Uganda'
x = x.reset_index().set_index(['j','t','m'])

x = x.fillna(0)

x.to_parquet('../var/food_expenditures.parquet')
