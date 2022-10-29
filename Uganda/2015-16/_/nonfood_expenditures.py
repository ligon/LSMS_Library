#!/usr/bin/env python
import sys
sys.path.append('../../_')
from uganda import nonfood_expenditures
import dvc.api
from lsms import from_dta
import pandas as pd 

myvars = dict(fn='../Data/gsec15c.dta',
              item='itmcd',
              HHID='hhid',
              purchased='h15cq5',
              away=None,
              produced='h15cq7',
              given='h15cq9')

x = nonfood_expenditures(**myvars) 

#general hh dataset 
with dvc.api.open('../Data/gsec1.dta',mode='rb') as dta: 
    a = from_dta(dta)
a = a[["hh", "HHID"]].rename(columns={'hh': 'j', 'HHID': 'correct_hhid'})
#replace wrong hhid  
x = x.reset_index()
x = pd.merge(x, a, on='j')
x = x.drop('j', axis=1).rename(columns={'correct_hhid':'j'}).set_index('j')
x.columns.name = 'i'

x.to_parquet('nonfood_expenditures.parquet')
