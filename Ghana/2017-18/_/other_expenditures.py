#!/usr/bin/env python
import sys
import dvc.api
import pandas as pd
import numpy as np

t = '2017-18'

myvars = dict(fn='../Data/11c_otheritems.dta',item=None,HHID='FPrimary')

with dvc.api.open(myvars['fn'],mode='rb') as dta:
    x = pd.read_stata(dta).set_index(myvars['HHID'])

x.index.name = 'j'
x.columns.name = 'i'
x['t'] = t
x['m'] = 'Ghana'

x.drop('interviewedid',axis=1)

x = x.reset_index().set_index(['j','t','m'])

x = x.replace(0.,np.nan)

x.to_parquet('other_expenditures.parquet')

