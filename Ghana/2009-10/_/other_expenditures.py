#!/usr/bin/env python
import sys
import dvc.api
import pandas as pd
import numpy as np

t = '2009-10'

myvars = dict(fn='Ghana/%s/Data/S11C.dta' % t,item='itemname',HHID='hhno')

with dvc.api.open(myvars['fn'],mode='rb') as dta:
    df = pd.read_stata(dta)

# Values recorded as cedis & pesewas; add 'em up
df['value'] = df['s11c_1'] + df['s11c_2']/100

x = df[[myvars['HHID'],myvars['item'],'value']]

x[myvars['HHID']] = x[myvars['HHID']].astype(str)

x = x.set_index([myvars['HHID'],myvars['item']]).squeeze().unstack(myvars['item'])

x.index.name = 'j'
x.columns.name = 'i'
x['t'] = t
x['m'] = 'Ghana'

x = x.reset_index().set_index(['j','t','m'])

x = x.replace(0.,np.nan)

x.to_parquet('other_expenditures.parquet')

