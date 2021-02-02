#!/usr/bin/env python
import sys
sys.path.append('../../_')
from ghana_panel import food_expenditures
import numpy as np
import dvc.api
import pandas as pd

t = '2009-10'

myvars = dict(fn='Ghana/%s/Data/S11A.dta' % t,item='itname',HHID='hhno')

with dvc.api.open(myvars['fn'],mode='rb') as dta:
    df = pd.read_stata(dta)

# Values recorded as cedis & pesewas; add 'em up
df['purchased'] = df['s11a_cii'] + df['s11a_ciii']/100
df['produced'] = df['s11a_bii'] + df['s11a_biii']/100
df['given'] = df['s11a_dii'] + df['s11a_diii']/100

x = df[[myvars['HHID'],myvars['item'],'purchased','produced','given']]

x = x.set_index([myvars['HHID'],myvars['item']]).sum(axis=1).unstack(myvars['item'])

x.index.name = 'j'
x.columns.name = 'i'
x['t'] = t
x['m'] = 'Ghana'

x = x.reset_index().set_index(['j','t','m'])

x = x.replace(0.,np.nan)

x.to_parquet('food_expenditures.parquet')

