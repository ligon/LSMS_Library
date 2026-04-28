#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_dataframe
import sys
import pandas as pd
import numpy as np

t = '2009-10'

myvars = dict(fn='../Data/S11C.dta',item='itemname',HHID='hhno')

df = get_dataframe(myvars['fn'])

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

to_parquet(x, 'other_expenditures.parquet')

