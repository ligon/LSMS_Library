#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_dataframe
import sys
import pandas as pd
import numpy as np

t = '2017-18'

myvars = dict(fn='../Data/11c_otheritems.dta',item=None,HHID='FPrimary')

x = get_dataframe(myvars['fn']).set_index(myvars['HHID'])

x.index.name = 'j'
x.columns.name = 'i'
x['t'] = t
x['m'] = 'Ghana'

x.drop('interviewedid',axis=1)

x = x.reset_index().set_index(['j','t','m'])

x = x.replace(0.,np.nan)

to_parquet(x, 'other_expenditures.parquet')

