#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import dvc.api
from lsms import from_dta
import sys
sys.path.append('../../_/')
from tanzania import age_sex_composition, get_household_roster

round_match = {1:'2008-09', 2:'2010-11', 3:'2012-13', 4:'2014-15'}

with dvc.api.open('../Data/upd4_hh_b.dta',mode='rb') as dta:
    roster = from_dta(dta)

# Problem is that we don't have the household id we need in the roster!
b = dict(HHID = 'r_hhid',
         ID='UPI',
         sex = 'hb_02',
         age = 'hb_04',
         wave = 'round')

roster = roster[b.values()]

# Grab other file that has id we need
with dvc.api.open('../Data/upd4_hh_a.dta',mode='rb') as dta:
    cover =  from_dta(dta)

cover = cover[['r_hhid','UPHI','round']]

df = pd.merge(roster,cover,on=['r_hhid','round'])

b['HHID'] = 'r_hhid'
del b['ID']

Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))

hc = get_household_roster(df,fn_type=None,Age_ints=Age_ints,**b)

hc.index = hc.index.rename({'HHID':'j','region':'m','wave':'t'})

hc = hc.rename(index=round_match,level='t')

hc = hc.reset_index().set_index(['j','t'])

hc = hc.filter(regex='ales ')

hc['log HSize'] = np.log(hc.sum(axis=1))

# Drop any obs with infinities...
hc = hc.loc[np.isfinite(hc.min(axis=1)),:]

assert hc.index.is_unique, "Non-unique index!  Fix me!"

hc.to_parquet('household_characteristics.parquet')
