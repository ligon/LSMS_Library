#!/usr/bin/env python

import sys
from lsms import from_dta
import dvc.api
import json

with dvc.api.open('Tanzania/2010-11/Data/HH_SEC_K1.dta',mode='rb') as dta:
    q = from_dta(dta)

q = q[['y2_hhid','itemcode','hh_k02_1','hh_k02_2']]    

q = q.rename(columns={'y2_hhid':'j','itemcode':'i','hh_k02_2':'q','hh_k02_1':'u'})

q = q.set_index(['j','u','i'])

q = q.dropna()

with open('../../_/food_items.json') as f:
    food_labels = json.load(f)

q = q.rename(index=food_labels,level='i')

q = q.groupby(['j','u','i']).sum()

q.to_parquet('food_quantities.parquet')

