#!/usr/bin/env python

import sys
from lsms import from_dta
import dvc.api
import json

with dvc.api.open('../Data/HH_SEC_J1.dta',mode='rb') as dta:
    q = from_dta(dta)

q = q.rename(columns={'y3_hhid':'j','itemcode':'i','hh_j02_2':'q','hh_j02_1':'u'})

q = q[['j','i','u','q']]    

q = q.set_index(['j','u','i'])

q = q.dropna()

with open('../../_/food_items.json') as f:
    food_labels = json.load(f)

q = q.rename(index=food_labels,level='i')

q = q.groupby(['j','u','i']).sum()

q.to_parquet('food_quantities.parquet')

