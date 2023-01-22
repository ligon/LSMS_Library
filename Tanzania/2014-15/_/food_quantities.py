#!/usr/bin/env python

import sys
from lsms import from_dta
import dvc.api
import json

with dvc.api.open('../Data/hh_sec_j1.DTA',mode='rb') as dta:
    q = from_dta(dta)

q = q.rename(columns={'y4_hhid':'j','itemcode':'i','hh_j02_2':'q','hh_j02_1':'u'})

q = q[['j','i','u','q']]    

q = q.set_index(['j','u','i'])

q = q.dropna()

with open('../../_/food_items.json') as f:
    food_labels = json.load(f)

q = q.rename(index=food_labels,level='i')

q = q.groupby(['j','u','i']).sum()

q.to_parquet('food_quantities.parquet')

