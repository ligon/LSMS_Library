#!/usr/bin/env python

import sys
from lsms import from_dta
import dvc.api
import json

with dvc.api.open('../Data/SEC_K1.dta',mode='rb') as dta:
    q = from_dta(dta)[['hhid','skcode','skq2_amount','skq2_meas']]

q = q.rename(columns={'hhid':'j','skcode':'i','skq2_amount':'q','skq2_meas':'u'})

q = q.set_index(['j','u','i'])

q = q.dropna()

with open('../../_/food_items.json') as f:
    food_labels = json.load(f)

q = q.rename(index=food_labels,level='i')

q = q.groupby(['j','u','i']).sum()

q.to_parquet('food_quantities.parquet')

