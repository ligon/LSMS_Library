#!/usr/bin/env python

from lsms import from_dta
import dvc.api
import json
import pandas as pd

with dvc.api.open('../Data/HH_SEC_K1.dta',mode='rb') as dta:
    p = from_dta(dta)

p = p.rename(columns={'y2_hhid':'j','itemcode':'i','hh_k03_2':'q','hh_k03_1':'u','hh_k04':'x'})

p = p.set_index(['j','u','i'])

p = p[['q','x']]

p = p.dropna()

with open('../../_/food_items.json') as f:
    food_labels = json.load(f)

p = p.rename(index=food_labels,level='i')

p = p.groupby(['j','u','i']).sum()

unitvalues = pd.DataFrame({'p':p['x']/p['q']})

unitvalues.to_parquet('food_unitvalues.parquet')



