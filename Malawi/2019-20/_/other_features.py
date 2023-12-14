#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/hh_mod_a_filt.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=False)

df = df.loc[:,[ "case_id", "region"]]
df['region'] = df['region'].map({1: 'North', 2: 'Central', 3: 'Southern'})
df =  df.rename({'case_id': 'j', 'region' : 'm'}, axis = 1)
df['t'] = '2019-20'
df = df.set_index(['j','t'])
df.columns.name = 'k'

df.to_parquet('other_features.parquet')
