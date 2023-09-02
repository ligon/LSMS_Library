#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/s00_me_sen2018.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

df['hhid'] = df['vague'].astype(str) + df['grappe'].astype(str) + df['menage'].astype(str)
waves = {1: 2018, 2: 2019}
df['t'] = df['vague'].map(waves)

regions  = df.groupby('hhid').agg({'s00q01' : 'first', 't': 'first'})

regions = regions.rename(columns = {'s00q01' : 'm'})
regions.index.name = 'j'
regions = regions.set_index('t', append = True)

regions.to_parquet('other_features.parquet')
