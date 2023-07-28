#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/hh_sec_1.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

df = df.reset_index()
df = df.rename(columns = {'s01q02': 'm', 'HHID': 'j'})
df = df.loc[:,['j', 'm']]

df = df.set_index('j')
df['m'] = df['m'].str.strip()

df.to_parquet('other_features.parquet')
