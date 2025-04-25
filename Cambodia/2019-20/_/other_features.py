#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

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
df = df.rename(columns = {'s01q02': 'm', 'HHID': 'j', 's01q06b' : 'Rural'})
df = df.loc[:,['j', 'm', 'Rural']]
df['Rural'] = df['Rural'].map({'Urban':0, 'Rural':1})
df['m'] = df['m'].str.strip()
df = df.set_index(['j', 'm'])

to_parquet(df, 'other_features.parquet')
