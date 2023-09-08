#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/household.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

cols = ['opstina', 'popkrug', 'dom']
df['j'] = df[cols].apply(lambda row: ''.join(row.values.astype(str)), axis=1)

df = df.set_index('j').loc[:, 'region2'].str.capitalize().to_frame()
df = df.rename({'region2': 'm'}, axis=1)

df.to_parquet('other_features.parquet')
