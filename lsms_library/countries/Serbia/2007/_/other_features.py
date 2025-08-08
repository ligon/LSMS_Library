#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

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

df = df.set_index('j').loc[:, ['region2', 'urban']]
df.region2 = df.region2.str.capitalize()
df = df.rename({'region2': 'm', 
                'urban': 'Rural'}, axis=1)
df.Rural = (df.Rural=='rural') + 0.

to_parquet(df, 'other_features.parquet')
