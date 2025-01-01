#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/g7sec0.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

of = df[['hid','region','urbrur']]

of = of.rename(columns = {'hid': 'j',
                          'region': 'm',
                          'urbrur': 'Rural'})

of['j'] = of['j'].astype(str)
of['t'] = '2016-17'
of = of.set_index(['j','t','m'])

of.to_parquet('other_features.parquet')
