#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta


with dvc.api.open('../Data/SEC0A.DTA', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

of = df[['region','loc2']]
of['hhid'] = str(int(df['clust'])) + str(int(df['nh']))

of = of.rename(columns = {'hhid': 'j',
                          'region': 'm',
                          'loc2': 'Rural'})

of['j'] = of['j'].astype(str)
of['t'] = '1998-99'
of = of.set_index(['j','t','m'])

of.to_parquet('other_features.parquet')
