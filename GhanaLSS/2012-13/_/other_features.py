#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/PARTA/SEC0.dta', mode='rb') as dta: #complete list of HID
    df = from_dta(dta, convert_categoricals=True)

with dvc.api.open('../Data/stata/partb/partb.zip', mode='rb') as dta: #complete list of HID
    df = dta

with dvc.api.open('../Data/GOVERNANCE/gps-sec0.dta', mode='rb') as dta: #urban indicator for an incomplete list of HID
    df2 = from_dta(dta, convert_categoricals=True)

of = df[['HID', 'region']].merge(df2[['HID', 'loc2']].drop_duplicates(), on = ['HID'], how = 'left')

of = of.rename(columns = {'HID': 'j',
                          'region': 'm',
                          'loc2': 'Rural'})

of['j'] = of['j'].astype(str)
of['t'] = '2012-13'
of = of.set_index(['j','t','m'])

of['Rural'] = of['Rural'].replace({'Rural':1, 'Urban':0}) #preserve NaNs

of.to_parquet('other_features.parquet')
