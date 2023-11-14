#!/usr/bin/env python
import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
sys.path.append('../../../_/')
from local_tools import df_from_orgfile

with dvc.api.open('../Data/Y00A.DAT', mode='rb') as csv:
    df = pd.read_csv(csv)

of = df[['HID']].drop_duplicates()
of = of.rename(columns = {'HID': 'j'})

#no data on specific region and rural/urban classification
of['j'] = of['j'].astype(str)
of['t'] = '1988-89'
of['m'] = 'Ghana'
of['Rural'] = np.nan
of = of.set_index(['j','t','m'])
of.to_parquet('other_features.parquet')
