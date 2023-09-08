#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/emc2014_p1_individu_27022015.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

df["hhid"]  = df["zd"].astype(str) + '-'  + df["menage"].astype(int).astype(str)
regions  = df.groupby('hhid').agg({'region' : 'first'})

regions = regions.rename(columns = {'region' : 'm'})
regions.index.name = 'j'
regions['t'] = '2013_Q4'
regions = regions.set_index('t', append = True)

regions.to_parquet('other_features.parquet')
