#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/00_hh_info.dta', mode='rb') as dta:
    df = from_dta(dta)

of = df[['FPrimary','region']].drop_duplicates()

of = of.rename(columns = {'FPrimary': 'j',
                          'region': 'm',
                          })

of['t'] = '2017-18'
of['Rural'] = np.nan 
of = of.drop_duplicates()
of = of.set_index(['j','t','m'])

#of['Rural'] = (of.Rural=='Rural') + 0.

of.to_parquet('other_features.parquet')
