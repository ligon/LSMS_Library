#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/key_hhld_info.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

of = df[['hhno','id1','urbrur']]

of = of.rename(columns = {'hhno': 'j',
                          'id1': 'm',
                          'urbrur': 'Rural'})

of['j'] = of['j'].astype(str)
of['t'] = '2009-10'
of = of.set_index(['j','t','m'])

of['Rural'] = (of.Rural=='Rural') + 0.

to_parquet(of, 'other_features.parquet')
