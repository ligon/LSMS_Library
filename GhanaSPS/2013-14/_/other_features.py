from lsms_library.local_tools import to_parquet
#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/01b2_preroster.dta', mode='rb') as dta:
    df = from_dta(dta)

of = df[['FPrimary','movedtolocation_region']].drop_duplicates()

of = of.rename(columns = {'FPrimary': 'j',
                          'movedtolocation_region': 'm',
                          })

of['t'] = '2013-14'
of['m'] = of['m'].replace('', np.nan)
of = of.dropna(how = 'any')
of['Rural'] = np.nan 
of = of.drop_duplicates()

of = of.set_index(['j','t','m'])

#of['Rural'] = (of.Rural=='Rural') + 0.

to_parquet(of, 'other_features.parquet')
