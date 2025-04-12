from lsms_library.local_tools import to_parquet
#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster

with dvc.api.open('../Data/individual.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=False)

cols = ['opstina', 'popkrug', 'dom']
df['j'] = df[cols].apply(lambda row: ''.join(row.values.astype(str)), axis=1)

Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
df = get_household_roster(df, sex='pol', sex_converter=lambda x:['m','f'][x==2],
                                  age='starost', age_converter=None, HHID='j',
                                  convert_categoricals=True,Age_ints=Age_ints,fn_type=None)
df['log HSize'] = np.log(df[['girls', 'boys', 'men', 'women']].sum(axis=1))

df.index.name = 'j'
df.columns.name = 'k'

to_parquet(df, 'household_characteristics.parquet')
