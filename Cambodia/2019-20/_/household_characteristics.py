from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster
from cambodia import age_sex_composition

with dvc.api.open('../Data/hh_sec_2.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=False)

df = age_sex_composition(df)
df['t'] = "2019-20"

regions = get_dataframe('other_features.parquet').reset_index().set_index('j')
df = df.join(regions, on=['j'])

df = df.set_index(['t', 'm'], append = True)
df.columns.name = 'k'

to_parquet(df, 'household_characteristics.parquet')
