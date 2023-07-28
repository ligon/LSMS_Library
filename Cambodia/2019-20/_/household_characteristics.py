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

regions = pd.read_parquet('other_features.parquet')
df = df.join(regions, on=['j'])

df = df.set_index(['t', 'm'], append = True)
df.columns.name = 'k'

df.to_parquet('household_characteristics.parquet')
