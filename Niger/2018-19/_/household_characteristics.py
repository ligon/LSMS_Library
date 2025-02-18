#!/usr/bin/env python
import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster
from niger import age_sex_composition, age_handler

#find the date that the interview was taken
with dvc.api.open('../Data/s00_me_ner2018.dta', mode='rb') as dta:
    df_general = from_dta(dta, convert_categoricals=True)

df_general['j'] = (df_general['grappe'].astype(str) + df_general['menage'].astype(str)).astype(str)

with dvc.api.open('../Data/s01_me_ner2018.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=False)

df['j'] = (df['grappe'].astype(str) + df['menage'].astype(str)).astype(str)
joined = pd.merge(df, df_general, how = 'left', on ='j')

joined.replace(9999, np.nan, inplace=True)

joined = age_handler(joined, interview_date = 's00q23a', age = 's01q04a',  m = 's01q03b', d= 's01q03a', y= 's01q03c', interview_year= '2018')

joined['t'] = joined['s00q23a'].dt.year

region = joined[['s00q01', 'j', 't']].set_index('j')
region = region[~region.index.duplicated(keep='first')]
region['s00q01'] = region['s00q01'].astype(str).str.capitalize()

hh = age_sex_composition(joined, sex='s01q01', sex_converter=(lambda x: 'm' if x == 1 else 'f'),
                           age='age', age_converter=None, hhid='j')
final = pd.merge(hh, region, how= 'left', left_index=True, right_index=True).rename({'s00q01': 'm'}, axis = 1)
final = final.set_index(['t', 'm'], append = True)

final.to_parquet('household_characteristics.parquet')
