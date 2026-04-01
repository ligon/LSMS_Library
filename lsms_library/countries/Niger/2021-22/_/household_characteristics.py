#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_dataframe
import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from niger import age_sex_composition, age_handler

df_general = get_dataframe('../Data/s00_me_ner2021.dta')

df_general['j'] = (df_general['grappe'].astype(str) + df_general['menage'].astype(str)).astype(str)

df = get_dataframe('../Data/s01_me_ner2021.dta', convert_categoricals=False)

df['j'] = (df['grappe'].astype(str) + df['menage'].astype(str)).astype(str)
joined = pd.merge(df, df_general, how='left', on='j')

joined = joined.replace(9999, np.nan)

joined = age_handler(joined, interview_date='s00q23a', age='s01q04a', m='s01q03b', d='s01q03a', y='s01q03c', interview_year='2021')

joined['t'] = joined['s00q23a'].dt.year

region = joined[['s00q01', 'j', 't']].set_index('j')
region = region[~region.index.duplicated(keep='first')]
region['s00q01'] = region['s00q01'].astype(str).str.capitalize()

hh = age_sex_composition(joined, sex='s01q01', sex_converter=(lambda x: 'm' if x == 1 else 'f'),
                         age='age', age_converter=None, hhid='j')
final = pd.merge(hh, region, how='left', left_index=True, right_index=True).rename({'s00q01': 'm'}, axis=1)
final = final.set_index(['t', 'm'], append=True)

to_parquet(final, 'household_characteristics.parquet')
