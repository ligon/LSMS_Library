#!/usr/bin/env python

import sys
sys.path.append('../../../_/')
import pandas as pd
import numpy as np
import json
from lsms_library.local_tools import to_parquet, get_dataframe, df_from_orgfile

df = get_dataframe('../Data/hh_sec_5.dta', convert_categoricals=False)

df = df.rename({'HHID': 'j', 'food_consumption_roster_1__id' : 'i', 's05q03' : 'units','s05q04' : 'quantity', 's05q05' : 'total spent', 's05q06': 'value obtained'}, axis=1)

# Harmonize raw item codes to Preferred Labels via food_items.org,
# keyed on the numeric item code 1-64 (food_consumption_roster_1__id).
# We key on the code rather than the categorical text because some raw
# Stata value labels carry mojibake (e.g. a zero-width space in item 41)
# that does not round-trip reliably as a join key.
df['i'] = pd.to_numeric(df['i'], errors='coerce').astype('Int64').astype(str)
food_items = df_from_orgfile('../../_/food_items.org', name='food_label', to_numeric=False)
food_items = food_items.loc[:, ['Preferred Label', 'Code']]
food_items['Code'] = food_items['Code'].str.strip()
food_items = food_items.replace(['', '---'], pd.NA).dropna()
food_items = food_items.set_index('Code')['Preferred Label'].str.strip().to_dict()
df['i'] = df['i'].replace(food_items)

cols = df.loc[:, ['quantity', 'total spent', 'value obtained']].columns
df[cols] = df[cols].apply(pd.to_numeric, errors='coerce')
df = df.fillna(0)

df['price per unit'] = (df['total spent']+ df['value obtained'])/df['quantity']
df = df.drop({'s05_start_time', 's05_end_time', 's05_respondent'}, axis=1)
df = df.set_index(['j', 'i'])

to_parquet(df, "food_acquired.parquet")
