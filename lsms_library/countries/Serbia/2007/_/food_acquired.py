#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_dataframe, df_from_orgfile

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json

df = get_dataframe('../Data/m5_1_diary.dta', convert_categoricals=False)

cols = ['opstina', 'popkrug', 'dom']
df['j'] = df[cols].apply(lambda row: ''.join(row.values.astype(str)), axis=1)

quant = [col for col in df if col.startswith('kol')]
df['Quantity'] = df[quant].sum(axis=1)
spent = [col for col in df if col.startswith('din')]
df['Total Expenditure'] = df[spent].sum(axis=1)

dailyprice = pd.concat([df[s]/df[q] for q,s in zip(quant, spent)], axis=1)
median = dailyprice.median(axis=1)
df['Price'] = median

# Use the numeric product code 'proizvod' as the harmonization key.
# (The text 'nsifra' field carries the legacy YUSCII transliteration in
# which '|' stands for "đ", which cannot survive an org-mode table; see
# ../../_/food_items.org for the rationale.)
dict = {'proizvod': 'i', 'mera': 'units'}
df = df.rename(dict, axis = 1).reset_index()

# Harmonize raw item codes to Preferred Labels via food_items.org,
# keyed on the numeric 'proizvod' code.
food_items = df_from_orgfile('../../_/food_items.org', name='food_label', to_numeric=False)
food_items = food_items.loc[:, ['Preferred Label', 'proizvod']]
food_items['proizvod'] = food_items['proizvod'].str.strip()
food_items = food_items.replace(['', '---'], pd.NA).dropna()
food_items = food_items.set_index('proizvod')['Preferred Label'].str.strip().to_dict()
df['i'] = df['i'].astype(str).str.strip().replace(food_items)

final = df.loc[:, ['j', 'i', 'Quantity', 'units', 'Total Expenditure', 'Price']]

final = final.set_index(['j','i'])
to_parquet(final, 'food_acquired.parquet')
