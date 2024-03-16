#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
import pyreadstat
sys.path.append('../../../_/')
from local_tools import to_parquet

fs = dvc.api.DVCFileSystem('../../')
fs.get_file('/Panama/2008/Data/05alimentos.dta', '/tmp/05alimentos.dta')
df, meta = pyreadstat.read_dta('/tmp/05alimentos.dta', apply_value_formats = True, formats_as_category = True)

df = df.loc[:, ['hogar','producto', 's11a6a', 's11a6b', 's11a6c', 's11a10a', 's11a10b']]
df = df.rename({'hogar': 'j', 'producto':'i', 's11a6a':'quantity bought', 's11a6b':'unitcode (bought)',
                's11a6c':'total spent', 's11a10a':'quantity obtained', 's11a10b':'unitcode (obtained)'}, axis=1)


cat_columns = df.iloc[:, 2:].select_dtypes(['category']).columns
df[cat_columns] = df[cat_columns].apply(lambda x: x.cat.codes)
df.replace({-1: np.NaN}, inplace=True)
val = df._get_numeric_data()
val[val >= 99999] = np.NaN
val[val == 0] = np.NaN

food_items = pd.read_csv('../../_/food_items.org', sep='|', skipinitialspace=True, converters={1:lambda s: s.strip()})
food_items.columns = [s.strip() for s in food_items.columns]
food_items = food_items.loc[:, ['Preferred Label', '2008']].iloc[1:]
food_items['2008'] = food_items['2008'].str.strip()
food_items = food_items.set_index('2008')
food_items = food_items.squeeze().str.strip().to_dict()

unit_dict = {'galón': 'gallon', 'gramos': 'grams', 'libra': 'pounds', 'litro': 'liters', 'onzas': 'ounces', 'unidad':'units', 'centímetro cúbico o mililitro': 'milliliters'}

df['i'] = df['i'].map(food_items)
df['j'] = df['j'].astype(int).astype(str)
df = df.set_index(['j', 'i'])
df['unitcode (bought)'] = df['unitcode (bought)'].map(unit_dict).astype(str)
df['unitcode (obtained)'] = df['unitcode (obtained)'].map(unit_dict).astype(str)
df['quantity bought'] = pd.to_numeric(df['quantity bought'], errors='coerce')
df['quantity obtained'] = pd.to_numeric(df['quantity obtained'], errors='coerce')

df['price per unit'] = df['total spent']/df['quantity bought']


to_parquet(df, "food_acquired.parquet")
