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
from lsms_library.local_tools import to_parquet, df_from_orgfile, format_id

t = '1997'

fs = dvc.api.DVCFileSystem('../../')
fs.get_file('/Panama/1997/Data/GAST-A.DTA', '/tmp/GAST-A.DTA')
df, meta = pyreadstat.read_dta('/tmp/GAST-A.DTA')
with open('../../_/units.json','r') as f:
   unit_conversions = json.load(f)

units = pd.DataFrame(unit_conversions["units"])

df = df.loc[:, ['form','ga100', 'ga106a', 'ga106b', 'ga106c', 'ga110a', 'ga110b']]
df = df.rename({'form': 'j', 'ga100':'i', 'ga106a':'quantity bought', 'ga106b':'unitcode (bought)', 'ga106c':'total spent', 'ga110a':'quantity obtained', 'ga110b':'unitcode (obtained)'}, axis=1)
df = df.mask(df > 1e99).fillna(0)

food_items = df_from_orgfile('../../_/food_items.org')
food_items = food_items.loc[:, ['Preferred Label', t]]

if food_items[t].dtypes==object:
   food_items[t] = food_items[t].str.strip()

food_items = food_items.replace(['','---'],np.nan).dropna()
food_items = food_items.set_index(t).dropna()
food_items = food_items.squeeze().str.strip().to_dict()

unit_dict = units.loc[:, ["Unitcode", "Translation"]].set_index("Unitcode").to_dict()["Translation"]

df['i'] = df['i'].replace(food_items)
df['j'] = df['j'].apply(format_id)
df = df.set_index(['j', 'i'])

df['unitcode (bought)'], df['unitcode (obtained)'] = df['unitcode (bought)'].astype(int).map(unit_dict), df['unitcode (obtained)'].astype(int).map(unit_dict)

pound_dict = units.loc[:, ["Translation", "Conversion to Pounds"]].set_index("Translation").to_dict()["Conversion to Pounds"]
poundmappingb = df["unitcode (bought)"].map(pound_dict).fillna(1) * df['quantity bought']
poundmappingo = df["unitcode (obtained)"].map(pound_dict).fillna(1) * df['quantity obtained']

df['unitcode (bought)'] = np.where(poundmappingb != df["quantity bought"], 'pound',  df["unitcode (bought)"])
df['unitcode (obtained)'] = np.where(poundmappingo != df["quantity obtained"], 'pound', df["unitcode (obtained)"])

df['quantity bought'] = poundmappingb
df['quantity obtained'] = poundmappingo

# replace numbers near 9999 which indicate missing 
tolerance = 1
numeric_cols = df.select_dtypes(include=[np.number])
df[numeric_cols.columns] = numeric_cols.where((numeric_cols < (9999 - tolerance)) | (numeric_cols > (9999 + tolerance)), np.nan)

df['unitcode (bought)'] = df['unitcode (bought)'].astype(str)
df['unitcode (obtained)'] = df['unitcode (obtained)'].astype(str)

df['price per unit'] = df['total spent']/df['quantity bought']
df['price per unit'] = df['price per unit'].where(np.isfinite(df['price per unit']))

to_parquet(df, "food_acquired.parquet")
