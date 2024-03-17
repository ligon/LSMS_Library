import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
import pyreadstat
sys.path.append('../../../_/')
from local_tools import to_parquet, df_from_orgfile, format_id

t = '2003'

fs = dvc.api.DVCFileSystem('../../')
fs.get_file('/Panama/2003/Data/E03GA10B.DTA', '/tmp/E03GA10B.DTA')
df, meta = pyreadstat.read_dta('/tmp/E03GA10B.DTA', apply_value_formats=True)

columns_dict = {"form": "j", "gai00": "i", "gai06a": "quantity (bought, in original units)", "gai06b1": "conversionb",  "gai06b2": "unitcode (bought)",
                "gai06c": "total spent", "gai10a": "quantity (obtained, in original units)", "gai10b1": "conversiono", "gai10b2": "unitcode (obtained)"}

food_items = df_from_orgfile('../../_/food_items.org')
food_items = food_items.loc[:, ['Preferred Label', t]]
food_items[t] = food_items[t].str.strip()
food_items = food_items.replace(['','---'],np.nan).dropna()
food_items = food_items.set_index(t).dropna()
food_items = food_items.squeeze().str.strip().to_dict()

df = df.loc[:, columns_dict.keys()]
df = df.rename(columns_dict, axis=1)

df['i'] = df['i'].replace(food_items)
df['j'] = df['j'].apply(format_id)
df = df.set_index(['j', 'i'])
df['quantity bought'] = df['quantity (bought, in original units)'].astype(float)*df['conversionb'].astype(float)
df['quantity obtained'] = df['quantity (obtained, in original units)'].astype(float)*df['conversiono'].astype(float)

df = df.loc[:, ['quantity bought', 'unitcode (bought)', 'total spent', 'quantity obtained', 'unitcode (obtained)']]
df['total spent'] = df['total spent'].astype(float).mask(df['total spent'].astype(float) >= 99999)
df['quantity bought'] = df['quantity bought'].mask(df['quantity bought'] >= 99999)
df['quantity obtained'] = df['quantity obtained'].mask(df['quantity obtained'] >= 99999)
df.loc[df['unitcode (bought)'] == 'FRAMO', 'unitcode (bought)'] = 'GRAMO'

unit_dict = {'GALON': 'gallon', 'GRAMO': 'grams', 'KILOGRAMO': 'kilograms', 'LIBRA': 'pounds', 'LITRO': 'liters', 'ONZA': 'ounces', 'MILILITRO': 'milliliters'}

df['unitcode (bought)'] = df['unitcode (bought)'].map(unit_dict).astype(str)
df['unitcode (obtained)'] = df['unitcode (obtained)'].map(unit_dict).astype(str)

df['price per unit'] = df['total spent']/df['quantity bought']
df['price per unit'] = df['price per unit'].where(np.isfinite(df['price per unit']))

to_parquet(df, "food_acquired.parquet")
