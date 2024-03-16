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
fs.get_file('/Panama/2003/Data/E03GA10B.DTA', '/tmp/E03GA10B.DTA')
df, meta = pyreadstat.read_dta('/tmp/E03GA10B.DTA', apply_value_formats=True)

columns_dict = {"form": "j", "gai00": "i", "gai06a": "quantity (bought, in original units)", "gai06b1": "conversionb",  "gai06b2": "unitcode (bought)",
                "gai06c": "total spent", "gai10a": "quantity (obtained, in original units)", "gai10b1": "conversiono", "gai10b2": "unitcode (obtained)"}

food_items = pd.read_csv('../../_/food_items.org', sep='|', skipinitialspace=True, converters={1 : lambda s: s.strip()})
food_items.columns = [s.strip() for s in food_items.columns]
food_items = food_items.loc[:, ['Preferred Label', '2003']].iloc[1:]
food_items['2003'] = food_items['2003'].str.strip()
food_items = food_items.set_index('2003')
food_items = food_items.squeeze().str.strip().to_dict()

df = df.loc[:, ["form", "gai00", "gai06a", "gai06b1", "gai06b2", "gai06c", "gai10a", "gai10b1", "gai10b2"]]
df = df.rename(columns_dict, axis=1)

df['i'] = df['i'].map(food_items)
df['j'] = df['j'].astype(int).astype(str)
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

to_parquet(df, "food_acquired.parquet")
