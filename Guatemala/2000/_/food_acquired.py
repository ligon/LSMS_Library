#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import pyreadstat
import numpy as np
import json
import dvc.api
from lsms import from_dta

fs = dvc.api.DVCFileSystem('../../')
fs.get_file('/Guatemala/2000/Data/ECV13G12.DTA', '/tmp/ECV13G12.DTA')
df, meta = pyreadstat.read_dta('/tmp/ECV13G12.DTA', apply_value_formats = True, formats_as_category = True)

food_labels = df['item']

#deal with labels
food_items = pd.read_csv('../../_/food_items.org', sep='|', skipinitialspace=True, converters={1:lambda s: s.strip()})
food_items.columns = [s.strip() for s in food_items.columns]
food_items = food_items['Preferred Label']
food_items.index = food_items.str.strip().str.lower()
food_items = food_items.squeeze().str.strip().to_dict()
df['item'] = df['item'].map(food_items)

df['hogar'] = df['hogar'].astype(int).astype(str)
labels = {'hogar': 'j', 'item': 'i', 'p12a03': 'bought', 'p12a06d': 'expense', 'p12a06a' : 'amount bought','p12a06b': 'units in bought',
          'p12a06c' : 'equivalent', 'p12a07': 'obtained', 'p12a09a' : 'amount obtained', 'p12a09b': 'units in obtained', 'umr' : 'umr', 'cnlib': 'conversion factor'}
df = df.loc[:, labels.keys()]
df = df.rename(columns=labels)
df = df.set_index(['j', 'i'])
df = df[(df['bought'] == 1) | (df['obtained'] == 1)] #filter out unbought and unobtained

df['pounds bought'] = df['amount bought'].mul(df['conversion factor'])
df['pounds bought'] = df['pounds bought'].mul(df['equivalent'])
df['price/original unit'] = df['expense']/df['amount bought']
df['price/umr'] = df['expense']/(df['amount bought'] * df['equivalent'])
df['price/pounds'] = df['expense']/df['pounds bought']
df = df.loc[df.index.dropna()]

means = df.groupby('i').agg({'price/pounds' : np.mean})
stds = df.groupby('i').agg({'price/pounds' : np.std})

def unbelievable(row):
    if row['bought'] == 2:
        return True
    return abs(row['price/pounds'] - means.loc[row.name[1]]) < 2*stds.loc[row.name[1]]

df['plausible'] = df.apply(lambda x: unbelievable(x), axis=1)
