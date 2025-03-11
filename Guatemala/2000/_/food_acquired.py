#!/usr/bin/env python

import sys
import pandas as pd
import pyreadstat
import numpy as np
import dvc.api
sys.path.append('../../../_/')
from lsms_library.local_tools import df_from_orgfile

fs = dvc.api.DVCFileSystem('../../')
fs.get_file('/Guatemala/2000/Data/ECV13G12.DTA', '/tmp/ECV13G12.DTA')
df, meta = pyreadstat.read_dta('/tmp/ECV13G12.DTA', apply_value_formats = True, formats_as_category = True)

#deal with labels
food_items = df_from_orgfile('../../_/food_items.org')
food_labels = {}
food_labels = food_items[['Preferred Label', '2000']].set_index('2000').to_dict('dict')
df['item'] = df['item'].replace(food_labels['Preferred Label'])


df['hogar'] = df['hogar'].astype(int).astype(str)
labels = {'hogar': 'j', 'item': 'i', 'p12a03': 'bought', 'p12a06d': 'expense', 'p12a06a' : 'amount bought','p12a06b': 'units in bought',
          'p12a06c' : 'equivalent', 'p12a07': 'obtained', 'p12a09a' : 'amount obtained', 'p12a09b': 'units in obtained', 'umr' : 'umr', 'cnlib': 'conversion factor'}
df = df.loc[:, labels.keys()]
df = df.rename(columns=labels)
df = df.set_index(['j', 'i'])
df = df[(df['bought'] == 1) | (df['obtained'] == 1)] #filter out unbought and unobtained

df['pounds bought'] = df['amount bought'].mul(df['conversion factor'])
df['pounds bought'] = df['pounds bought'].mul(df['equivalent'])

df['pounds obtained'] = df['amount obtained'].mul(df['conversion factor'])
df['pounds obtained'] = df['pounds obtained'].mul(df['equivalent'])

df['price/original unit'] = df['expense']/df['amount bought']
df['price/umr'] = df['expense']/(df['amount bought'] * df['equivalent'])
df['price/pound'] = df['expense']/df['pounds bought']
df = df.loc[df.index.dropna()]

means = df.groupby('i').agg({'price/pound' : 'mean'})
stds = df.groupby('i').agg({'price/pound' : 'std'})

def unbelievable(row):
    if row['bought'] == 2:
        return True
    return abs(row['price/pound'] - means.loc[row.name[1]]) < 2*stds.loc[row.name[1]]

df['plausible'] = df.apply(lambda x: unbelievable(x), axis=1)

cols = {'expense':'Purchased Value',
        'pounds bought':'Purchased Amount',
        'pounds obtained':'Obtained Amount',
        'price/pound':'Unit Value'}

final = df.rename(columns=cols)[list(cols.values())]

final['Total Quantity'] = final[['Purchased Amount']].sum(axis=1)
final['Total Expenditure'] = final['Total Quantity']*final['Unit Value']

final = final.reset_index().set_index(['j','i'])
final.to_parquet('food_acquired.parquet')
