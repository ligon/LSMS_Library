#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from uganda import harmonized_food_labels, harmonized_unit_labels
import dvc.api
from lsms import from_dta
import pandas as pd
import json
import warnings

fn='../Data/GSEC15B.dta'

# See https://microdata.worldbank.org/index.php/catalog/3795/data-dictionary/F93?file_name=GSEC15B.dta
# Note that notations on Q don't seem to match!

myvars = {'units':'CEB03C',
          'item':'CEB01',
          'HHID':'hhid',
          'market_home':'CEB14a',
          'market_away':'CEB14b',
          'market_own':'CEB14c',
          'farmgate':'CEB15',
          'value_home':'CEB07',
          'value_away':'CEB09',
          'value_own':'CEB11',
          'value_inkind':'CEB013',
          'quantity_home':'CEB06',
          'quantity_away':'CEB08',
          'quantity_own':'CEB10',
          'quantity_inkind':'CEB012'}


with dvc.api.open(fn,mode='rb') as dta:
    df = from_dta(dta,convert_categoricals=False)

df = df.loc[:,[v for v in myvars.values()]].rename(columns={v:k for k,v in myvars.items()})

df = df.set_index(['HHID','item','units']).dropna(how='all')
df = df.rename(index=harmonized_food_labels(),level='item')
unitlabels = harmonized_unit_labels()
df = df.rename(index=unitlabels,level='units')

# Possibly duplicated indices?
dupes = df.index.duplicated()
if dupes.sum():
    warnings.warn("There are %d duplicates in index.  Dropping dupes." % dupes.sum())
    df = df.loc[~dupes,:]

# Compute unit values
df['unitvalue_home'] = df['value_home']/df['quantity_home']
df['unitvalue_away'] = df['value_away']/df['quantity_away']
df['unitvalue_own'] = df['value_own']/df['quantity_own']
df['unitvalue_inkind'] = df['value_inkind']/df['quantity_inkind']

unitvalues = df.filter(regex='^unitvalue').dropna(how='all')
prices = df[['market_home','market_away','market_own','farmgate']].dropna(how='all')

values = pd.concat([unitvalues,prices],axis=1)

# Get list of units used in survey
units = list(set(prices.index.get_level_values('units').tolist()))

unknown_units = set(units).difference(unitlabels.values())
if len(unknown_units):
    warnings.warn("Dropping some unknown unit codes!")
    print(unknown_units)
    df = df.loc[df.index.isin(unitlabels.values(),level='units')]

with open('../../_/conversion_to_kgs.json','r') as f:
    conversion_to_kgs = pd.Series(json.load(f))

conversion_to_kgs.name='Kgs'
conversion_to_kgs.index.name='units'

values = values.join(conversion_to_kgs,on='units')
values = values.divide(values.Kgs,axis=0)  # Convert to Kgs
del values['Kgs']

values = values.droplevel('units').dropna(how='all')
values = values.astype(float)

home = values[['unitvalue_home','market_home']]
home.columns = ['unitvalue','price']
away = values[['unitvalue_away','market_away']]
away.columns = ['unitvalue','price']
own = values[['unitvalue_own','market_own']]
own.columns = ['unitvalue','price']

# No variation to speak of between unitvalues and prices, so combine...
p = pd.DataFrame({'home':home.mean(axis=1),
                  'away':away.mean(axis=1),
                  'own':own.mean(axis=1)})

# Almost no differences between farmgate and own prices.  Combine...
p['own'] = pd.DataFrame({'own':p.own,'farmgate':values.farmgate}).mean(axis=1)
p.index.names = ['j','i']

p = p.median(axis=1)
pbar = p.groupby('i').median()

pd.DataFrame({'Price/Kg':pbar}).to_parquet('food_prices.parquet')
