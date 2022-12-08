#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from uganda import harmonized_food_labels, harmonized_unit_labels
import dvc.api
from lsms import from_dta
import pandas as pd
import json
import warnings

fn = '../Data/HH/gsec15b.dta'

fn='../Data/GSEC14A.dta'

myvars = dict(item='h14aq2',
              HHID='HHID',
              market='h14aq12',
              farmgate='h14aq13',
              value_home='h14aq5',
              value_away='h14aq7',
              value_own='h14aq9',
              value_inkind='h14aq11',
              quantity_home='h14aq4',
              quantity_away='h14aq6',
              quantity_own='h14aq8',
              quantity_inkind='h14aq10',
              units='h14aq3')

with dvc.api.open(fn,mode='rb') as dta:
    df = from_dta(dta,convert_categoricals=False)


df = df.loc[:,[v for v in myvars.values()]].rename(columns={v:k for k,v in myvars.items()})

df = df.set_index(['HHID','item','units']).dropna(how='all')
df = df.rename(index=harmonized_food_labels(),level='item')
unitlabels = harmonized_unit_labels()
df = df.rename(index=unitlabels,level='units')

# Compute unit values
df['unitvalue_home'] = df['value_home']/df['quantity_home']
df['unitvalue_away'] = df['value_away']/df['quantity_away']
df['unitvalue_own'] = df['value_own']/df['quantity_own']
df['unitvalue_inkind'] = df['value_inkind']/df['quantity_inkind']

unitvalues = df.filter(regex='^unitvalue').dropna(how='all')
prices = df[['market','farmgate']].dropna(how='all')

values = pd.concat([unitvalues,prices],axis=1)

# Get list of units used in 2019 survey
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

p = values.median(axis=1)
p.index.names = ['j','i']

pbar = p.groupby('i').median()

pd.DataFrame({'Price/Kg':pbar}).to_parquet('food_prices.parquet')
