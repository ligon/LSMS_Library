#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from uganda import harmonized_food_labels
import dvc.api
from lsms import from_dta
import pandas as pd
import json

fn = '../Data/HH/gsec15b.dta'

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
    df = from_dta(dta,convert_categoricals=True)

########################################################################
# Awkwardly we need the numeric codes to assign the item labels we want...
with dvc.api.open(fn,mode='rb') as dta:
    codes = from_dta(dta,convert_categoricals=False)

with dvc.api.open(fn,mode='rb') as dta:
    codes = from_dta(dta,convert_categoricals=False)
d = dict(zip(df.CEB01.tolist(),codes.CEB01.tolist()))
df = df.replace({'CEB01':d})

df = df.replace({'CEB01':harmonized_food_labels()})
# This gives us preferred labels
#######################################################################


df = df.loc[:,[v for v in myvars.values()]].rename(columns={v:k for k,v in myvars.items()})

df = df.set_index(['HHID','item','units']).dropna(how='all')

df['unitvalue_home'] = df['value_home']/df['quantity_home']
df['unitvalue_away'] = df['value_away']/df['quantity_away']
df['unitvalue_own'] = df['value_own']/df['quantity_own']
df['unitvalue_inkind'] = df['value_inkind']/df['quantity_inkind']

unitvalues = df.filter(regex='^unitvalue').dropna(how='all')
prices = df[['market_home','market_away','market_own','farmgate']].dropna(how='all')

values = pd.concat([unitvalues,prices],axis=1)

# Get list of units used in 2019 survey
units = list(set(prices.index.get_level_values('units').tolist()))

# Drop units that aren't strings
units = [s for s in units if type(s)==str]

# Create cleaned up units
myunits = [s.strip() for s in units]
myunits = [s.replace('  ',' ') for s in myunits]
myunits = [s.replace('  ',' ') for s in myunits]
myunits = [s.replace('p(','p (') for s in myunits]
myunits = [s.replace('n(','n (') for s in myunits]
myunits = [s.replace(')-',') -') for s in myunits]
myunits = [s.replace('-B','- B') for s in myunits]
myunits = [s.replace('-M','- M') for s in myunits]
myunits = [s.replace('-S','- S') for s in myunits]
myunits = [s.replace('e(','e (') for s in myunits]
myunits = [s.replace('e-','e -') for s in myunits]
myunits = [s.replace('s(','s (') for s in myunits]
myunits = [s.replace('Sackets','Sacket') for s in myunits]
myunits = [s.replace('Plastin','Plastic') for s in myunits]
myunits = [s.replace('sacket','Sacket') for s in myunits]

unitnames = dict(zip(units,myunits))

with open('../../_/conversion_to_kgs.json','r') as f:
    conversion_to_kgs = pd.Series(json.load(f))

conversion_to_kgs.name='Kgs'
conversion_to_kgs.index.name='units'

values.rename(index=unitnames,level='units')

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

pd.DataFrame({'Price/Kg':p.median(axis=1)}).to_parquet('food_prices.parquet')
