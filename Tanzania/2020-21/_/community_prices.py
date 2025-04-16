from lsms_library.local_tools import to_parquet
#!/usr/bin/env python3
import pandas as pd
import dvc.api
from lsms import from_dta
import numpy as np
from pint import UnitRegistry, UndefinedUnitError, DimensionalityError

ureg = UnitRegistry(case_sensitive=False)
ureg.define('Piece = 1*count')

fn = '../Data/cm_sec_f_id.dta'  # Data on prices

# Prices reported for village (=v=), district capital (=d=). Each is price =p=
# is of some weight =w= measured in units =u=.

b = dict(int_key    = 'interview__key',  # interview__{key,id} both unique identifiers?
         i          = 'item_id',
         price_v    = 'cm_f063',
         weight_v   = 'cm_f062',
         unit_v     = 'cm_f061',
         price_d    = 'cm_f066',
         weight_d   = 'cm_f065',
         unit_d     = 'cm_f064',
         )

with dvc.api.open(fn,mode='rb') as dta: df = from_dta(dta)

df = df[b.values()]
df = df.rename(columns={v:k for k,v in b.items()}).set_index(['int_key','i'])

df = df.dropna(how='all')

#########################################
# Now place for which prices are reported
#########################################

fn = '../Data/cm_sec_f.dta'  # Data on prices

c = dict(int_key    = 'interview__key',  # interview__{key,id} both unique identifiers?
         region     = 'id_01',
         district   = 'id_02',
         ward       = 'id_03',
         village    = 'id_04',
         ea         = 'id_05',
         )

with dvc.api.open(fn,mode='rb') as dta: place = from_dta(dta,convert_categoricals=False)

place = place.replace('**CONFIDENTIAL**',np.nan)
place = place.loc[:,place.count()>0] # Drop columns with no data

place = place[c.values()]
place = place.rename(columns={v:k for k,v in c.items()}).set_index(['int_key'])

place = place.dropna(how='all')

### Merge ###
out = pd.merge(df.reset_index('i'),place,on='int_key',how='outer')

# Get regions for households

from lsms.tools import from_dta

fn = '../Data/hh_sec_a.dta'

myvars = dict(HHID='y5_hhid',
              urban='y5_rural',
              domain='domain',
              ea = 'hh_a04_1',
              village = 'hh_a03_3a',
              ward = 'hh_a03_1',
              district = 'hh_a02_1',
              region = 'hh_a01_1',
              )

with dvc.api.open(fn,mode='rb') as dta: hhloc = from_dta(dta)


hhloc = hhloc.replace('**CONFIDENTIAL**',np.nan)
hhloc = hhloc.loc[:,hhloc.count()>0] # Drop columns with no data

hhloc = hhloc[myvars.values()]
hhloc = hhloc.rename(columns={v:k for k,v in myvars.items()}).set_index(['HHID'])

mdict = hhloc[['domain','region']].dropna()

mdict = mdict.drop_duplicates()

mdict = {x[2]:x[1] for x in mdict.to_records()}

out['m'] = out.region.map(mdict)

out = out.reset_index().set_index(['int_key','i','m'])

# Handle unit conversions
def to_kgs(q,u,ureg=ureg):
    """Convert quantity q of units u to kgs or ls"""
    if type(u) is float: return ureg.Quantity(np.nan,'Piece')
    try:
        x = ureg.Quantity(float(q),u.lower())
    except UndefinedUnitError:
        return ureg.Quantity(float(q),'Piece')

    try:
        return x.to(ureg.kilogram)
    except DimensionalityError:
        if x.u == 'Piece': return x
        return x.to(ureg.liter)

def price_per_unit(p,q,ureg=ureg):
    try:
        return p/q
    except ZeroDivisionError:
        return ureg.Quantity(np.nan,q.u)

out['w_v']=out[['weight_v','unit_v']].T.apply(lambda x : to_kgs(x['weight_v'],x['unit_v']))

village_price = out[['price_v','w_v']].T.apply(lambda x: price_per_unit(x['price_v'],x['w_v']))

out['w_d']=out[['weight_d','unit_d']].T.apply(lambda x : to_kgs(x['weight_d'],x['unit_d']))

district_price = out[['price_d','w_d']].T.apply(lambda x: price_per_unit(x['price_d'],x['w_d']))

vg = village_price.apply(lambda x: x.m).groupby(['i','m'])

to_parquet(vg.median().unstack('m'), 'community_prices.parquet')
