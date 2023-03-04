from lsms.tools import get_food_prices, get_food_expenditures, get_household_roster
from lsms import from_dta
import pandas as pd
import numpy as np
import dvc.api
import warnings
import json

Waves = {'2008-15':('upd4_hh_a.dta','UPHI','r_hhid','round'),
         '2019-20':('HH_SEC_A.dta','y4_hhid','sdd_hhid'),
         '2020-21':('hh_sec_a.dta','y4_hhid','y5_hhid')}

def harmonized_food_labels(fn='../../_/food_items.org'):
    # Harmonized food labels
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:int,2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items[['Code','Preferred Label']].dropna()
    food_items.set_index('Code',inplace=True)    

    return food_items.to_dict()['Preferred Label']
    

def prices_and_units(fn='',units='units',item='item',HHID='HHID',market='market',farmgate='farmgate'):

    food_items = harmonized_food_labels(fn='../../_/food_items.org')

    # Unit labels
    with dvc.api.open(fn,mode='rb') as dta:
        sr = pd.io.stata.StataReader(dta)
        try:
            unitlabels = sr.value_labels()[units]
        except KeyError: # No guarantee that keys for labels match variables!?
            foo = sr.value_labels()
            key = [k for k,v in foo.items() if 'Kilogram' in [u[:8] for l,u in v.items()]][0]
            unitlabels = sr.value_labels()[key]

    # Prices
    with dvc.api.open(fn,mode='rb') as dta:
        prices,itemlabels=get_food_prices(dta,itmcd=item,HHID=HHID, market=market,
                                          farmgate=farmgate,units=units,itemlabels=food_items)

    prices = prices.replace({'units':unitlabels})
    prices.units = prices.units.astype(str)

    pd.Series(unitlabels).to_csv('unitlabels.csv')

    return prices

def food_expenditures(fn='',purchased=None,away=None,produced=None,given=None,item='item',HHID='HHID'):
    food_items = harmonized_food_labels(fn='../../_/food_items.org')

    with dvc.api.open(fn,mode='rb') as dta:
        expenditures,itemlabels=get_food_expenditures(dta,purchased,away,produced,given,itmcd=item,HHID=HHID,itemlabels=food_items)

    expenditures.index.name = 'j'
    expenditures.columns.name = 'i'
        
    return expenditures

def food_quantities(fn='',item='item',HHID='HHID',
                    purchased=None,away=None,produced=None,given=None,units=None):
    food_items = harmonized_food_labels(fn='../../_/food_items.org')

        # Prices
    with dvc.api.open(fn,mode='rb') as dta:
        quantities,itemlabels=get_food_expenditures(dta,purchased,away,produced,given,itmcd=item,HHID=HHID,units=units,itemlabels=food_items)

    quantities.index.name = 'j'
    quantities.columns.name = 'i'
        
    return quantities

def age_sex_composition(fn,sex='sex',sex_converter=None,age='age',months_spent='months_spent',HHID='HHID',months_converter=None, convert_categoricals=True,Age_ints=None,fn_type='stata'):

    if Age_ints is None:
        # Match Uganda FCT categories
        Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
        
    with dvc.api.open(fn,mode='rb') as dta:
        df = get_household_roster(fn=dta,HHID=HHID,sex=sex,age=age,months_spent=months_spent,
                                  sex_converter=sex_converter,months_converter=months_converter,
                                  Age_ints=Age_ints)

    df.index.name = 'j'
    df.columns.name = 'k'
    
    return df

def harmonized_unit_labels(fn='../../_/unitlabels.csv',key='Label',value='Preferred Label'):
    unitlabels = pd.read_csv(fn)
    unitlabels.columns = [s.strip() for s in unitlabels.columns]
    unitlabels = unitlabels[[key,value]].dropna()
    unitlabels.set_index(key,inplace=True)
    return unitlabels.squeeze().str.strip().to_dict()

    
def food_acquired(fn,myvars):
    if 'year' in myvars:
        with dvc.api.open(fn,mode='rb') as dta:
            df = from_dta(dta)

        df = df.loc[:,[v for v in myvars.values()]].rename(columns={v:k for k,v in myvars.items()})
        #map round code to actual years
        dict = {1:'2008-09', 2:'2010-11', 3:'2012-13', 4:'2014-15'}
        df.replace({"year": dict},inplace=True)
        df = df.set_index(['HHID','item','year']).dropna(how='all')
        df.index.names = ['j','i','t']
    else:
        with dvc.api.open(fn,mode='rb') as dta:
            df = from_dta(dta)
        df = df.loc[:,[v for v in myvars.values()]].rename(columns={v:k for k,v in myvars.items()})
        df = df.set_index(['HHID','item']).dropna(how='all')
        df.index.names = ['j','i']
        
    # Fix type of hhids if need be
    if df.index.get_level_values('j').dtype ==float:
        fix = {k: v for k, v in zip(df.index.levels[0],df.index.levels[0].astype(int).astype(str))}
        df = df.rename(index=fix,level=0)

    #harmonize food labels 
    #df = df.rename(index=harmonized_food_labels(),level='i')
    unitlabels = {0: float("nan"), 'KILOGRAMS':'Kg', 'GRAMS':'Gram', 'LITRE':'Litre', 'MILLILITRE':'Millilitre', 'PIECES':'Piece'}
    unitcolumn = {'unit_ttl_consume': unitlabels, 'unit_purchase': unitlabels, 'unit_own': unitlabels, 'unit_inkind': unitlabels}
    df.replace(unitcolumn,inplace=True)

    df['unitvalue_purchase'] = df['value_purchase']/df['quant_purchase']

    #with open('../../_/conversion_to_kgs.json','r') as f:
        #conversion_to_kgs = pd.Series(json.load(f))
    #conversion_to_kgs.name='unit_ttl_consume_Kgs'
    #conversion_to_kgs.index.name='unit_ttl_consume'
    #df = df.join(conversion_to_kgs,on='unit_ttl_consume')
    #df = df.astype(float)
    return df

