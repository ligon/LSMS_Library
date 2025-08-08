from lsms.tools import get_food_prices, get_food_expenditures, get_household_roster, get_household_identification_particulars
from lsms import from_dta
import numpy as np
import pandas as pd
import dvc.api
from collections import defaultdict
from cfe.df_utils import use_indices
import warnings
import json

if __name__=='__main__':
    import sys
    sys.path.append('../../_')
    sys.path.append('../../../_')
    from local_tools import format_id
else:
    from lsms_library.local_tools import format_id

# Data to link household ids across waves
Waves = {'2005-06':(),
         '2009-10':(), # ID of parent household  in ('GSEC1.dta',"HHID",'HHID_parent'), but not clear how to use
         '2010-11':(),
         '2011-12':(),
         '2013-14':('GSEC1.dta','HHID','HHID_old'),
         '2015-16':('gsec1.dta','HHID','hh',lambda s: s.replace('-05-','-04-')),
         '2018-19':('GSEC1.dta','hhid','t0_hhid'),
         '2019-20':('HH/gsec1.dta','hhid','hhidold')}

def harmonized_unit_labels(fn='../../_/unitlabels.csv',key='Code',value='Preferred Label'):
    unitlabels = pd.read_csv(fn)
    unitlabels.columns = [s.strip() for s in unitlabels.columns]
    unitlabels = unitlabels[[key,value]].dropna()
    unitlabels.set_index(key,inplace=True)

    unitlabels = unitlabels.squeeze().str.strip().to_dict()

    return unitlabels

def harmonized_food_labels(fn='../../_/food_items.org',key='Code',value='Preferred Label'):
    # Harmonized food labels
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:int,2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items[[key,value]].dropna()
    food_items.set_index(key,inplace=True)

    return food_items.squeeze().str.strip().to_dict()

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

    with dvc.api.open(fn,mode='rb') as dta:
        # Prices
        prices,itemlabels=get_food_prices(dta,itmcd=item,HHID=HHID, market=market,
                                          farmgate=farmgate,units=units,itemlabels=food_items)

    prices = prices.replace({'units':unitlabels})
    prices.units = prices.units.astype(str)

    pd.Series(unitlabels).to_csv('unitlabels.csv')

    return prices

def food_acquired(fn,myvars):

    with dvc.api.open(fn,mode='rb') as dta:
        df = from_dta(dta,convert_categoricals=False)

    df = df.loc[:,[v for v in myvars.values()]].rename(columns={v:k for k,v in myvars.items()})

    # Replace missing unit values
    df['units'] = df['units'].fillna('---')

    df = df.set_index(['HHID','item','units']).dropna(how='all')

    df.index.names = ['j','i','u']


    # Fix type of hhids if need be
    if df.index.get_level_values('j').dtype ==float:
        fix = dict(zip(df.index.levels[0],df.index.levels[0].astype(int).astype(str)))
        df = df.rename(index=fix,level=0)

    df = df.rename(index=harmonized_food_labels(),level='i')
    unitlabels = harmonized_unit_labels()
    df = df.rename(index=unitlabels,level='u')

    if not 'market' in df.columns:
        df['market'] = df.filter(regex='^market').median(axis=1)

    # Compute unit values
    df['unitvalue_home'] = df['value_home']/df['quantity_home']
    df['unitvalue_away'] = df['value_away']/df['quantity_away']
    df['unitvalue_own'] = df['value_own']/df['quantity_own']
    df['unitvalue_inkind'] = df['value_inkind']/df['quantity_inkind']

    # Get list of units used in current survey
    units = list(set(df.index.get_level_values('u').tolist()))

    unknown_units = set(units).difference(unitlabels.values())
    if len(unknown_units):
        warnings.warn("Dropping some unknown unit codes!")
        print(unknown_units)
        df = df.loc[df.index.isin(unitlabels.values(),level='u')]

    with open('../../_/conversion_to_kgs.json','r') as f:
        conversion_to_kgs = pd.Series(json.load(f))

    conversion_to_kgs.name='Kgs'
    conversion_to_kgs.index.name='u'

    df = df.join(conversion_to_kgs,on='u')
    df = df.astype(float)

    return df

def food_expenditures(fn='',purchased=None,away=None,produced=None,given=None,item='item',HHID='HHID'):
    food_items = harmonized_food_labels(fn='../../_/food_items.org')

    with dvc.api.open(fn,mode='rb') as dta:
        expenditures,itemlabels=get_food_expenditures(dta,purchased,away,produced,given,itmcd=item,HHID=HHID,itemlabels=food_items)

    expenditures.index.name = 'j'
    expenditures.columns.name = 'i'

    expenditures = expenditures[expenditures.columns.intersection(food_items.values())]
        
    return expenditures


def nonfood_expenditures(fn='',purchased=None,away=None,produced=None,given=None,item='item',HHID='HHID'):
    nonfood_items = harmonized_food_labels(fn='../../_/nonfood_items.org',key='Code',value='Preferred Label')
    with dvc.api.open(fn,mode='rb') as dta:
        expenditures,itemlabels=get_food_expenditures(dta,purchased,away,produced,given,itmcd=item,HHID=HHID,itemlabels=nonfood_items)

    expenditures.index.name = 'j'
    expenditures.columns.name = 'i'
    expenditures = expenditures[expenditures.columns.intersection(nonfood_items.values())]

    return expenditures

def food_quantities(fn='',item='item',HHID='HHID',
                    purchased=None,away=None,produced=None,given=None,units=None):
    food_items = harmonized_food_labels(fn='../../_/food_items.org')

        # Prices
    with dvc.api.open(fn,mode='rb') as dta:
        quantities,itemlabels=get_food_expenditures(dta,purchased,away,produced,given,itmcd=item,HHID=HHID,units=units,itemlabels=food_items)

    quantities.index.names = ['j','u']
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


def other_features(fn,urban=None,region=None,HHID='HHID',urban_converter=None):

    with dvc.api.open(fn,mode='rb') as dta:
        df = get_household_identification_particulars(fn=dta,HHID=HHID,urban=urban,region=region,urban_converter=urban_converter)

    df.index.name = 'j'
    df.columns.name = 'k'

    return df

def id_walk(df, updated_ids, hh_index='j'):
    '''
    Updates household IDs in panel data across different waves separately.

    Parameters:
        df (DataFrame): Panel data with a MultiIndex, including 't' for wave and 'i' (default) for household ID.
        updated_ids (dict): A dictionary mapping each wave to another dictionary that maps original household IDs to updated IDs.
            Format:
                {wave_1: {original_id: new_id, ...},
                 wave_2: {original_id: new_id, ...}, ...}
        hh_index (str): Index name for the household ID level (default is 'i').

    Example:
        updated_ids = {
            '2013-14': {'0001-001': '101012150028', '0009-001': '101015620053', '0005-001': '101012150022'},
            '2016-17': {'0001-002': '0001-001', '0003-001': '0005-001', '0005-001': '0009-001'}
        }

        In this example, IDs are updated independently for each wave.
        Because the same original household ID across different waves may not represent the same household.
        Specifically, household '0005-001' in wave '2016-17' corresponds to household '0009-001' from wave '2013-14', not '0005-001' from '2013-14'.

    The function handles these wave-specific mappings separately, ensuring accurate household identification over time.
    '''
    #seperate df into different waves:
    dfs = {}
    waves = df.index.get_level_values('t').unique()
    for wave in waves:
        dfs[wave] = df[df.index.get_level_values('t') == wave].copy()
    #update ids for each wave
    for wave, df_wave in dfs.items():
        #update ids
        if wave in updated_ids:
            df_wave = df_wave.rename(index=updated_ids[wave], level=hh_index)
            #update the dataframe with the new ids
            dfs[wave] = df_wave
        else:
            continue
    #combine the updated dataframes
    df = pd.concat(dfs.values(), axis=0)

    # df= df.rename(index=updated_ids,level=['t', hh_index])
    df.attrs['id_converted'] = True
    return df  

