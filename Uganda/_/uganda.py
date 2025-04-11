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

#def id_walk(df, updated_ids, index ='j'):
#    level_num = df.index.names.index(index)
#    new_level = df.index.get_level_values(index).map(lambda x: updated_ids.get(x, x))
#    df.index = df.index.set_levels(df.index.levels[:level_num] + [new_level] + df.index.levels[level_num + 1:])
#    return df
def id_walk(df, updated_ids, index ='j'):
    return df.rename(index=updated_ids,level=index)

def panel_attrition(df, Waves, return_ids=False, waves = None,  split_households_new_sample=True):
    """
    Produce an upper-triangular) matrix showing the number of households (j) that
    transition between rounds (t) of df.
            split_households_new_sample (bool): Determines how to count split households:
                                - If True, we assume split_households as new sample. So we
                                     do not count and trace splitted household, only counts 
                                     the primary household in each split. The number represents
                                     how many main (primary) households in previous waves have 
                                     appeared in current round.
                                - If False, counts all split households that can be traced 
                                    back to previous wave households. The number represents how 
                                    many households (including splitted households
                                    round can be traced back to the previous round.
    
    Note: First three rounds used same sample. Splits of the main households may happen in different rounds.
    """
    idxs = df.reset_index().groupby('t')['j'].apply(list).to_dict()

    if waves is None:
        waves = list(Waves.keys())

    foo = pd.DataFrame(index=waves,columns=waves)
    IDs = {}
    for m,s in enumerate(waves):
        for t in waves[m:]:
            pairs = set(idxs[s]).intersection(idxs[t])
            list2_rest = set(idxs[t]) - pairs
            if not split_households_new_sample:
                new_paired = {i for i in list2_rest  if i.split('_')[0] in idxs[s]}
                pairs.update(new_paired)   
                
            IDs[(s,t)] = pairs
            foo.loc[s,t] = len(IDs[(s,t)])

    if return_ids:
        return foo,IDs
    else:
        return foo
