from lsms.tools import get_food_prices, get_food_expenditures, get_household_roster, get_household_identification_particulars
from lsms import from_dta
import pandas as pd
import numpy as np
import dvc.api
import warnings
import json
import sys
sys.path.append('../../_')
sys.path.append('../../../_')
from local_tools import add_markets_from_other_features

country = 'Tanzania'

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

def age_sex_composition(fn,sex='sex',sex_converter=None,age='age',
                        months_spent='months_spent',HHID='HHID',months_converter=None,
                        wave=None,convert_categoricals=True,Age_ints=None,fn_type='stata'):

    if Age_ints is None:
        # Match Uganda FCT categories
        Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))

    with dvc.api.open(fn,mode='rb') as dta:
        df = get_household_roster(fn=dta,HHID=HHID,sex=sex,age=age,months_spent=months_spent,
                                  sex_converter=sex_converter,months_converter=months_converter,
                                  Age_ints=Age_ints,
                                  wave=wave)

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
    with dvc.api.open(fn,mode='rb') as dta:
        df = from_dta(dta)
    df = df.loc[:,list(myvars.values())].rename(columns={v:k for k,v in myvars.items()})

    if 'year' in myvars:
        #map round code to actual years
        dict = {1:'2008-09', 2:'2010-11', 3:'2012-13', 4:'2014-15'}
        df.replace({"year": dict},inplace=True)
        df = df.set_index(['HHID','item','year']).dropna(how='all')
        df.index.names = ['j','i','t']
        try:
            # Attempt to assert that the index is unique
            assert df.index.is_unique, "Non-unique index!  Fix me!"
        except AssertionError as e:
            # Drop completely duplicated rows 
            # Same HH recorded down multiple times due to tracking of complete HH lineage in the UPHI system
            if df[~df.index.duplicated()].shape[0] == df.reset_index().drop_duplicates().shape[0]:
                pd.testing.assert_frame_equal(df.reset_index().drop_duplicates().set_index(['j','i','t']), df[~df.index.duplicated()])
                df = df[~df.index.duplicated()]
                if not df.index.is_unique:
                    raise ValueError("Non-unique index! Even after attempted fix.")
            else:
                raise e
    else:
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

    #fix quantities that are read as categorical vars
    df.replace(['none', 'NONE', 'hakuna'], 0, inplace = True)
    df = df.astype({"quant_purchase": 'float64',
                    "quant_own" : 'float64',
                    "quant_inkind" : 'float64'})

    df['unitvalue_purchase'] = df['value_purchase']/df['quant_purchase']
    df['unitvalue_purchase'] = df['unitvalue_purchase'].where(np.isfinite(df['unitvalue_purchase']))


    #with open('../../_/conversion_to_kgs.json','r') as f:
        #conversion_to_kgs = pd.Series(json.load(f))
    #conversion_to_kgs.name='unit_ttl_consume_Kgs'
    #conversion_to_kgs.index.name='unit_ttl_consume'
    #df = df.join(conversion_to_kgs,on='unit_ttl_consume')
    #df = df.astype(float)
    return df

def other_features(fn,urban=None,region=None,HHID='HHID',urban_converter=None,wave=None):
    """
    Pass a dictionary othervars to grab other variables.
    """
    with dvc.api.open(fn,mode='rb') as dta:
        df = get_household_identification_particulars(fn=dta,
                                                      HHID=HHID,
                                                      urban=urban,
                                                      region=region,
                                                      urban_converter=urban_converter,
                                                      wave=wave)
    # Fix any floats in j
    df.index.name = 'j'
    k = df.index.get_level_values('j')
    f2s = {i:str(i).split('.')[0] for i in k}

    df.columns.name = 'k'

    df = df.rename(index=f2s,level='j')

    return df


def id_match(df, wave, waves_dict):
    df = df.reset_index()
    if len(waves_dict[wave]) == 3:
        if 'y4_hhid' and 'UPHI' not in df.columns:
            with dvc.api.open('../%s/Data/%s' % (wave,waves_dict[wave][0]),mode='rb') as dta:
                h = from_dta(dta)
            h = h[[waves_dict[wave][1], waves_dict[wave][2]]]
            m = df.merge(h, how = 'left', left_on ='j', right_on =waves_dict[wave][2])

            with dvc.api.open('../2008-15/Data/upd4_hh_a.dta',mode='rb') as dta:
                uphi = from_dta(dta)[['UPHI','r_hhid','round']]
            uphi['UPHI'] = uphi['UPHI'].astype(int).astype(str)
            y4 = uphi.loc[uphi['round']==4, 'r_hhid'].to_frame().rename(columns ={'r_hhid':'y4_hhid'})
            uphi = uphi.join(y4)    
            uphi = uphi[['UPHI', 'y4_hhid']].dropna()
            m = m.merge(uphi, how= 'left', on = 'y4_hhid')

            m['UPHI'].replace('', np.nan, inplace=True)
            m['UPHI'] = m['UPHI'].fillna(m.pop(waves_dict[wave][2]))
            m.j = m.UPHI
            m = m.drop(columns=['UPHI', 'y4_hhid'])
            if 't' not in m.columns:
                m.insert(1, 't', wave) 

    if len(waves_dict[wave]) == 4:
        if 'UPHI'  in df.columns: 
            m = df.rename(columns={'UPHI': 'j'})
        else: 
            with dvc.api.open('../%s/Data/%s' % (wave,waves_dict[wave][0]),mode='rb') as dta:
                h = from_dta(dta)
            h = h[[waves_dict[wave][1], waves_dict[wave][2], waves_dict[wave][3]]]
            h[waves_dict[wave][1]] = h[waves_dict[wave][1]].astype(int).astype(str)
            dict = {1:'2008-09', 2:'2010-11', 3:'2012-13', 4:'2014-15'}
            h.replace({"round": dict},inplace=True)
            m = df.merge(h.drop_duplicates(), how = 'left', left_on =['j','t'], right_on =[waves_dict[wave][2], waves_dict[wave][3]])
            m['UPHI'] = m['UPHI'].fillna(m.pop('j'))
            m = m.rename(columns={'UPHI': 'j'})
            m = m.drop(columns=[waves_dict[wave][2], waves_dict[wave][3]])
    return m

def new_harmonize_units(df, unit_conversion):
    pair = {'quant': ['quant_ttl_consume', 'quant_purchase', 'quant_own', 'quant_inkind'] ,
        'unit': ['unit_ttl_consume', 'unit_purchase', 'unit_own', 'unit_inkind']}
    
    #convert categorical columns to object columns for fillna to work
    df[pair['unit']] = df[pair['unit']].astype('object') 

    df = df.fillna(0).replace(unit_conversion).replace(['none', 'NONE', 'hakuna'], 0)
    pattern = r"[p+]"
    for i in range(4):
        df[pair['quant'][i]] = df[pair['quant'][i]].astype(np.int64) * df[pair['unit'][i]]
        df[pair['quant'][i]].replace('', 0, inplace=True)
        if df[pair['quant'][i]].dtype != 'O':
            df[pair['unit'][i]] = 'kg'
        else: 
            df[pair['unit'][i]] = np.where(df[pair['quant'][i]].str.contains(pattern).to_frame() == True, 'piece', 'kg')
            df[pair['quant'][i]] = df[pair['quant'][i]].apply(lambda x: x if str(x).count('p') == 0 else str(x).count('p'))

    df['agg_u'] = df[pair['unit']].apply(lambda x: max(x) if min(x) == max(x) else min(x) + '+' + max(x), axis = 1)

    df['unitvalue_purchase'] = df['value_purchase']/df['quant_purchase']
    df.replace([np.inf, -np.inf, 0], np.nan, inplace=True)
    return df
