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
from lsms_library.local_tools import add_markets_from_other_features, format_id, id_walk, RecursiveDict, get_dataframe, update_id
from collections import defaultdict

country = 'Tanzania'

def map_08_15(df, col):
    # hhid index is defined as 'i' in this function to use panel_ids function in local_tools
    hhid = df[col]
    hhid_sorted = hhid.sort_values(['UPHI', 'round'])
    hhid_sorted['previous_i'] = hhid_sorted.groupby('UPHI')['r_hhid'].shift(1)
    map_round = {1: '2008-09', 2: '2010-11', 3: '2012-13', 4: '2014-15'}
    hhid_sorted['round'] = hhid_sorted['round'].map(map_round)
    hhid_sorted = hhid_sorted.dropna(how='any')
    hhid_sorted.rename(columns={'r_hhid': 'i', 'round': 't'}, inplace=True)
    hhid_sorted = hhid_sorted.set_index(['t', 'i'])[['previous_i']]
    hhid_sorted = hhid_sorted.loc[~hhid_sorted.index.duplicated(keep='first')]
    return hhid_sorted

Waves = {'2008-15':('upd4_hh_a.dta',['r_hhid','round','UPHI'], map_08_15),
         '2019-20':('HH_SEC_A.dta','sdd_hhid','y4_hhid'),
         '2020-21':('hh_sec_a.dta','y5_hhid','y4_hhid')}

waves = ['2008-09', '2010-11', '2012-13', '2014-15', '2019-20', '2020-21']
wave_folder_map = {'2008-09':'2008-15', '2010-11':'2008-15', '2012-13':'2008-15', '2014-15':'2008-15', '2019-20':'2019-20', '2020-21':'2020-21'}

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

def other_features(fn,urban=None,region=None,HHID='HHID',urban_converter=None,wave=None,**kwargs):
    """
    Pass a dictionary othervars to grab other variables.
    """
    with dvc.api.open(fn,mode='rb') as dta:
        df = get_household_identification_particulars(fn=dta,
                                                      HHID=HHID,
                                                      urban=urban,
                                                      region=region,
                                                      urban_converter=urban_converter,
                                                      wave=wave,**kwargs)
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



def panel_ids(Waves):
    '''
    Input: DataFrame with a MultiIndex that includes a level named 't' representing the wave and 'i' current househod ID'
            And single 'previous_i' column as the previous household ID.
    Output: Wave-specific panel id mapping dictionaires and a recursive dictionary of tuple of (wave, household identifiers)
    '''
    if isinstance(Waves, dict):
        dfs = []
        for wave_year, wave_info in Waves.items():
            if not wave_info:
                continue  # skip empty entries

            file_path = f"../{wave_year}/Data/{wave_info[0]}"
            if isinstance(wave_info[1], list):
                columns = wave_info[1]
            else:
                columns = [wave_info[1], wave_info[2]]

            df = get_dataframe(file_path)[columns]

            # Process mapping when recent_id is a list (list-based mapping)
            if isinstance(wave_info[1], list): #tanzania
                df = wave_info[2](df, wave_info[1])
            else:
                df[wave_info[1]] = df[wave_info[1]].apply(format_id)
                df[wave_info[2]] = df[wave_info[2]].apply(format_id)
                # If a transformation function is provided (tuple length 4), apply it to the old_id column
                if len(wave_info) == 4:
                    df[wave_info[2]] = df[wave_info[2]].apply(wave_info[3])
                df['t'] = wave_year
                df = df.rename(columns={wave_info[1]: 'i', wave_info[2]: 'previous_i'})
                df = df.set_index(['t', 'i'])[['previous_i']]
            dfs.append(df)
        panel_ids_df = pd.concat(dfs, axis=0)
    else:
        # If Waves is not a dictionary, assume it's a DataFrame
        panel_ids_df = Waves.copy()

    updated_wave = {}
    check_id_split = {}
    sorted_waves = sorted(panel_ids_df.index.get_level_values('t').unique())
    recursive_D = RecursiveDict()
    for wave_year in sorted_waves:
        df = panel_ids_df[panel_ids_df.index.get_level_values('t') == wave_year].copy().reset_index()
        wave_matches = df[['i', 'previous_i']].dropna().set_index('i')['previous_i'].to_dict()
        previous_wave = sorted_waves[sorted_waves.index(wave_year) - 1] if sorted_waves.index(wave_year) > 0 else None
        if wave_year == '2020-21':
            previous_wave = '2014-15'
        if previous_wave:
            previous_wave_matches = updated_wave[previous_wave]
            # update the current wave matches dictionary values to the previous wave matches
            wave_matches = {k: previous_wave_matches.get(v, v)for k, v in wave_matches.items()}
            recursive_D.update({(wave_year, k): (previous_wave, v) for k, v in wave_matches.items()})
        wave_matches, check_id_split = update_id(wave_matches,  check_id_split)
        updated_wave[wave_year] = wave_matches
    return recursive_D, updated_wave