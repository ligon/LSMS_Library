from lsms.tools import get_food_prices, get_food_expenditures, get_household_roster
import pandas as pd
from lsms import from_dta
import numpy as np
import dvc.api
from collections import defaultdict

# Data to link household ids across waves
Waves = {'2009-10':(),
         '2013-14':(),
         '2017-18':('00_hh_info.dta', 'FPrimary', 'FPrimary_original')
         }

def split_by_visit(df, first_visit, last_visit, t, ind = ['j','t','i'], unit_col = None, aggregate_amount = False):
    df = df.set_index([ind[0]] + ind[2:])
    df_by_visit = []
    for i in range(first_visit, last_visit+1):
        tem = df[df.columns[df.columns.str.contains(str(i))]]
        temp = tem.dropna(how='all').copy()
        temp['t']= [(t,str(i))] * len(temp) #a tuple in the form: (round, visit)
        temp = temp.reset_index().set_index(ind)
        temp.columns = ['_'.join(c.split('_')[:-1]) for c in temp.columns]
        #temp = temp.set_index('t', append = True)
        df_by_visit.append(temp)
    result = pd.concat(df_by_visit)
    result = result.rename(columns={unit_col: 'u'})
    if aggregate_amount:
        if unit_col != None:
            return result.groupby(ind + ['u']).agg(sum)
        return result.groupby(ind).agg(sum) 
    if unit_col != None:
        return result.reset_index().set_index(ind + ['u'])
    return result.reset_index().set_index(ind)

def harmonized_food_labels2(fn='../../_/food_items.org'):
    # Harmonized food labels
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:int,2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items[['Code','Preferred Label']].dropna()
    food_items.set_index('Code',inplace=True)    

    return food_items.to_dict()['Preferred Label']

def harmonized_food_labels(fn='../../_/food_items.org',key=list(Waves.keys()),value='Preferred Label'):
    # Harmonized food labels
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:lambda s: s.strip(),2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items.loc[:,food_items.count()>0]
    food_items = food_items.drop(columns = ['Food Codes','FCT Label']).apply(lambda x: x.str.strip())

    if type(key) == list :
        for k in key:
            if type(k) is not str:  # Assume a series of foods
                myfoods = set(k.values)
                for k in food_items.columns:
                    if len(myfoods.difference(set(food_items[k].values)))==0: # my foods all in key
                        break

        food_items = food_items[key + [value]].replace('---', np.nan).dropna(how = 'all')
    else:
        food_items = food_items[[key] + [value]].replace('---', np.nan).dropna(how = 'all')
        
    food_items.set_index(key,inplace=True)

    return food_items.squeeze().str.strip().to_dict()

def prices_and_units(fn='',units='units',item='item',HHID='HHID',market='market',farmgate='farmgate'):

    #food_items = harmonized_food_labels(fn='../../_/food_items.org')

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
                                          farmgate=farmgate,units=units,convert_categoricals=True)

    prices = prices.replace({'units':unitlabels})
    prices.units = prices.units.astype(str)

    pd.Series(unitlabels).to_csv('unitlabels.csv')

    return prices

def food_expenditures(fn='',purchased=None,away=None,produced=None,given=None,item='item',HHID='HHID'):

    with dvc.api.open(fn,mode='rb') as dta:
        expenditures,itemlabels=get_food_expenditures(dta,purchased,away,produced,given,itmcd=item,HHID=HHID)

    return expenditures

def food_quantities(fn='',item='item',HHID='HHID',
                    purchased=None,away=None,produced=None,given=None,units=None):

    with dvc.api.open(fn,mode='rb') as dta:
        quantities,itemlabels=get_food_expenditures(dta,purchased,away,produced,given,itmcd=item,HHID=HHID,units=units,convert_categoricals=True)

    return quantities

def age_sex_composition(fn,sex='sex',sex_converter=None,
                        age='age',months_spent='months_spent',HHID='HHID',months_converter=None, convert_categoricals=True,Age_ints=None,fn_type='stata'):

    with dvc.api.open(fn,mode='rb') as dta:
        df = get_household_roster(fn=dta,HHID=HHID,sex=sex,age=age,months_spent=months_spent,
                                  sex_converter=sex_converter,months_converter=months_converter)
    df.index.name = 'j'
    df.columns.name = 'k'
    
    return df

def household_characteristics(fn='',sex='',age='',HHID='HHID',months_spent='months_spent'):

    if type(sex) in [list,tuple]:
        sex,sex_converter = sex
    else:
        sex_converter = None

    with dvc.api.open(fn,mode='rb') as dta:
        df = get_household_roster(dta,sex=sex,sex_converter=sex_converter,age=age,HHID=HHID,months_spent=months_spent,fn_type='stata')

    df.index.name = 'j'
    df.columns.name = 'k'
    df['log HSize'] = np.log(df[['girls', 'boys', 'men', 'women']].sum(axis=1))
    
    return df


def change_id(x,fn=None,id0=None,id1=None,transform_id1=None):
    """Replace instances of id0 with id1.

    The identifier id0 is assumed to be unique.

    If mapping id0->id1 is not one-to-one, then id1 modified with
    suffixes of the form _%d, with %d replaced by a sequence of
    integers.
    """
    idx = x.index.names

    if fn is None:
        x = x.reset_index()
        if x['j'].dtype==float:
            x['j'] = x['j'].astype(str).apply(lambda s: s.split('.')[0]).replace('nan',np.nan)
        elif x['j'].dtype==int:
            x['j'] = x['j'].astype(str)
        elif x['j'].dtype==str:
            x['j'] = x['j'].replace('',np.nan)

        x = x.set_index(idx)

        return x

    try:
        with open(fn,mode='rb') as dta:
            id = from_dta(dta)
    except IOError:
        with dvc.api.open(fn,mode='rb') as dta:
            id = from_dta(dta)

    id = id[[id0,id1]]
    id[id1] = id[id1].replace('', np.nan).fillna(id[id0])

    for column in id:
        if id[column].dtype==float:
            id[column] = id[column].astype(str).apply(lambda s: s.split('.')[0]).replace('nan',np.nan)
        elif id[column].dtype==int:
            id[column] = id[column].astype(str).replace('nan',np.nan)
        elif id[column].dtype==object:
            id[column] = id[column].replace('nan',np.nan)
            id[column] = id[column].replace('',np.nan)

    ids = dict(id[[id0,id1]].values.tolist())

    if transform_id1 is not None:
        ids = {k:transform_id1(v) for k,v in ids.items()}

    d = defaultdict(list)

    for k,v in ids.items():
        d[v] += [k]

    try:
        d.pop(np.nan)  # Get rid of nan key, if any
    except KeyError: pass

    updated_id = {}
    for k,v in d.items():
        if len(v)==1: updated_id[v[0]] = k
        else:
            for it,v_element in enumerate(v):
                updated_id[v_element] = '%s_%d' % (k,it)

    x = x.reset_index()
    x['j'] = x['j'].map(updated_id).fillna(x['j'])
    x = x.set_index(idx)

    assert x.index.is_unique, "Non-unique index."

    return x
