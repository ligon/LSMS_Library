from lsms.tools import get_food_prices, get_food_expenditures, get_household_roster, get_household_identification_particulars
from lsms import from_dta
import numpy as np
import pandas as pd
import dvc.api
from collections import defaultdict
from cfe.df_utils import use_indices
import warnings
import json
import difflib
import types
from pyarrow.lib import ArrowInvalid
from functools import lru_cache
from pathlib import Path

def _to_numeric(x,coerce=False):
    try:
        if coerce:
            return pd.to_numeric(x,errors='coerce')
        else:
            return pd.to_numeric(x)
    except (ValueError,TypeError):
        return x

@lru_cache(maxsize=3)
def get_dataframe(fn,convert_categoricals=True,encoding=None,categories_only=False):
    """From a file named fn, try  to return a dataframe.

    Hope is that caller can be agnostic about file type,
    or if file is local or on a dvc remote.
    """

    def local_file(fn):
    # Is the file local?
        try:
            with open(fn) as f:
                pass
            return True
        except FileNotFoundError:
            return False

    def read_file(f,convert_categoricals=convert_categoricals,encoding=encoding):
        try:
            return pd.read_parquet(f)
        except (ArrowInvalid,):
            pass

        try:
            f.seek(0)
            return from_dta(f,convert_categoricals=convert_categoricals,encoding=encoding,categories_only=categories_only)
        except ValueError:
            pass

        try:
            f.seek(0)
            return pd.read_csv(f,encoding=encoding)
        except (pd.errors.ParserError, UnicodeDecodeError):
            pass

        try:
            f.seek(0)
            return pd.read_excel(f)
        except (pd.errors.ParserError, UnicodeDecodeError):
            pass

        try:
            f.seek(0)
            return pd.read_feather(f)
        except (pd.errors.ParserError, UnicodeDecodeError):
            pass

        try:
            f.seek(0)
            return pd.read_fwf(f)
        except (pd.errors.ParserError, UnicodeDecodeError):
            pass

        raise ValueError(f"Unknown file type for {fn}.")

    if local_file(fn):
        with open(fn,mode='rb') as f:
            df = read_file(f,convert_categoricals=convert_categoricals,encoding=encoding)
    else:
        with dvc.api.open(fn,mode='rb') as f:
            df = read_file(f,convert_categoricals=convert_categoricals,encoding=encoding)

    return df

def df_data_grabber(fn,idxvars,convert_categoricals=True,encoding=None,orgtbl=None,**kwargs):
    """From a file named fn, grab both index variables and additional variables
    specified in kwargs and construct a pandas dataframe.

    A special case: if fn is an orgfile, grab orgtbl.

    For both idxvars and kwargs, expect one of the three following formats:

     - Simple: {newvarname:existingvarname}, where "newvarname" is the name of
      the variable we want in the final dataframe, and "existingvarname" is the
      name of the variable as it's found in fn; or

     - Tricky: {newvarname:(existingvarname,transformation)}, where varnames are
       as in "Simple", but where "transformation" is a function mapping the
       existing data into the form desired for newvarname; or

     - Trickier: {newvarname:(listofexistingvarnames,transformation)}, where newvarname is
       as in "Simple", but where "transformation" is a function mapping the variables in
       listofexistingvarnames into the form desired for newvarname.

    Options convert_categoricals and encoding are passed to lsms.from_dta, and
    are documented there.

    Ethan Ligon                                                      March 2024

    """

    def grabber(df,v):
        if isinstance(v,str): # Simple
            return df[v]
        else:
            s,f = v
            if isinstance(f,types.FunctionType):  # Tricky & Trickier
                if isinstance(s,str):
                    return df[s].apply(f)
                else:
                    return df[s].apply(f,axis=1)
            elif isinstance(f,dict):
                return df[s].apply(lambda x: f.get(x,x))

        raise ValueError(df_data_grabber.__doc__)

    if orgtbl is None:
        df = get_dataframe(fn,convert_categoricals=convert_categoricals,encoding=encoding)
    else:
        df = df_from_orgfile(fn,name=orgtbl,encoding=encoding)
        if df.shape[0]==0:
            raise KeyError(f'No table {orgtbl} in {fn}.')

    out = {}

    if isinstance(idxvars,str):
        idxvars={idxvars:idxvars}

    for k,v in idxvars.items():
        out[k] = grabber(df,v)

    out = pd.DataFrame(out)

    if len(kwargs):
        try:
            for k,v in kwargs.items():
                out[k] = grabber(df,v)
        except AttributeError:
            if isinstance(kwargs,str):
                out[k] = df[k]
            else: # A list?
                for k in kwargs:
                    out[k] = df[k]
    else:
        out = df

    out = out.set_index(list(idxvars.keys()))

    return out

def get_categorical_mapping(fn='categorical_mapping.org',tablename=None,idxvars='Code',
                            dirs=['./','../../_/','../../../_/'],asdict=True,**kwargs):
    """Return mappings for categories.

    By default, searches for =tablename= in an orgfile
    'categorical_mapping.org'. But if fn is a path to a dta file instead,
    returns categories for tablename from the stata file.
    """
    ext = Path(fn).suffix

    if ext.lower()=='.dta': # A stata file.
        cats = get_dataframe(fn,convert_categoricals=True,categories_only=True)
        if tablename is None:
            return cats
        else:
            return cats[tablename]

    for d in dirs:
        try:
            if d[-1]!="/": d+='/'
            df = df_data_grabber(d+fn,idxvars,orgtbl=tablename,**kwargs)
            df = df.squeeze()
            if asdict:
                return df.to_dict()
            else:
                return df
        except (FileNotFoundError,KeyError) as error:
            exc = error

    exc.add_note(f"No table {tablename} found in any file {fn} in directories {dirs}.")
    raise exc


def harmonized_unit_labels(fn='../../_/unitlabels.csv',key='Code',value='Preferred Label'):
    unitlabels = pd.read_csv(fn)
    unitlabels.columns = [s.strip() for s in unitlabels.columns]
    unitlabels = unitlabels[[key,value]].dropna()
    unitlabels.set_index(key,inplace=True)

    return unitlabels.squeeze().str.strip().to_dict()


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

def food_acquired(fn,myvars,convert_categoricals):

    with dvc.api.open(fn,mode='rb') as dta:
        df = from_dta(dta,convert_categoricals=False)

    df = df.loc[:,[v for v in myvars.values()]].rename(columns={v:k for k,v in myvars.items()})

    df = df.set_index(['HHID','item','units']).dropna(how='all')

    df.index.names = ['j','i','units']


    # Fix type of hhids if need be
    if df.index.get_level_values('j').dtype ==float:
        fix = dict(zip(df.index.levels[0],df.index.levels[0].astype(int).astype(str)))
        df = df.rename(index=fix,level=0)

    df = df.rename(index=harmonized_food_labels(),level='i')
    unitlabels = harmonized_unit_labels()
    df = df.rename(index=unitlabels,level='units')

    if not 'market' in df.columns:
        df['market'] = df.filter(regex='^market').median(axis=1)

    # Compute unit values
    df['unitvalue_home'] = df['value_home']/df['quantity_home']
    df['unitvalue_away'] = df['value_away']/df['quantity_away']
    df['unitvalue_own'] = df['value_own']/df['quantity_own']
    df['unitvalue_inkind'] = df['value_inkind']/df['quantity_inkind']

    # Deal with possible zeros in quantities
    df['unitvalue_home'] = df['unitvalue_home'].where(np.isfinite(df['unitvalue_home']))
    df['unitvalue_away'] = df['unitvalue_away'].where(np.isfinite(df['unitvalue_away']))
    df['unitvalue_own'] = df['unitvalue_own'].where(np.isfinite(df['unitvalue_own']))
    df['unitvalue_inkind'] = df['unitvalue_inkind'].where(np.isfinite(df['unitvalue_inkind']))


    # Get list of units used in current survey
    units = list(set(df.index.get_level_values('units').tolist()))

    unknown_units = set(units).difference(unitlabels.values())
    if len(unknown_units):
        warnings.warn("Dropping some unknown unit codes!")
        print(unknown_units)
        df = df.loc[df.index.isin(unitlabels.values(),level='units')]

    with open('../../_/conversion_to_kgs.json','r') as f:
        conversion_to_kgs = pd.Series(json.load(f))

    conversion_to_kgs.name='Kgs'
    conversion_to_kgs.index.name='units'

    df = df.join(conversion_to_kgs,on='units')
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
            x['j'].astype(str).apply(lambda s: s.split('.')[0]).replace('nan',np.nan)
        elif x['j'].dtype==int:
            x['j'] = x['j'].astype(str)

        x = x.set_index(idx)

        return x

    try:
        with open(fn,mode='rb') as dta:
            id = from_dta(dta)
    except IOError:
        with dvc.api.open(fn,mode='rb') as dta:
            id = from_dta(dta)

    id = id[[id0,id1]]

    for column in id:
        if id[column].dtype==float:
            id[column] = id[column].astype(str).apply(lambda s: s.split('.')[0]).replace('nan',np.nan)
        elif id[column].dtype==int:
            id[column] = id[column].astype(str).replace('nan',np.nan)
        elif id[column].dtype==object:
            id[column] = id[column].replace('nan',np.nan)

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



def add_markets_from_other_features(country,df,additional_other_features=False):
    of = pd.read_parquet(f"../{country}/var/other_features.parquet")

    df_idx = df.index.names

    try:
        df = df.droplevel('m')
    except KeyError:
        pass

    colname = df.columns.names

    if additional_other_features:
        if 'm' in of.index.names:
            df = df.join(of.reset_index('m'), on=['j','t'])
        else:
            df = df.join(of, on=['j','t'])
    else:
        if 'm' in of.index.names:
            df = df.join(of.reset_index('m')['m'], on=['j','t'])
        else:
            df = df.join(of['m'], on=['j','t'])


    df = df.reset_index().set_index(df_idx)
    df.columns.names = colname

    return df

def df_from_orgfile(orgfn,name=None,set_columns=True,to_numeric=True,encoding=None):
    """Extract the org table with name from the orgmode file named orgfn; return a pd.DataFrame.

    If name is None (the default), then we assume the orgtable is the very first
    thing in the file, with the possible exception of options (lines starting with #+).

    Note that we assume that cells with the string '---' should be null.

    Ethan Ligon                                                       March 2023
    """
    # Grab file as a list of strings
    with open(orgfn,'r',encoding=encoding) as f:
        contents = f.readlines()

    # Get indices of #+name: lines:
    names = [i for i,s in enumerate(contents) if f'#+name: {name}' in s.strip().lower()]

    if len(names)==0:
        #warnings.warn(f'No table {name} in {orgfn}.')
        start = 0
    elif len(names)>1:
        start = names[0]
        warnings.warn(f'More than one table with {name} in {orgfn}.  Reading first one at line {start}.')
    else:
        start = names[0]

    # Advance to line that starts table
    i = start
    while contents[i].strip()[:2] == '#+': i +=1

    table =[]
    nextline = contents[i].strip()
    if set_columns and len(nextline) and nextline[0] == '|':
        columns = [s.strip() for s in nextline.split('|')[1:-1]]
        i+=1
        nextline = contents[i].strip()
    else:
        columns = None

    while len(nextline) and nextline[0] == '|':
        line = contents[i].strip()
        if line[-1] == '|' and  line[:2] != '|-':
            table.append([s.strip() for s in line.split('|')[1:-1]])
        i+=1
        try:
            nextline = contents[i].strip()
        except IndexError: # End of file?
            break

    df = pd.DataFrame(table,columns=columns)

    df = df.replace({'---':np.nan})

    if to_numeric:
        # Try to convert columns to numeric types, but fail gracefully
        df = df.apply(_to_numeric)

    return df

def change_encoding(s,from_encoding,to_encoding='utf-8',errors='ignore'):
    """
    Change encoding of a string s from_encoding to_encoding.

    For example, strings in data may be encoded in latin-1 or ISO-8859-1.
    We usually want utf-8.
    """
    return bytes(s,encoding=from_encoding).decode(to_encoding,errors=errors)

def to_parquet(df,fn):
    """
    Write df to parquet file fn.

    Parquet (pyarrow) is slightly more picky about data types and layout than is pandas;
    here we fix some possible problems before calling pd.DataFrame.to_parquet.
    """
    if len(df.shape)==0: # A series?  Need a dataframe.
        df = pd.DataFrame(df)

    # Can't mix types of category labels.
    for col in df:
        if df[col].dtype == 'category':
            cats = df[col].cat.categories
            if str in [type(x) for x in cats]: # At least some categories are strings...
                df[col] = df[col].cat.rename_categories(lambda x: str(x))

    # Pyarrow can't deal with mixes of types in columns of type object. Just
    # convert them all to str.
    idxnames = df.index.names
    all = df.reset_index()
    for column in all:
        if all[column].dtype=='O':
            all[column] = all[column].astype(str).astype('str[pyarrow]').replace('nan',None)
    df = all.set_index(idxnames)

    df.to_parquet(fn)

    return df

from collections import UserDict

class RecursiveDict(UserDict):
    def __init__(self,*arg,**kw):
      super(RecursiveDict, self).__init__(*arg, **kw)

    def __getitem__(self,k):
        try:
            while True:
                k = UserDict.__getitem__(self,k)
        except KeyError:
            return k

def format_id(id,zeropadding=0):
    """Nice string format for any id, string or numeric.

    Optional zeropadding parameter takes an integer
    formats as {id:0z} where
    """
    if pd.isnull(id) or id in ['','.']: return None

    try:  # If numeric, return as string int
        return ('%d' % id).zfill(zeropadding)
    except TypeError:  # Not numeric
        return id.split('.')[0].strip().zfill(zeropadding)
    except ValueError:
        return None

def panel_ids(Waves):
    """
    Used to build panel_ids data. 
    Return RecursiveDict of household identifiers to trace households(splited) across waves.
    
    Waves: Dictionary of waves with the following structure:
        The key of the dictionary is each wave year;
        The value is a tuple in the maximum of 4 elements: (id_datafile, recent_id, old_id, function_to_transform_old_id)
        
        For example (Uganda example):
        Waves = {'2011-12':(),
        '2013-14':('GSEC1.dta','HHID','HHID_old'),
        '2015-16':('gsec1.dta','HHID','hh',lambda s: s.replace('-05-','-04-')),
        '2018-19':('GSEC1.dta','hhid','t0_hhid'),
        '2019-20':('HH/gsec1.dta','hhid','hhidold')}

        Waves include a list, example (Tanzania example), the list is used to map the ids by the function map_08_15:
            Waves = {'2008-15':('upd4_hh_a.dta',['r_hhid','round','UPHI'], map_08_15),
                    '2019-20':('HH_SEC_A.dta','sdd_hhid','y4_hhid'),
                    '2020-21':('hh_sec_a.dta','y5_hhid','y4_hhid')}
    
    Faye Fang                                                                            Sept. 2024

    """
    D = RecursiveDict()
    for t,v in Waves.items():
        if len(v):
            fn = f"../{t}/Data/{v[0]}"
            columns = v[1] if isinstance(v[1], list) else [v[1], v[2]]
            try:
                df = from_dta(fn)[columns]
            except FileNotFoundError:
                with dvc.api.open(fn,mode='rb') as dta: df = from_dta(dta)[columns]

            if isinstance(v[1], list):
                df[v[1][0]]=df[v[1][0]].apply(format_id)
                D = v[2](df,v[1], D) 
            else:                  
                # Clean-up ids
                df[v[1]] = df[v[1]].apply(format_id)
                df[v[2]] = df[v[2]].apply(format_id)

                if len(v)==4: # Remap id1
                    df[v[2]] = df[v[2]].apply(v[3])

                D.update(df[[v[1],v[2]]].dropna().values.tolist())

    return D

def conversion_table_matching_global(df, conversions, conversion_label_name, num_matches=3, cutoff = 0.6):
    """
    Returns a Dataframe containing matches and Dictionary mapping top choice
    from a conversion table's labels to item labels from a given df.

    """
    D = defaultdict(dict)
    all_matches = pd.DataFrame(columns=["Conversion Table Label"] +
                               ["Match " + str(n) for n in range(1, num_matches + 1)])
    items_unique = df['i'].str.capitalize().unique()
    for l in conversions[conversion_label_name].unique():
        k = difflib.get_close_matches(l.capitalize(), items_unique, n = num_matches, cutoff=cutoff)
        if len(k):
            D[l] = k[0]
            k = [l] + k
            all_matches.loc[len(all_matches.index)] = k + [np.nan] * (num_matches + 1 - len(k))
        else:
            D[l] = l
            all_matches.loc[len(all_matches.index)] = [l] + [np.nan] * num_matches
    return all_matches, D

def category_union(dict_list):
    """Construct union of a list of dictionaries, preserving unique *values*.

    Returns this union, as well as a list of dictionaries mapping the original
    dicts into the union.

    >>> c1={1:'a',2:'b',3:'c'}
    >>> c2={1:'b',2:'c',3:'d',4:"a'"}
    >>> c0,t1,t2 = category_union([c1,c2])
    >>> c0[t1[2]]==c1[2]
    True
    """
    cv = []
    for i in range(len(dict_list)):
        cv += list(set(dict_list[i].values()))

    cv = list(set(cv))

    c0 = dict(zip(range(len(cv)),cv))

    c0inv = {v:k for k,v in c0.items()}

    t = []
    for i in range(len(dict_list)):
        t.append({k:c0inv[v] for k,v in dict_list[i].items()})

    return c0,*tuple(t)

def category_remap(c,remaps):
    """
    Return a "remapped" dictionary.

    A dictionary remaps values in dict c into other values in c.
    """
    cinv = {v:k for k,v in c.items()}
    for k,v in remaps.items():
        c[cinv[k]] = v

    return c

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
