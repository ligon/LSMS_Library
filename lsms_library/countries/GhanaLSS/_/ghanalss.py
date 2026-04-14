import pandas as pd
from ligonlibrary.dataframes import from_dta
import numpy as np
import dvc.api
from collections import defaultdict
from lsms_library.local_tools import get_dataframe

# Formatting  Functions for Ghana 2016-17
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict

def i(value):
    '''
    Formatting household id
    '''
    if type(value) == pd.Series:
        return tools.format_id(value.iloc[0])+'/'+tools.format_id(value.iloc[1],zeropadding=2)
    else:
        return tools.format_id(value)


def Sex(value):
    '''
    Formatting sex veriable
    '''
    if pd.isna(value):
        return pd.NA
    else:
        return str(value).upper()[0]

def Age(value):
    '''
    Formatting age variable
    '''
    if pd.isna(value):
        return np.nan
    else:
        return int(value)
    
def Birthplace(value):
    '''
    Formatting birthplace variable
    '''
    if pd.isna(value):
        return pd.NA
    else:
        return value.title() if isinstance(value,str) else pd.NA

def Relationship(value):
    '''
    Formatting relationship variable
    '''
    if pd.isna(value):
        return pd.NA
    else:
        return value.title()



# Data to link household ids across waves
# PANELC.DAT in 1988-89 maps HID1 (GLSS1) -> HID2 (GLSS2) at person level;
# panel_ids() deduplicates to household level.  714 panel households.
#
# Waves 1991-92 through 2016-17: cluster numbering changes between rounds
# (3xxx, 4xxx, 5xxx, 6xxxx, 7xxxx) and no explicit prior-wave HH ID column
# exists in the available data files, so cross-wave linkage cannot be
# established.  The 2005-06 and 2012-13 hints ('rhhno') referred to
# within-wave replacement households, not cross-wave panel linkage.
Waves = {'1987-88':(),
         '1988-89':('PANELC.DAT', 'HID2', 'HID1'),
         '1991-92':(),  # No linkage file; cluster scheme changed (2xxxx -> 3xxx)
         '1998-99':(),  # No linkage file; cluster scheme changed (3xxx -> 4xxx)
         '2005-06':(),  # No linkage file; cluster scheme changed (4xxx -> 5xxx)
         '2012-13':(),  # reint marks 1476 panel HHs but no prior-wave ID column
         '2016-17':()   # No linkage fields
         }

def yearly_expenditure(row, cost = 'CFOODB', freq = 'TFOODB', freq_unit = 'UTFOODB', months = 'MFOODBLY'):
    row = row.replace({'.':0})
    val = float(row[cost]) * float(row[freq]) #cost each time * freq per time unit
    if row[freq_unit] == '3': #if bought daily
        val *= 30 * float(row[months]) # * days in a month * num of months item bought
    elif row[freq_unit] == '4': #if bought weekly
        val *= 4 * float(row[months]) # * weeks in a month * num of months item bought
    elif row[freq_unit] == '5': #if bought monthly
        val *= float(row[months]) # * num of months item bought
    elif row[freq_unit] == '6': #if bought quarterly
        val *= 4 # * quarters in a year 
    elif row[freq_unit] == '7': #if bought semiannually
        val *= 2 # * half years in a year
    elif row[freq_unit] == '8': #if bought yearly
        val *= 1
    else:
        val = 0
    return val

def load_large_dta(fn, convert_categoricals = False):
    import sys

    reader = pd.read_stata(fn, iterator=True, convert_categoricals = convert_categoricals)
    df = pd.DataFrame()

    try:
        chunk = reader.get_chunk(100*1000)
        while len(chunk) > 0:
            df = pd.concat([df, chunk], ignore_index=True)
            chunk = reader.get_chunk(100*1000)
            sys.stdout.flush()
    except (StopIteration, KeyboardInterrupt):
        pass
    print('\nloaded {} rows'.format(len(df)))
    return df

def split_by_visit(df, first_visit, last_visit, t, ind = ('j','t','i'), unit_col = None, aggregate_amount = False):
    ind = list(ind)
    df = df.set_index([ind[0]] + ind[2:])
    df_by_visit = []
    for i in range(first_visit, last_visit+1):
        tem = df[df.columns[df.columns.str.contains(str(i))]]
        temp = tem.dropna(how='all').copy()
        temp['t']= t + ', ' + str(i)
        temp = temp.reset_index().set_index(ind)
        temp.columns = ['_'.join(c.split('_')[:-1]) for c in temp.columns]
        #temp = temp.set_index('t', append = True)
        df_by_visit.append(temp)
    result = pd.concat(df_by_visit)
    result = result.rename(columns={unit_col: 'u'})
    if aggregate_amount:
        try:
            return result.groupby(ind + ['u']).agg("sum")
        except KeyError:
            return result.groupby(ind).agg("sum")
    else:
        try:
            return result.reset_index().set_index(ind + ['u'])
        except KeyError:
            return result.reset_index().set_index(ind)

def harmonized_food_labels2(fn='../../_/food_items.org'):
    # Harmonized food labels
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:int,2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items[['Code','Preferred Label']].dropna()
    food_items = food_items.set_index('Code')

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

        food_items = food_items[key + [value]].replace('---', pd.NA).dropna(how = 'all')
    else:
        food_items = food_items[[key] + [value]].replace('---', pd.NA).dropna(how = 'all')
        
    food_items = food_items.set_index(key)

    return food_items.squeeze().str.strip().to_dict()

def _sum_expenditures_from_file(fn, purchased, away, produced, given, itmcd, HHID,
                                 units=None, itemlabels=None, convert_categoricals=False):
    """Inline replacement for lsms.tools.get_food_expenditures (file-opening path)."""
    df = get_dataframe(fn, convert_categoricals=convert_categoricals)
    sources = {'purchased': purchased, 'away': away, 'produced': produced, 'given': given}
    varnames = {v: k for k, v in sources.items() if v is not None}
    varnames[HHID] = 'HHID'
    varnames[itmcd] = 'itmcd'
    if units is not None:
        varnames[units] = 'units'
    df = df.rename(columns=varnames)
    value_cols = [k for k, v in sources.items() if v is not None]
    for col in value_cols:
        df[col] = df[col].astype(np.float64)
    try:
        df['itmcd'] = df['itmcd'].astype(float)
        df = df.loc[~np.isnan(df['itmcd'])]
        df['itmcd'] = df['itmcd'].astype(int)
    except (ValueError, TypeError):
        pass
    if itemlabels is not None:
        df = df.replace({'itmcd': itemlabels})
    valvars = ['HHID', 'itmcd'] + value_cols
    if units is not None:
        df['units'] = df['units'].fillna(0).astype(int)
        g = df.loc[:, valvars + ['units']].groupby(['HHID', 'units', 'itmcd'])
        x = g.sum().sum(axis=1).unstack('itmcd')
    else:
        g = df.loc[:, valvars].groupby(['HHID', 'itmcd'])
        x = g.sum().sum(axis=1).unstack('itmcd')
    x = x.fillna(0)
    if itemlabels is not None:
        x = x.loc[:, x.columns.isin(itemlabels.values())]
    return x


def _household_roster_from_file(fn, sex='sex', age='age', HHID='HHID',
                                  months_spent='months_spent', sex_converter=None,
                                  months_converter=None, Age_ints=None,
                                  convert_categoricals=True):
    """Inline replacement for lsms.tools.get_household_roster (file-opening path)."""
    df = get_dataframe(fn, convert_categoricals=convert_categoricals)
    cols = [c for c in [HHID, sex, age, months_spent] if c in df.columns]
    df = df.loc[:, cols].rename(columns={HHID: 'HHID', sex: 'sex', age: 'age',
                                          months_spent: 'months_spent'})
    if months_converter is not None:
        df['months_spent'] = df['months_spent'].apply(months_converter)
    if sex_converter is not None:
        df['sex'] = df['sex'].apply(sex_converter)
    df = df.dropna(how='any')
    df['sex'] = df['sex'].apply(lambda s: str(s[0]).lower())
    df['boys']  = (df['sex'] == 'm') & (df['age'] < 18)
    df['girls'] = (df['sex'] == 'f') & (df['age'] < 18)
    df['men']   = (df['sex'] == 'm') & (df['age'] >= 18)
    df['women'] = (df['sex'] == 'f') & (df['age'] >= 18)
    if Age_ints is None:
        Age_ints = ((0,1),(1,5),(5,10),(10,15),(15,20),(20,30),(30,50),(50,60),(60,100))
    valvars = list({'HHID','girls','boys','men','women'}.intersection(df.columns))
    for lo, hi in Age_ints:
        s, e = lo, hi - 1
        df['Males %02d-%02d' % (s, e)]   = (df['sex'] == 'm') & (df['age'] >= lo) & (df['age'] < hi)
        df['Females %02d-%02d' % (s, e)] = (df['sex'] == 'f') & (df['age'] >= lo) & (df['age'] < hi)
        valvars += ['Males %02d-%02d' % (s, e), 'Females %02d-%02d' % (s, e)]
    try:
        if df['HHID'].iloc[0].split('.')[-1] == '0':
            df['HHID'] = df['HHID'].apply(lambda x: '%d' % int(float(x)))
    except (ValueError, AttributeError):
        pass
    if 'months_spent' in df.columns and df['months_spent'].count() > 0:
        g = df.loc[df['months_spent'] > 0, valvars].groupby('HHID')
    else:
        g = df[valvars].groupby('HHID')
    return g.sum()


def prices_and_units(fn='',units='units',item='item',HHID='HHID',market='market',farmgate='farmgate'):

    df = get_dataframe(fn, convert_categoricals=True)

    # Unit labels from Stata value labels
    with dvc.api.open(fn,mode='rb') as dta:
        sr = pd.io.stata.StataReader(dta)
        try:
            unitlabels = sr.value_labels()[units]
        except KeyError:
            foo = sr.value_labels()
            key = [k for k,v in foo.items() if 'Kilogram' in [u[:8] for l,u in v.items()]][0]
            unitlabels = sr.value_labels()[key]

    df = df.rename(columns={HHID: 'HHID', item: 'itmcd', farmgate: 'farmgate',
                             market: 'market', units: 'units'})
    try:
        df['itmcd'] = df['itmcd'].astype(float)
        df = df.loc[~np.isnan(df['itmcd'])]
        df['itmcd'] = df['itmcd'].astype(int)
    except (ValueError, TypeError):
        pass
    prices = df.loc[:, ['HHID', 'itmcd', 'farmgate', 'market', 'units']].set_index(['HHID', 'itmcd'])
    prices = prices.replace({'units': unitlabels})
    prices.units = prices.units.astype(str)

    pd.Series(unitlabels).to_csv('unitlabels.csv')

    return prices

def food_expenditures(fn='',purchased=None,away=None,produced=None,given=None,item='item',HHID='HHID'):

    expenditures = _sum_expenditures_from_file(fn, purchased, away, produced, given,
                                                itmcd=item, HHID=HHID)
    return expenditures

def food_quantities(fn='',item='item',HHID='HHID',
                    purchased=None,away=None,produced=None,given=None,units=None):

    quantities = _sum_expenditures_from_file(fn, purchased, away, produced, given,
                                              itmcd=item, HHID=HHID, units=units,
                                              convert_categoricals=True)
    return quantities

def age_sex_composition(fn,sex='sex',sex_converter=None,
                        age='age',months_spent='months_spent',HHID='HHID',months_converter=None, convert_categoricals=True,Age_ints=None,fn_type='stata'):

    df = _household_roster_from_file(fn, sex=sex, age=age, HHID=HHID,
                                      months_spent=months_spent,
                                      sex_converter=sex_converter,
                                      months_converter=months_converter,
                                      convert_categoricals=convert_categoricals)
    df.index.name = 'j'
    df.columns.name = 'k'

    return df

def household_characteristics(fn='',sex='',age='',HHID='HHID',months_spent='months_spent', fn_type = 'stata'):

    if type(sex) in [list,tuple]:
        sex,sex_converter = sex
    else:
        sex_converter = None

    df = _household_roster_from_file(fn, sex=sex, age=age, HHID=HHID,
                                      months_spent=months_spent,
                                      sex_converter=sex_converter)
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
            x['j'] = x['j'].astype(str).apply(lambda s: s.split('.')[0]).replace('nan',pd.NA)
        elif x['j'].dtype==int:
            x['j'] = x['j'].astype(str)
        elif x['j'].dtype==str:
            x['j'] = x['j'].replace('',pd.NA)

        x = x.set_index(idx)

        return x

    try:
        with open(fn,mode='rb') as dta:
            id = from_dta(dta)
    except IOError:
        with dvc.api.open(fn,mode='rb') as dta:
            id = from_dta(dta)
    #generalize to ids being a list of columns needing to be joined        
    if type(id0) == list:
        id['id0'] = concate_id(id, id0[0], id0[1],True, 2)
        id0 = 'id0'
    if type(id1) == list:
        id['id1'] = concate_id(id, id1[0], id1[1],True, 2)
        id1 = 'id1'

    id = id[[id0,id1]]
    id[id1] = id[id1].replace('', pd.NA).fillna(id[id0])

    for column in id:
        if id[column].dtype==float:
            id[column] = id[column].astype(str).apply(lambda s: s.split('.')[0]).replace('nan',pd.NA)
        elif id[column].dtype==int:
            id[column] = id[column].astype(str).replace('nan',pd.NA)
        elif id[column].dtype==object:
            id[column] = id[column].replace('nan',pd.NA)
            id[column] = id[column].replace('',pd.NA)

    ids = dict(id[[id0,id1]].values.tolist())

    if transform_id1 is not None:
        ids = {k:transform_id1(v) for k,v in ids.items()}

    d = defaultdict(list)

    for k,v in ids.items():
        d[v] += [k]

    try:
        d.pop(pd.NA)  # Get rid of nan key, if any
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

def concate_id(df, parta, partb, leading_zero = False, digit = None):
    df = df.replace('', pd.NA)
    df.loc[df[parta].isna(), partb] = pd.NA
    df.loc[df[partb].isna(), parta] = pd.NA
    if leading_zero and digit != None:
        df['newid'] = df[parta].astype('Int64').astype(str) + df[partb].astype('Int64').astype(str).str.zfill(digit)
    else:
        df['newid'] = df[parta].astype('Int64').astype(str) + df[partb].astype('Int64').astype(str)
    na_id = df.loc[df[parta].isna(), 'newid'].iloc[0]
    df['newid'] = df['newid'].replace(na_id, pd.NA)
    return df['newid']
