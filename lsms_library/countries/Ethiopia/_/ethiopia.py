import numpy as np
import pandas as pd
from collections import defaultdict
from cfe.df_utils import use_indices
import warnings
import json
from lsms_library.local_tools import get_dataframe, DVCFS

# Wave list used by harmonized_food_labels() and other helpers.
# NB: panel_ids no longer uses this dict -- see panel_ids.py for the
# bespoke chain that goes W3->W2->W1 via household_id2 identity mapping.
Waves = {'2011-12':(),
         '2013-14':('sect_cover_hh_w2.dta','household_id2','household_id'),
         '2015-16':('sect_cover_hh_w3.dta','household_id2','household_id'),
         '2018-19':(),  # Entirely new sample drawn
         '2021-22':(),
         }


def harmonized_unit_labels(fn='../../_/unitlabels.csv',key='Code',value='Preferred Label'):
    unitlabels = pd.read_csv(fn)
    unitlabels.columns = [s.strip() for s in unitlabels.columns]
    unitlabels = unitlabels[[key,value]].dropna()
    unitlabels = unitlabels.set_index(key)

    return unitlabels.squeeze().str.strip().to_dict()


def harmonized_food_labels(fn='../../_/food_items.org',key=list(Waves.keys()),value='Preferred Label'):
    # Harmonized food labels
    food_items = pd.read_csv(fn,delimiter='|',skipinitialspace=True,converters={1:lambda s: s.strip(),2:lambda s: s.strip()})
    food_items.columns = [s.strip() for s in food_items.columns]
    food_items = food_items.loc[:,food_items.count()>0]
    food_items = food_items.drop(columns = ['FTC Code', 'FDC ID']).apply(lambda x: x.str.strip())

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


def _household_identification_from_file(fn, HHID='HHID', urban='urban', region='region',
                                         urban_converter=None, region_converter=None,
                                         convert_categoricals=True, **kwargs):
    """Inline replacement for lsms.tools.get_household_identification_particulars."""
    df = get_dataframe(fn, convert_categoricals=convert_categoricals)
    df = df.rename(columns={HHID: 'HHID', urban: 'urban', region: 'region'})
    if kwargs:
        df = df.rename(columns={v: k for k, v in kwargs.items()})
    if urban_converter is not None:
        df['urban'] = df['urban'].apply(urban_converter)
    if region_converter is not None:
        df['region'] = df['region'].apply(region_converter)
    df['region'] = df['region'].apply(lambda s: str(s).lower())
    df['urban'] = df['urban'].apply(lambda x: x == 1)
    try:
        if df['HHID'].iloc[0].split('.')[-1] == '0':
            df['HHID'] = df['HHID'].apply(lambda x: '%d' % int(float(x)))
    except (ValueError, AttributeError):
        pass
    columns = ['urban', 'region'] + list(kwargs.keys())
    df = df.loc[:, ['HHID'] + columns]
    df = df.set_index('HHID')
    return df.loc[:, columns]


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

    food_items = harmonized_food_labels(fn='../../_/food_items.org')

    df = get_dataframe(fn, convert_categoricals=True)

    # Unit labels from Stata value labels (need a stream, not a DataFrame)
    with DVCFS.open(fn) as dta:
        sr = pd.io.stata.StataReader(dta)
        try:
            unitlabels = sr.value_labels()[units]
        except KeyError:
            foo = sr.value_labels()
            key = [k for k,v in foo.items() if 'Kilogram' in [u[:8] for l,u in v.items()]][0]
            unitlabels = sr.value_labels()[key]

    df = df.rename(columns={HHID: 'HHID', item: 'itmcd', farmgate: 'farmgate',
                             market: 'market', units: 'units'})
    if food_items is not None:
        df = df.replace({'itmcd': food_items})
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

def food_acquired(fn,myvars):
    """Reshape Ethiopia's food_acquired into the canonical (t, i, j, u, s) form.

    Phase 3 of GH #169 / DESIGN_food_acquired_canonical_2026-05-05.org.
    Per the design row for Ethiopia:
        "derive purchased + produced (= total - purchased)"

    Ethiopia's source records, per (household, item):
      - quantity / units              : TOTAL acquired in `units`
      - value_purchased               : monetary value of the purchased subset
      - quantity_purchased / units_purchased : amount and unit of the
        purchased subset

    The wave script supplies `myvars` mapping these to the source columns;
    `t` is appended from the wave folder name (parent of cwd).

    Unit handling (decision documented 2026-05-06):
      In wave 1 (2011-12) only 64/19,231 (0.3%) of rows where both quantity
      and quantity_purchased are positive have `units != units_purchased`.
      The asymmetry is rare enough that we treat the row as "single-unit":
      we use `units` as the canonical `u` axis and silently drop
      `units_purchased`.  Produced is then derived in the same unit:
          Produced = (quantity - quantity_purchased).clip(lower=0)
      For the rare unit-mismatch rows the subtraction is approximate, but
      preserving the framework helper's one-unit-per-row contract is more
      important than the ~0.3% accuracy loss.  No kg conversion is done at
      this layer — that lives downstream in the framework's
      food_quantities_from_acquired path.
    """
    from lsms_library.transformations import food_acquired_to_canonical

    df = get_dataframe(fn,convert_categoricals=True)

    df = df.loc[:,list(myvars.values())].rename(columns={v:k for k,v in myvars.items()})

    # Correct unit labels (title-case + a few historical typos).  Preserved
    # from the legacy implementation -- keeps `u` values consistent with the
    # countries/Ethiopia/_/conversion_to_kgs.json keys that downstream code
    # still references for kg conversion.
    df['units_purchased'] = df['units_purchased'].str.title()
    df['units'] = df['units'].str.title()
    rep = {r'\s+':' ',
           'Meduim': 'Medium',
           'Kubaya ':'Kubaya/Cup ',
           'Milliliter' : 'Mili Liter'}
    df = df.replace(rep, regex=True)

    # Coerce HHID to canonical integer-string form when it lands as float
    # (some Ethiopia waves' household_id columns import as float64).
    if df['HHID'].dtype == float:
        df['HHID'] = df['HHID'].astype(str).str.split('.').str[0].replace('nan', pd.NA)

    # Compute Produced = total - purchased, clipped at 0.  In ~0.3% of
    # populated rows units != units_purchased; we accept the approximate
    # subtraction and use `units` as the canonical unit u.
    quantity = pd.to_numeric(df['quantity'], errors='coerce')
    quantity_purchased = pd.to_numeric(df['quantity_purchased'], errors='coerce')
    df['Quantity'] = quantity
    df['Produced'] = (quantity.fillna(0) - quantity_purchased.fillna(0)).clip(lower=0)
    df['Expenditure'] = pd.to_numeric(df['value_purchased'], errors='coerce')

    # Derive `t` from the wave folder name.  Wave scripts cd into
    # countries/Ethiopia/<wave>/_/ before importing this helper, so the
    # parent directory of cwd is the wave label.  Mirrors the established
    # script-path pattern in Tanzania / Malawi.
    import os
    wave = os.path.basename(os.path.dirname(os.getcwd()))
    df['t'] = wave

    # Build the wide-form frame the framework helper expects:
    # index (t, i, j, u), columns (Quantity, Expenditure, Produced).
    df = (df.rename(columns={'HHID': 'i', 'item': 'j', 'units': 'u'})
            .set_index(['t', 'i', 'j', 'u'])
            [['Quantity', 'Expenditure', 'Produced']]
            .dropna(how='all'))

    # Ditch the now-unused `units_purchased` (already dropped via the
    # column projection above).  Helper produces (t, i, j, u, s) with
    # s in {'purchased', 'produced'}.
    out = food_acquired_to_canonical(df, drop_columns=())
    return out

def food_expenditures(fn='',purchased=None,away=None,produced=None,given=None,item='item',HHID='HHID'):
    food_items = harmonized_food_labels(fn='../../_/food_items.org')

    expenditures = _sum_expenditures_from_file(fn, purchased, away, produced, given,
                                                itmcd=item, HHID=HHID, itemlabels=food_items)

    expenditures.index.name = 'j'
    expenditures.columns.name = 'i'

    expenditures = expenditures[expenditures.columns.intersection(food_items.values())]

    return expenditures


def nonfood_expenditures(fn='',purchased=None,away=None,produced=None,given=None,item='item',HHID='HHID'):
    nonfood_items = harmonized_food_labels(fn='../../_/nonfood_items.org',key='Code',value='Preferred Label')

    expenditures = _sum_expenditures_from_file(fn, purchased, away, produced, given,
                                                itmcd=item, HHID=HHID, itemlabels=nonfood_items)

    expenditures.index.name = 'j'
    expenditures.columns.name = 'i'
    expenditures = expenditures[expenditures.columns.intersection(nonfood_items.values())]

    return expenditures

def food_quantities(fn='',item='item',HHID='HHID',
                    purchased=None,away=None,produced=None,given=None,units=None):
    food_items = harmonized_food_labels(fn='../../_/food_items.org')

    quantities = _sum_expenditures_from_file(fn, purchased, away, produced, given,
                                              itmcd=item, HHID=HHID, units=units,
                                              itemlabels=food_items)

    quantities.index.names = ['j','u']
    quantities.columns.name = 'i'

    return quantities

def age_sex_composition(fn,sex='sex',sex_converter=None,age='age',months_spent='months_spent',HHID='HHID',months_converter=None, convert_categoricals=True,Age_ints=None,fn_type='stata'):

    if Age_ints is None:
        # Match Uganda FCT categories
        Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))

    df = _household_roster_from_file(fn, sex=sex, age=age, HHID=HHID,
                                      months_spent=months_spent,
                                      sex_converter=sex_converter,
                                      months_converter=months_converter,
                                      Age_ints=Age_ints,
                                      convert_categoricals=convert_categoricals)
    df.index.name = 'j'
    df.columns.name = 'k'

    return df


def other_features(fn,urban=None,region=None,HHID='HHID',urban_converter=None):

    df = _household_identification_from_file(fn, HHID=HHID, urban=urban, region=region,
                                              urban_converter=urban_converter)
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
            x['j'] = x['j'].astype(str).apply(lambda s: s.split('.')[0]).replace('nan',pd.NA)
        elif x['j'].dtype==int:
            x['j'] = x['j'].astype(str)
        elif x['j'].dtype==str:
            x['j'] = x['j'].replace('',pd.NA)

        x = x.set_index(idx)

        return x

    id = get_dataframe(fn)

    id = id[[id0,id1]]

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

    d.pop(np.nan, None)  # Get rid of nan key, if any
    d.pop(pd.NA, None)

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

def panel_attrition(df,return_ids=False,waves=None):
    """
    Produce an upper-triangular) matrix showing the number of households (j) that
    transition between rounds (t) of df.
    """
    idxs = df.reset_index().groupby('t')['j'].apply(list).to_dict()

    if waves is None:
        waves = list(Waves.keys())

    foo = pd.DataFrame(index=waves,columns=waves)
    IDs = {}
    for m,s in enumerate(waves):
        for t in waves[m:]:
            IDs[(s,t)] = set(idxs[s]).intersection(idxs[t])
            foo.loc[s,t] = len(IDs[(s,t)])

    if return_ids:
        return foo,IDs
    else:
        return foo
