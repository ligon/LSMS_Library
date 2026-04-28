#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_dataframe
import sys
import pandas as pd
import json
import numpy as np


def _sum_expenditures(df, purchased, away, produced, given, itmcd, HHID, itemlabels):
    """Inline replacement for lsms.tools.get_food_expenditures(fn_type=None).

    df is already a DataFrame; groups by HHID+itmcd, sums across sources,
    replaces itmcd codes with itemlabels, and filters to labelled items.
    """
    sources = {'purchased': purchased, 'away': away, 'produced': produced, 'given': given}
    varnames = {v: k for k, v in sources.items() if v is not None}
    varnames[HHID] = 'HHID'
    varnames[itmcd] = 'itmcd'
    df = df.rename(columns=varnames)
    value_cols = [k for k, v in sources.items() if v is not None]
    for col in value_cols:
        df[col] = df[col].astype(np.float64)
    valvars = ['HHID', 'itmcd'] + value_cols
    try:
        df['itmcd'] = df['itmcd'].astype(float)
        df = df.loc[~np.isnan(df['itmcd'])]
        df['itmcd'] = df['itmcd'].astype(int)
    except (ValueError, TypeError):
        pass
    if itemlabels is not None:
        df = df.replace({'itmcd': itemlabels})
    g = df.loc[:, valvars].groupby(['HHID', 'itmcd'])
    x = g.sum().sum(axis=1).unstack('itmcd')
    x = x.fillna(0)
    if itemlabels is not None:
        x = x.loc[:, x.columns.isin(itemlabels.values())]
    return x


# There are two waves of data in each file, so we modify food expenditures to allow filtering.

def food_expenditures(fn='',purchased=None,away=None,produced=None,given=None,item='item',HHID='HHID',filter=None):

    with open('food_items.json') as f:
        food_items = {int(float(k)):v for k,v in json.load(f)['Label'].items()}

    # expenditures
    df = get_dataframe(fn,convert_categoricals=False,preserve_dtypes=False)

    df['HHID'] = df.grappe*1000+df.menage

    if filter is not None:
        df = df.query(filter)

    expenditures = _sum_expenditures(df, purchased, away, produced, given, itmcd=item, HHID=HHID, itemlabels=food_items)

    expenditures.columns.name = 'i'
    expenditures.index.name = 'j'
    expenditures = expenditures.replace(0, np.nan)

    return expenditures

# Wave 1
t = '2018'

myvars = dict(fn='../Data/Menage/s07b_me_CIV2018.dta',item='s07bq01',
              purchased='s07bq08',filter='vague==1')

x = food_expenditures(**myvars)

x['t'] = t
x['m'] = "Cote d'Ivoire"

x = x.reset_index().set_index(['j','t','m'])

X = [x.copy()]

# Wave 2

myvars = dict(fn='../Data/Menage/s07b_me_CIV2018.dta',item='s07bq01',
              purchased='s07bq08',filter='vague==2')

x = food_expenditures(**myvars)

x['t'] = '2019'

x['m'] = "Cote d'Ivoire"

x = x.reset_index().set_index(['j','t','m'])

X.append(x)

df = pd.concat(X)

to_parquet(df, 'food_expenditures.parquet')

