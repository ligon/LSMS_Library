#!/usr/bin/env python3

"""
A collection of mappings to transform dataframes.
"""
import pandas as pd
import numpy as np
from pandas import concat, get_dummies, MultiIndex
from cfe.df_utils import use_indices


def age_intervals(age,age_cuts=(0,4,9,14,19,31,51)):
    """
    Take as input a Series (e.g., a row from a dataframe), and use variables =Age= and =Sex=
    to create a set of coarser categories.
    """
    age_cuts = [-np.inf]+list(age_cuts)+[np.inf]
    return pd.cut(age,age_cuts,duplicates='drop', right = False)

def dummies(df,cols,suffix=False):
    """From a dataframe df, construct an array of indicator (dummy) variables,
    with a column for every unique row df[cols]. Note that the list cols can
    include names of levels of multiindices.

    The optional argument =suffix=, if provided as a string, will append suffix
    to column names of dummy variables. If suffix=True, then the string '_d'
    will be appended.
    """
    idxcols = list(set(df.index.names).intersection(cols))
    colcols = list(set(cols).difference(idxcols))

    v = concat([use_indices(df,idxcols),df[colcols]],axis=1)

    usecols = []
    for s in idxcols+colcols:
        usecols.append(v[s].squeeze())

    tuples = pd.Series(list(zip(*usecols)),index=v.index)

    v = get_dummies(tuples).astype(int)

    if suffix==True:
        suffix = '_d'

    if suffix!=False and len(suffix)>0:
        columns = [tuple([str(c)+suffix for c in t]) for t in v.columns]
    else:
        columns = v.columns

    v.columns = MultiIndex.from_tuples(columns,names=idxcols+colcols)

    return v

def format_interval(interval):
    if interval.right == np.inf:
        return f"{int(interval.left)}+"
    elif interval.left == -np.inf:
        return f'00-03'
    else:
        return f"{int(interval.left):02d}-{int(interval.right-1):02d}"

def roster_to_characteristics(df, age_cuts=(0,4,9,14,19,31,51), drop = 'pid', final_index = ['t','v','i']):
    roster_df = df.copy()
    roster_df.columns = roster_df.columns.str.lower()
    roster_df['age_interval'] = age_intervals(roster_df['age'], age_cuts)
    roster_df['sex_age'] = roster_df.apply(
        lambda x: f"{x['sex']} {format_interval(x['age_interval'])}" if not pd.isna(x['age_interval']) else f"{x['sex']} NA",
        axis=1
    )
    roster_df = dummies(roster_df,['sex_age'])
    roster_df.index = roster_df.index.droplevel(drop)
    result = roster_df.groupby(level=final_index).sum()
    result['log HSize'] = np.log(result.sum(axis=1))
    result.columns = result.columns.get_level_values(0)
    return result

# def conversion_to_kgs(df, price = ['Expenditure'], quantity = 'Quantity', index=['t','m','i']):
#     v = df.copy()
#     v = v.replace(0, np.nan)
#     v['Kgs'] = np.where(v.index.get_level_values('u').str.lower() == 'kg', v[quantity], np.nan)
#     pkg = v[price].divide(v['Kgs'], axis=0)
#     pkg = pkg.groupby(index).median().median(axis=1)
#     po = v[price].groupby(index + ['u']).median().median(axis=1)
#     kgper = (po / pkg).dropna()
#     kgper = kgper.groupby('u').median()
#     #convert to dict
#     kgper = kgper.to_dict()
#     return kgper

def conversion_to_kgs(df, price = ['Expenditure'], quantity = 'Quantity', index=['t','m','i'], unit_col = 'u'):
    v = df.copy()
    v = v.replace(0, np.nan)
    unit_conversion = {
        'kg': 1,
        'kilogram': 1,
        'gram': 1 / 1000,
        'g': 1 / 1000,
        'pound': 0.453592,
        'lbs': 0.453592,
        'kilogramme': 1,
        'gramm': 1 / 1000
    }
    #convert the value type in index level 'u' to be string
    v.reset_index(unit_col, inplace=True)
    if unit_col != 'u':
        v.rename(columns={unit_col: 'u'}, inplace=True)
    v['u'] = v['u'].astype(str)
    v['Kgs'] = v.apply(lambda row: row[quantity] * unit_conversion.get(row['u'].lower(), np.nan), axis=1)
    v.set_index('u', append=True, inplace=True)
    pkg = v[price].divide(v['Kgs'], axis=0)
    pkg = pkg.groupby(index).median().median(axis=1)
    po = v[price].groupby(index + ['u']).median().median(axis=1)
    kgper = (po / pkg).dropna()
    kgper = kgper.groupby('u').median()
    #convert to dict
    kgper = kgper.to_dict()
    return kgper
