#!/usr/bin/env python
import pandas as pd
import numpy as np
from lsms_library.local_tools import df_from_orgfile, get_categorical_mapping, format_id, df_data_grabber, _to_numeric, to_parquet
import warnings
from collections import defaultdict

w = '2016-17'
visits = range(1,7)

# categorical mapping
labelsd = get_categorical_mapping(tablename='harmonize_food',idxvars={'Code':('Code_9b',format_id)},**{'Label':'Preferred Label'})
unitsd = defaultdict(lambda:np.nan,get_categorical_mapping(tablename='units'))

# food expenditure
idxvars = dict(i=(['clust','nh'],lambda x: format_id(x.clust)+'/'+format_id(x.nh,zeropadding=2)),
               w=('nh',lambda x: w),
               v=('clust',format_id),
               j=('freqcd',lambda x: labelsd[format_id(x)]))

myvars = dict()
# Iterate over visits
for i in visits:
    myvars[f'u_{i}'] = (f"s9bq{i}c",lambda x: unitsd[_to_numeric(x)])
    myvars[f"Expenditure_{i}"]= (f"s9bq{i}a",_to_numeric)
    myvars[f"Quantity_{i}"]=(f"s9bq{i}b",_to_numeric)

x = df_data_grabber('../Data/g7sec9b_small.dta',idxvars,convert_categoricals=False,**myvars)

dfs = {}
for i in visits:
    columns = [f"Expenditure_{i}",f"Quantity_{i}",f"u_{i}"]
    dfs[i] = x.loc[:,columns].copy()
    dfs[i].columns = ['Expenditure','Quantity','u']
    dfs[i]['visit'] = i
    dfs[i] = dfs[i].reset_index().replace({r'':np.nan, 0 : np.nan})
fe = pd.concat(dfs.values(),ignore_index=True)
fe = fe[fe['j'] != '']
fe= fe.groupby(['w', 'v', 'i', 'j', 'visit', 'u']).sum() # Deal with some cases with multiple records for purchases


####################
# Home produced
####################

Visits = range(3,8)
labelsd = get_categorical_mapping(tablename='harmonize_food',idxvars={'Code':('Code_8h',format_id)},**{'Label':'Preferred Label'})

# food quantities
idxvars = dict(i=(['clust','nh'],lambda x: format_id(x.clust)+'/'+format_id(x.nh,zeropadding=2)),
               w=('nh',lambda x: w),
               v=('clust',format_id),
               j=('foodcd',lambda x: labelsd[format_id(x)]))

myvars = {}
# Iterate over visits
for i in Visits:
    myvars[f'Price_{i}'] = (f's8hq{i}p',_to_numeric)
    myvars[f"Produced_{i}"]=(f"s8hq{i}q",_to_numeric)
    myvars[f'u_{i}'] = (f's8hq{i}u',lambda x: unitsd[x])

y = df_data_grabber('../Data/g7sec8h.dta',idxvars,convert_categoricals=False,**myvars)

dfs = {}
for i in Visits:
    columns = [f"Price_{i}",f"Produced_{i}",f"u_{i}"]
    dfs[i] = y.loc[:,columns].copy()
    dfs[i].columns = ['Price','Produced','u']
    dfs[i]['visit'] = i
    dfs[i] = dfs[i].reset_index().replace({r'':np.nan, 0 : np.nan})

hp = pd.concat(dfs.values(),ignore_index=True)
hp = hp[hp['j'] != '']
hp= hp.groupby(['w', 'v', 'i', 'j', 'visit', 'u']).sum() # Deal with some cases with multiple records for purchases

df = hp.join(fe,how='outer')

# Oddity with large number for missing code
na = df.select_dtypes(exclude='object').max().max()

if na>1e99:  # Missing values?
    warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
    df = df.replace(na,np.nan)

df = df.dropna(how='all')

if __name__=='__main__':
    to_parquet(df,'food_acquired.parquet')
