#!/usr/bin/env python
import sys
import numpy as np
import pandas as pd
sys.path.append('../../../_/')
from local_tools import df_from_orgfile, get_categorical_mapping, format_id, df_data_grabber, _to_numeric, to_parquet
import warnings
from collections import defaultdict

w = '2016-17'
Visits = range(1,7)

# categorical mapping
labelsd = get_categorical_mapping(tablename='harmonize_food',idxvars={'Code':('Code_9b',format_id)},**{'Label':'Preferred Label'})
unitsd = defaultdict(lambda:np.nan,get_categorical_mapping(tablename='units'))

# food expenditure
idxvars = dict(h=(['clust','nh'],lambda x: format_id(x.clust)+format_id(x.nh)),
               w=('nh',lambda x: w),
               v=('clust',format_id),
               j=('freqcd',lambda x: labelsd[format_id(x)]))

visit = {}
# Iterate over visits
for i in Visits:
    idxvars['u'] = (f"s9bq{i}c",lambda x: unitsd[_to_numeric(x)])

    myvars = {f"Expenditure":(f"s9bq{i}a",_to_numeric),
              f"Quantity":(f"s9bq{i}b",_to_numeric)}

    visit[i] = df_data_grabber('../Data/g7sec9b_small.dta',idxvars,convert_categoricals=False,**myvars)
    visit[i] = visit[i].replace(0,np.nan).dropna(how='all')
    visit[i] = visit[i].groupby(list(idxvars.keys())).sum()

x = pd.concat(visit,names=['visit']+visit[1].index.names)

####################
# Home produced
####################

Visits = range(3,8)
labelsd = get_categorical_mapping(tablename='harmonize_food',idxvars={'Code':('Code_8h',format_id)},**{'Label':'Preferred Label'})

# food quantities
idxvars = dict(h=(['clust','nh'],lambda x: format_id(x.clust)+format_id(x.nh)),
               w=('nh',lambda x: w),
               v=('clust',format_id),
               j=('foodcd',lambda x: labelsd[format_id(x)]))

visit = {}
# Iterate over visits
for i in Visits:
    myvars = {'Price':(f's8hq{i}p',_to_numeric),
              "Produced":(f"s8hq{i}q",_to_numeric)}
    idxvars['u'] = (f's8hq{i}u',lambda x: unitsd[x])

    visit[i] = df_data_grabber('../Data/g7sec8h.dta',idxvars,convert_categoricals=False,**myvars)
    visit[i] = visit[i].replace(0,np.nan).dropna(how='all')

prod = pd.concat(visit,names=['visit']+visit[3].index.names)

# Oddity with large number for missing code
na = prod.select_dtypes(exclude='object').max().max()

if na>1e99:  # Missing values?
    warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
    prod = prod.replace(na,np.nan)

prod = prod.groupby(prod.index.names).sum()

fa = x.join(prod,how='outer')

fa = fa.dropna(how='all')

if __name__=='__main__':
    to_parquet(fa,'food_acquired.parquet')
