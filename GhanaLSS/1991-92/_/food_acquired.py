#!/usr/bin/env python
import sys
import numpy as np
import pandas as pd
sys.path.append('../../_')
from ghana import split_by_visit
sys.path.append('../../../_/')
from local_tools import df_from_orgfile, get_categorical_mapping, format_id, df_data_grabber, _to_numeric, to_parquet
import warnings

w = '1991-92'

# categorical mapping
labelsd = get_categorical_mapping(tablename='harmonize_food',idxvars={'Code':('Code_9b',format_id)},**{'Label':'Preferred Label'})

# food expenditure
idxvars = dict(h=(['clust','nh'],lambda x: format_id(x.clust)+format_id(x.nh)),
               w=('nh',lambda x: w),
               v=('clust',format_id),
               j=('fdexpcd',lambda x: labelsd[format_id(x)]))

# Keep visits separate
myvars = {f"Purchased_v{i}":(f"s9bq{i}",_to_numeric) for i in range(2,11)}


x = df_data_grabber('../Data/S9B.DTA',idxvars,**myvars)

x = x.groupby(['h','w','v','j']).sum() # Deal with some cases with multiple records for purchases
x = x.replace(0,np.nan)

x = pd.wide_to_long(x.reset_index(),['Purchased'],['h','w','v','j'],'visit',sep='_v')

# Add unit index--these will all be "Value" for purchases
x['u'] = 'Value'
x = x.reset_index().set_index(['h','w','v','j','u','visit'])

####################
# Home produced
####################

labelsd = get_categorical_mapping(tablename='harmonize_food',idxvars={'Code':('Code_8h',format_id)},**{'Label':'Preferred Label'})
unitsd = get_categorical_mapping(tablename='units')

# food quantities
idxvars = dict(h=(['clust','nh'],lambda x: format_id(x.clust)+format_id(x.nh)),
               w=('nh',lambda x: w),
               v=('clust',format_id),
               j=('homagrcd',lambda x: labelsd[format_id(x)]),
               u=('s8hq13',unitsd))

# Keep visits separate
myvars = {'Price':('s8hq14',_to_numeric)}
myvars.update({f"Produced_v{i}":(f"s8hq{i}",_to_numeric) for i in range(3,13)})

prod = df_data_grabber('../Data/S8H.DTA',idxvars,**myvars)

# Oddity with large number for missing code
na = prod.select_dtypes(exclude='object').max().max()

if na>1e99:  # Missing values?
    warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
    prod = prod.replace(na,np.nan)

prod = prod.groupby(['h','w','v','j','u']).sum() # Deal with some cases with multiple records

# Unstack by visits
prod = prod.replace(0,np.nan)
prod = pd.wide_to_long(prod.reset_index(),['Produced'],['h','w','v','j','u'],'visit',sep='_v')

fa = x.join(prod,how='outer')

fa = fa.dropna(how='all')

if __name__=='__main__':
    to_parquet(fa,'food_acquired.parquet')
