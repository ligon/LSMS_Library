#!/usr/bin/env python
import sys
import numpy as np
import pandas as pd
sys.path.append('../../_')
from ghanalss import split_by_visit
sys.path.append('../../../_/')
from lsms_library.local_tools import df_from_orgfile, get_categorical_mapping, format_id, df_data_grabber, _to_numeric, to_parquet
import warnings

w = '1991-92'

# categorical mapping
labelsd = get_categorical_mapping(tablename='harmonize_food',idxvars={'Code':('Code_9b',format_id)},**{'Label':'Preferred Label'})

# food expenditure
idxvars = dict(h=(['clust','nh'],lambda x: format_id(x.clust)+format_id(x.nh)),
               w=('nh',lambda x: w),
               j=('fdexpcd',lambda x: labelsd[format_id(x)]))

# Keep visits separate
myvars = {f"Purchased_v{i}":(f"s9bq{i}",_to_numeric) for i in range(2,11)}


x = df_data_grabber('../Data/S9B.DTA',idxvars,**myvars)

x = x.groupby(['h','w','j']).sum() # Deal with some cases with multiple records for purchases
x = x.replace(0,np.nan)

x = pd.wide_to_long(x.reset_index(),['Purchased'],['h','w','j'],'visit',sep='_v')

# Add unit index--these will all be "Value" for purchases
x['u'] = 'Value'
x = x.reset_index().set_index(['h','w','j','u','visit'])

####################
# Home produced
####################

labelsd = get_categorical_mapping(tablename='harmonize_food',idxvars={'Code':('Code_8h',format_id)},**{'Label':'Preferred Label'})
# GH #348: pass the value column ('Label') so the Code->Label dict is built.
# A bare get_categorical_mapping(tablename='units') yields an EMPTY dict (no
# value column requested -> df_data_grabber returns a column-less frame), so
# the wave-level unit decode was silently dead and raw s8hq13 codes
# (1/7/8/9/12/14/16/17/18/19/22/23/25) leaked into food_acquired's u.
unitsd = get_categorical_mapping(tablename='units', Label='Label')

# food quantities.  s8hq13 reads as float (1.0); the units table is keyed on
# string codes ('1'), so normalize via format_id before lookup (same pattern
# as j above) -- otherwise every code misses the dict.  Unknown / missing
# sentinel codes (Stata's large-number NA) map to <NA>.
idxvars = dict(h=(['clust','nh'],lambda x: format_id(x.clust)+format_id(x.nh)),
               w=('nh',lambda x: w),
               j=('homagrcd',lambda x: labelsd[format_id(x)]),
               u=('s8hq13',lambda x: unitsd.get(format_id(x), pd.NA)))

# Keep visits separate
myvars = {'Price':('s8hq14',_to_numeric)}
myvars.update({f"Produced_v{i}":(f"s8hq{i}",_to_numeric) for i in range(3,13)})

prod = df_data_grabber('../Data/S8H.DTA',idxvars,**myvars)

# Oddity with large number for missing code
na = prod.select_dtypes(exclude=['object', 'category']).max().max()

if na>1e99:  # Missing values?
    warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
    prod = prod.replace(na,np.nan)

prod = prod.groupby(['h','w','j','u']).sum() # Deal with some cases with multiple records

# Unstack by visits
prod = prod.replace(0,np.nan)
prod = pd.wide_to_long(prod.reset_index(),['Produced'],['h','w','j','u'],'visit',sep='_v')

fa = x.join(prod,how='outer')

fa = fa.dropna(how='all')

if __name__=='__main__':
    to_parquet(fa,'food_acquired.parquet')
