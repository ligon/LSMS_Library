#!/usr/bin/env python
"""GhanaLSS 1991-92 canonical ``food_acquired``.

Emits the canonical long form with index ``(t, i, j, u, s, visit)`` and
columns ``[Quantity, Expenditure, Price]`` (Uganda ``food_acquired_to_canonical``
contract PLUS the GhanaLSS-local ``visit`` level -- DESIGN doc D1: keep the
repeated-visit recall structure; the DERIVED tables sum it out).

``s`` in {purchased, produced}:
  * purchased (S9B / Code_9b) -- value-only: u='Value', Expenditure=value,
    Quantity=Expenditure (no fabricated/imputed qty; Phase-3 out of scope).
  * produced  (S8H / Code_8h) -- real Quantity + decoded unit ``u``
    (GH #348: s8hq13 decoded via the wave ``units`` table) + farmgate Price;
    Expenditure stays NaN (no produced value recorded this wave).

``i`` is built via this wave's canonical ``mapping.i()`` (clust + zero-padded
nh, no separator) so ``food.i`` matches ``sample.i`` / ``roster.i`` -- fixes the
100% NaN-v bug (GH #256): the old script used format_id(clust)+format_id(nh)
WITHOUT the 2-digit zero-pad ("30022" vs the canonical "300202").

``v`` is intentionally absent -- the framework joins it from ``sample()`` at API
time (``_join_v_from_sample``; see CLAUDE.md "sample() and Cluster Identity").
"""
import sys
import numpy as np
import pandas as pd
sys.path.append('.')
sys.path.append('../../_')
sys.path.append('../../../_/')
import mapping
from lsms_library.local_tools import get_categorical_mapping, format_id, df_data_grabber, _to_numeric, to_parquet
import warnings

t = '1991-92'

####################
# Purchased (S9B)
####################
# value-only this wave: u='Value', Expenditure=value, Quantity=Expenditure.
labelsd = get_categorical_mapping(tablename='harmonize_food',
                                  idxvars={'Code':('Code_9b',format_id)},
                                  **{'Label':'Preferred Label'})

idxvars = dict(i=(['clust','nh'], lambda x: mapping.i(pd.Series([x.clust, x.nh]))),
               t=('nh', lambda x: t),
               j=('fdexpcd', lambda x: labelsd[format_id(x)]))

# Keep visits separate (purchase visits ~ 1-9 here; stems s9bq2..s9bq10).
myvars = {f"Purchased_v{i}":(f"s9bq{i}",_to_numeric) for i in range(2,11)}

x = df_data_grabber('../Data/S9B.DTA',idxvars,**myvars)

x = x.groupby(['i','t','j']).sum()  # collapse multiple records per (i,t,j)
x = x.replace(0,np.nan)

x = pd.wide_to_long(x.reset_index(),['Purchased'],['i','t','j'],'visit',sep='_v')

# Purchases: u='Value'; Expenditure=value; Quantity mirrors Expenditure.
x['u'] = 'Value'
x['s'] = 'purchased'
x = x.rename(columns={'Purchased':'Expenditure'})
x['Quantity'] = x['Expenditure']
x['Price'] = np.nan
x = x.reset_index().set_index(['t','i','j','u','s','visit'])
x = x[['Quantity','Expenditure','Price']]

####################
# Home produced (S8H)
####################
labelsd = get_categorical_mapping(tablename='harmonize_food',
                                  idxvars={'Code':('Code_8h',format_id)},
                                  **{'Label':'Preferred Label'})
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
idxvars = dict(i=(['clust','nh'], lambda x: mapping.i(pd.Series([x.clust, x.nh]))),
               t=('nh', lambda x: t),
               j=('homagrcd', lambda x: labelsd[format_id(x)]),
               u=('s8hq13', lambda x: unitsd.get(format_id(x), pd.NA)))

# Keep visits separate (produced visits ~ 3-12; stems s8hq3..s8hq12).
myvars = {'Price':('s8hq14',_to_numeric)}
myvars.update({f"Produced_v{i}":(f"s8hq{i}",_to_numeric) for i in range(3,13)})

prod = df_data_grabber('../Data/S8H.DTA',idxvars,**myvars)

# Oddity with large number for missing code
na = prod.select_dtypes(exclude=['object', 'category']).max().max()

if na>1e99:  # Missing values?
    warnings.warn(f"Large number used for missing?  Replacing {na} with NaN.")
    prod = prod.replace(na,np.nan)

prod = prod.groupby(['i','t','j','u']).sum()  # collapse multiple records

# Unstack by visits
prod = prod.replace(0,np.nan)
prod = pd.wide_to_long(prod.reset_index(),['Produced'],['i','t','j','u'],'visit',sep='_v')

# Produced: real Quantity + decoded u + farmgate Price; Expenditure NaN.
prod = prod.rename(columns={'Produced':'Quantity'})
prod['s'] = 'produced'
prod['Expenditure'] = np.nan
prod = prod.reset_index().set_index(['t','i','j','u','s','visit'])
prod = prod[['Quantity','Expenditure','Price']]

####################
# Stack purchased + produced
####################
fa = pd.concat([x, prod])

fa = fa.dropna(how='all')

# Drop rows whose food item failed to harmonize (j == '').  Purchases are
# 100% food-mapped this wave per the audit, so this is near-zero.
fa = fa.reset_index()
fa = fa[fa['j'] != '']
fa = fa.set_index(['t','i','j','u','s','visit'])

if __name__=='__main__':
    to_parquet(fa,'food_acquired.parquet')
