"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""
import sys
sys.path.append('../../_')
from lsms_library.local_tools import df_from_orgfile
import pandas as pd
import numpy as np
from tanzania import Waves, add_markets_from_other_features, country, id_walk, waves
import dvc.api
from lsms import from_dta
import json
import warnings

x={}
for t in Waves.keys():
    x[t] = pd.read_parquet('../'+t+'/_/food_acquired.parquet')

x['2008-15'] = x['2008-15'].reset_index().rename(columns = {'UPHI':'j'}).set_index(['j','t','i'])

foo = x.copy()
x = pd.concat(x)

x = x.reset_index().set_index(['j','t','i'])

if 'm' in x.columns:
    x = x.drop('m',axis=1)

try:
    x = add_markets_from_other_features('',x)
except FileNotFoundError:
    warnings.warn('No other_features.parquet found.')
    x['m'] = country
    x = x.reset_index().set_index(['j','t','m','i'])

# Fix food labels
fl = df_from_orgfile('CONTENTS.org','food_labels')

fl = fl.set_index('Preferred Label')
# Map labels from different waves to Preferred
fl = fl[list(Waves.keys())]
foo = fl.stack().droplevel(1)
fl = {x[1]:x[0] for x in foo.to_frame().to_records()}

# ISSUE: fl seems not to include right variant of k in some cases.
# Super crude fix: Add all variants as keys.
keys = list(fl.keys())
for k in keys:
    fl[k.upper()] = fl[k]
    fl[k.lower()] = fl[k]
    fl[k.title()] = fl[k]

xlabels = set(x.index.get_level_values('i'))
xtra_labels = xlabels.difference(fl.keys())
if len(xtra_labels):
    warnings.warn('Extra labels without preferred label!')
    print(xtra_labels)

x = x.loc[~x.index.duplicated(),:]
x = x.rename(index=fl,level='i')
x = x.reset_index().set_index(['j','t','m','i'])

# Drop any observations with NaN in the index
idx = pd.MultiIndex.from_frame(pd.DataFrame(x.index.to_list(),columns=x.index.names).dropna())
x = x.loc[idx].sort_index()

with open('panel_ids.json','r') as f:
    panel_id_json =json.load(f)

x = id_walk(x, waves, panel_id_json)


assert x.index.is_unique, "Non-unique index!  Fix me!"

x.to_parquet('../var/food_acquired.parquet')
