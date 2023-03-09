"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
from tanzania import Waves, id_match
import dvc.api
from lsms import from_dta

x={}
for t in Waves.keys():
    x[t] = pd.read_parquet('../'+t+'/_/food_acquired.parquet')
    x[t] = id_match(x[t],t,Waves)

x = pd.concat(x.values())
x['m'] = 'Tanzania'
x = x.reset_index().set_index(['j','t','m', 'i'])
x = x.drop(columns ='index')
#of = pd.read_parquet('../var/other_features.parquet')

#p = p.join(of.reset_index('m')['m'],on=['j','t'])
#p = p.reset_index().set_index(['j','t','m','i','units'])

x.to_parquet('../var/food_acquired.parquet')
