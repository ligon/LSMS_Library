"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import pandas as pd
import numpy as np

p = []
for t in ['2005-06','2009-10','2010-11','2011-12','2013-14','2015-16','2018-19','2019-20']:
    df = pd.read_parquet('../'+t+'/_/food_acquired.parquet').squeeze()
    df['t'] = t
    df = df.reset_index().set_index(['j','t','i','units'])
    p.append(df)

p = pd.concat(p)

of = pd.read_parquet('../var/other_features.parquet')

p = p.join(of.reset_index('m')['m'],on=['j','t'])
p = p.reset_index().set_index(['j','t','m','i','units'])

p.to_parquet('../var/food_acquired.parquet')
