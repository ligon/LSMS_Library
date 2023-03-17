"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import pandas as pd
import numpy as np

p = []
for t in ['2000']:
    df = pd.read_parquet('../'+t+'/_/food_acquired.parquet').squeeze()
    df['units'] = 'lbs'
    # There may be occasional repeated reports of purchases of same food
    df = df.groupby(['j','t','i','units']).sum()
    df = df.reset_index().set_index(['j','t','i','units'])
    df = id_walk(df,t,Waves)
    p.append(df)

p = pd.concat(p)

of = pd.read_parquet('../var/household_characteristics.parquet')

p = p.join(of.reset_index('m')['m'],on=['j','t'])
p = p.reset_index().set_index(['j','t','m','i','units'])

p.to_parquet('../var/food_acquired.parquet')
