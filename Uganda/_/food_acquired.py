"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import pandas as pd
import numpy as np
from uganda import change_id, Waves, id_walk
import json
p = []
for t in ['2005-06','2009-10','2010-11','2011-12','2013-14','2015-16','2018-19','2019-20']:
    df = pd.read_parquet('../'+t+'/_/food_acquired.parquet').squeeze()
    df['t'] = t
    df.index = df.index.rename({'units':'u'})
    # There may be occasional repeated reports of purchases of same food
    df = df.groupby(['j','t','i','u']).sum()
    df = df.reset_index().set_index(['j','t','i','u'])
    p.append(df)

p = pd.concat(p)

of = pd.read_parquet('../var/other_features.parquet')

p = p.join(of.reset_index('m')['m'],on=['j','t'])
p = p.reset_index().set_index(['j','t','m','i','u'])

panel_id_json = json.load(open('../var/panel_id.json'))
p = id_walk(p, Waves, panel_id_json)

p.to_parquet('../var/food_acquired.parquet')
