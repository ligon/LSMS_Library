from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
"""Calculate food prices for different items across rounds; allow
different prices for different units.  
"""

import pandas as pd
import numpy as np
from uganda import Waves, id_walk
import json
p = []
for t in ['2005-06','2009-10','2010-11','2011-12','2013-14','2015-16','2018-19','2019-20']:
    df = get_dataframe('../'+t+'/_/food_acquired.parquet').squeeze()
    df['t'] = t
    df.index = df.index.rename({'units':'u'})
    # There may be occasional repeated reports of purchases of same food
    df = df.groupby(['j','t','i','u']).sum()
    df = df.reset_index().set_index(['j','t','i','u'])
    p.append(df)

p = pd.concat(p)

updated_ids = json.load(open('updated_ids.json'))
p = id_walk(p, updated_ids)

of = get_dataframe('../var/other_features.parquet')

p = p.join(of.reset_index('m')['m'],on=['j','t'])
p = p.reset_index().set_index(['j','t','m','i','u'])

to_parquet(p, '../var/food_acquired.parquet')
