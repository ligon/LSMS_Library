from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
"""Calculate food prices for different items across rounds; allow
different prices for different units.
"""

import pandas as pd
import numpy as np

fa = []
for t in ['2007']:
    df = get_dataframe('../'+t+'/_/food_acquired.parquet')
    df = df.reset_index()
    df['t'] = t
    df = df.set_index(['j', 't', 'i', 'units'])
    df.index = df.index.rename({'units': 'u'})
    fa.append(df)

fa = pd.concat(fa)

fa = fa.replace(0,np.nan)
fa = fa.groupby(['j','t','i','u']).sum()
fa = fa.replace(0,np.nan)

to_parquet(fa, '../var/food_acquired.parquet')
