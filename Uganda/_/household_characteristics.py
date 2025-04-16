from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd
from uganda import Waves, id_walk
import json

x = {}

for t in Waves.keys():
    print(t)
    x[t] = get_dataframe('../'+t+'/_/household_characteristics.parquet')
    x[t] = x[t].stack('k').dropna()
    x[t] = x[t].reset_index().set_index(['j','k']).squeeze()


z = pd.DataFrame(x)
z.columns.name = 't'

z = z.stack().unstack('k')

with open('updated_ids.json','r') as f:
    updated_ids =json.load(f)

z = id_walk(z, updated_ids)

try:
    of = get_dataframe('../var/other_features.parquet')
    z = z.join(of.reset_index('m')['m'], on=['j', 't'])

except FileNotFoundError:
    z['m'] ='Uganda'

z = z.reset_index().set_index(['j','t','m'])


to_parquet(z, '../var/household_characteristics.parquet')
