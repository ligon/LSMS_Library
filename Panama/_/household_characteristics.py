from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
for t in ['1997', '2003', '2008']:
    df = get_dataframe('../'+t+'/_/household_characteristics.parquet')
    df = df.reset_index()
    df['j'] = t + df['j'].astype(str)
    df.replace({'Comarca de San Blas': 'Comarca Kuna Yala'})
    df = df.set_index(['j', 't', 'm'])
    df.columns.name = 'k'
    x.append(df)

concatenated = pd.concat(x)

to_parquet(concatenated, '../var/household_characteristics.parquet')
