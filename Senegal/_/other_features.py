from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
years = ['2018-19']
for t in years:
    x.append(get_dataframe('../'+t+'/_/other_features.parquet'))

of = pd.concat(x)

to_parquet(of, '../var/other_features.parquet')
