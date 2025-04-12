from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python
"""
Concatenate data on other household features across rounds.
"""

import pandas as pd

x = []
for t in ['2014', '2018-19', '2021-22']:
    x.append(get_dataframe('../'+t+'/_/household_characteristics.parquet'))

concatenated = pd.concat(x)

to_parquet(concatenated, '../var/household_characteristics.parquet')
