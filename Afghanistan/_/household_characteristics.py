from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python
"""
Concatenate data on household characteristics across rounds.
"""

import pandas as pd

z = get_dataframe('../2016-17/_/household_characteristics.parquet')

to_parquet(z, 'household_characteristics.parquet')
