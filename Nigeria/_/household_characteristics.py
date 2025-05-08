import pandas as pd
import numpy as np
from lsms_library.transformations import roster_to_characteristics
from lsms_library.local_tools import map_index
from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
df = get_dataframe('../var/household_roster.parquet')
df = roster_to_characteristics(df, drop = 'indiv', final_index = ['t', 'm', 'j'])
df = map_index(df)

to_parquet(df, '../var/household_characteristics.parquet')
