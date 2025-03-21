import pandas as pd
import numpy as np
from lsms_library.transformations import roster_to_characteristics
from lsms_library.local_tools import map_index
df = pd.read_parquet('../var/household_roster.parquet')
df = roster_to_characteristics(df, drop = 'indiv', final_index = ['t', 'm', 'j'])
df = map_index(df)

df.to_parquet('../var/household_characteristics.parquet')
