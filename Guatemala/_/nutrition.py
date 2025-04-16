from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python
"""
Create a nutrition DataFrame for households based on food consumption quantities
"""

import pandas as pd
import numpy as np
from eep153_tools.sheets import read_sheets
import sys
sys.path.append('../../_/')
from lsms_library.local_tools import df_from_orgfile

fct = get_dataframe('../var/fct.parquet')
q = get_dataframe('../var/food_quantities.parquet').squeeze()
q = q.droplevel('u').unstack('i')
q = q.fillna(0)

use_foods = fct.index.intersection(q.columns)

n = q[use_foods]@fct.loc[use_foods,[np.issctype(d) for d in fct.dtypes]]
to_parquet(n, '../var/nutrition.parquet')
