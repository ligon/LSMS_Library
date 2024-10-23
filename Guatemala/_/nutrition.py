#!/usr/bin/env python
"""
Create a nutrition DataFrame for households based on food consumption quantities
"""

import pandas as pd
import numpy as np
from eep153_tools.sheets import read_sheets
import sys
sys.path.append('../../_/')
from local_tools import df_from_orgfile

fct = pd.read_parquet('../var/fct.parquet')
q = pd.read_parquet('../var/food_quantities.parquet').squeeze()
q = q.droplevel('u').unstack('i')
q = q.fillna(0)

use_foods = fct.index.intersection(q.columns)

n = q[use_foods]@fct.loc[use_foods,[np.issctype(d) for d in fct.dtypes]]
n.to_parquet('../var/nutrition.parquet')
