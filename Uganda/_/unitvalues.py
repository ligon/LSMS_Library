from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python
"""
Calculate unit values for different items across rounds.
"""

import pandas as pd
import numpy as np

q = get_dataframe('../var/food_quantities.parquet').squeeze()
# Use_units turns out to almost always be kilograms...
q = q.xs('Kg',level='u')
q = q.replace(0.0,np.nan).dropna()

x = get_dataframe('../var/food_expenditures.parquet').stack('i')

unitvalues = (x/q).dropna().unstack('i')

to_parquet(unitvalues, '../var/unitvalues.parquet')
