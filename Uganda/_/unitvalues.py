#!/usr/bin/env python
"""
Calculate unit values for different items across rounds.
"""

import pandas as pd
import numpy as np

q = pd.read_parquet('../var/food_quantities.parquet').squeeze()
# Use_units turns out to almost always be kilograms...
q = q.xs('Kg',level='u')
q = q.replace(0.0,np.nan).dropna()

x = pd.read_parquet('../var/food_expenditures.parquet').stack('i')

unitvalues = (x/q).dropna().unstack('i')

unitvalues.to_parquet('../var/unitvalues.parquet')
