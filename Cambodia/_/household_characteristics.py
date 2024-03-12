#!/usr/bin/env python
"""
Concatenate data on household characteristics across rounds.
"""

import pandas as pd

z = pd.read_parquet('../2019-20/_/household_characteristics.parquet')

z.to_parquet('../var/household_characteristics.parquet')
