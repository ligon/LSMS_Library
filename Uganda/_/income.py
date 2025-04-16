from lsms_library.local_tools import to_parquet
from lsms_library.local_tools import get_dataframe
#!/usr/bin/env python3
"""
Combine income from different sources to make 'total' household income.
"""

import pandas as pd
import numpy as np

income_sources = [('earnings',['earnings']),
                  ('enterprise_income',['profits'])]

income = 0
for source,cols in income_sources:
    income = income + get_dataframe(f'../var/{source}.parquet')[cols].sum(axis=1)

income = income.replace(0,np.nan).dropna()

to_parquet(pd.DataFrame({'income':income}), '../var/income.parquet')
