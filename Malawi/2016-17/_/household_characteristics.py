from lsms_library.local_tools import to_parquet
#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from malawi import get_household_characteristics

with dvc.api.open('../Data/hh_mod_b.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

final = get_household_characteristics(df, '2016-17')

to_parquet(final, 'household_characteristics.parquet')
