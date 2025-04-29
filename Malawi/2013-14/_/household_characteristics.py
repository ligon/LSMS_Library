#!/usr/bin/env python
from lsms_library.local_tools import to_parquet

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster
from malawi import get_household_characteristics

with dvc.api.open('../Data/HH_MOD_B_13.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

final = get_household_characteristics(df, '2013-14', hhid='y2_hhid')

to_parquet(final, 'household_characteristics.parquet')
