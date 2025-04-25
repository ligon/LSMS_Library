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

with dvc.api.open('../Data/Full_Sample/Household/hh_mod_b.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

final = get_household_characteristics(df, '2010-11')

to_parquet(final, 'household_characteristics.parquet')
