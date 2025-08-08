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
from malawi import age_sex_composition, sex_conv

with dvc.api.open('../Data/sec_b.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

final = age_sex_composition(df, sex='b03', sex_converter=sex_conv,
                           age='b05a', age_converter=None, hhid='case_id')

final = final.reset_index()

final['t'] = '2004-05'
final = final.set_index(['j','t'])
final.columns.name = 'k'

to_parquet(final, 'household_characteristics.parquet')
