#!/usr/bin/env python

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

final.to_parquet('household_characteristics.parquet')
