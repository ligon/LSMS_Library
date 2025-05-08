#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_dataframe

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from malawi import get_household_characteristics

with dvc.api.open('../Data/Cross_Sectional/hh_mod_b.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

panel_df = get_dataframe('../Data/Panel/hh_mod_b_16.dta')
final = get_household_characteristics(df, '2016-17')
final_panel_df = get_household_characteristics(panel_df, '2016-17', hhid = 'y3_hhid')

to_parquet(pd.concat([final, final_panel_df], axis=0), 'household_characteristics.parquet')
