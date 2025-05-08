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

with dvc.api.open('../Data/Cross_Sectional/HH_MOD_B.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

final = get_household_characteristics(df, '2019-20')
panel_df = get_dataframe('../Data/Panel/hh_mod_b_19.dta')
final_panel_df = get_household_characteristics(panel_df, '2019-17', hhid = 'y4_hhid')

to_parquet(pd.concat([final, final_panel_df], axis=0), 'household_characteristics.parquet')
