#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster
from panama import age_sex_composition

with dvc.api.open('../Data/E03PE03.DTA', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

with dvc.api.open('../Data/E03BASE.DTA', mode='rb') as dta:
    regional_info = from_dta(dta, convert_categoricals=True)

regions = regional_info.groupby('form').agg({'hno' : 'first', 'prov' : 'first'})
