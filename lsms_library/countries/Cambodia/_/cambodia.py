#!/usr/bin/env python

import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster

def age_sex_composition(df):
    Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
    testdf = get_household_roster(df, sex='s02q03', sex_converter=lambda x:['m','f'][x==2],
                                  age='s02q05a', age_converter=None, HHID='HHID',
                                  convert_categoricals=True,Age_ints=Age_ints,fn_type=None)
    testdf['log HSize'] = np.log(testdf[['girls', 'boys', 'men', 'women']].sum(axis=1))
    testdf.index.name = 'j'
    return testdf
