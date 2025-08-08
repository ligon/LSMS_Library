import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster
import pyreadstat

def age_sex_composition(df, sex, sex_converter, age, age_converter, hhid):
    Age_ints = ((0,4),(4,9),(9,14),(14,19),(19,31),(31,51),(51,100))
    testdf = get_household_roster(df, sex=sex, sex_converter=sex_converter,
                                  age=age, age_converter=age_converter, HHID=hhid,
                                  convert_categoricals=True,Age_ints=Age_ints,fn_type=None)
    testdf['log HSize'] = np.log(testdf[['girls', 'boys', 'men', 'women']].sum(axis=1))
    testdf.index.name = 'j'
    return testdf

def regional_data(t, filename, hhid, provinces):
    fs = dvc.api.DVCFileSystem('../../')
    fs.get_file('/Panama/'+t+'/Data/'+filename, '/tmp/'+filename)
    regional_info, meta_r  = pyreadstat.read_dta('/tmp/E03BASE.DTA', apply_value_formats = True, formats_as_category = True)
    regions = regional_info.groupby(hhid).agg({provinces: 'first'})
    regions.index = regions.index.map(str)
    return regions
