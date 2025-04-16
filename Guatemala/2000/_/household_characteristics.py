from lsms_library.local_tools import to_parquet
#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import pyreadstat
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_roster
from guatemala import age_sex_composition

fs = dvc.api.DVCFileSystem('../../')
fs.get_file('/Guatemala/2000/Data/ECV09P05.DTA', '/tmp/ECV09P05.DTA')
df, meta = pyreadstat.read_dta('/tmp/ECV09P05.DTA', apply_value_formats = True, formats_as_category = True)

final  = age_sex_composition(df, sex='sexo', sex_converter=lambda x: ['m', 'f'][x=='femenino'],
                           age='edad', age_converter=None, hhid='hogar')

final = final.reset_index()
final['j'] = final.j.astype(str).apply(lambda s: s.split('.')[0])

final['t'] = '2000'
final = final.set_index(['j','t'])
final.columns.name = 'k'

to_parquet(final, 'household_characteristics.parquet')
