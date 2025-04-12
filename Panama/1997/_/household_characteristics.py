from lsms_library.local_tools import to_parquet
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

with dvc.api.open('../Data/PERSONA.DTA', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=False)

provinces = {1: 'Bocas Del Toro', 2: 'Coclé', 3: 'Colón', 4: 'Chíriqui', 5: 'Darién', 6: 'Herrera', 7: 'Los Santos', 8: 'Panamá', 9: 'Veraguas'}
df = df.replace({'provinci': provinces})

regions = df.groupby('form').agg({'provinci' : 'first'})
regions.index = regions.index.map(str)

out = age_sex_composition(df, sex='p202', sex_converter=lambda x: ['m', 'f'][x==2],
                           age='p203', age_converter=None, hhid='form')

out.index = out.index.map(str)

final = out.join(regions)

final = final.rename(columns = {'provinci' : 'm'})
final['t'] = '1997'
final = final.set_index(['t', 'm'], append = True)
final.columns.name = 'k'

to_parquet(final, 'household_characteristics.parquet')
