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

with dvc.api.open('../Data/04persona.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

regions = df.groupby('hogar').agg({'prov' : 'first'})
regions.index = regions.index.map(str)

final  = age_sex_composition(df, sex='p3_sexo', sex_converter=lambda x: ['m', 'f'][x=='mujer'],
                           age='p4_edad', age_converter=None, hhid='hogar')

final = pd.merge(left = final, right = regions, how = 'left', left_index = True, right_index = True)
final = final.rename(columns = {'prov' : 'm'})
final['t'] = '2008'
region_dict = {'bocas del toro' : 'Bocas Del Toro', 'colón': 'Colón', 'coclé': 'Coclé',
               'chiriquí': 'Chíriqui', 'darién': 'Darién', 'panamá': 'Panamá', 'veraguas': 'Veraguas',
               'herrera': 'Herrera', 'los santos': 'Los Santos', 'comarca kuna yala': 'Comarca Guna Yala', 'comarca emberá': 'Comarca Emberá', 'comarca ngöbe bugle': 'Comarca Ngobe Bugle'}
final = final.replace({'m' : region_dict})

final = final.set_index(['t', 'm'], append = True)
final.columns.name = 'k'

final.to_parquet('household_characteristics.parquet')
