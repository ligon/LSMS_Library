#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/04persona.dta', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=True)

regions = df.groupby('hogar').agg({'prov' : 'first'})
regions.index = regions.index.map(str)

region_dict = {'bocas del toro' : 'Bocas Del Toro', 'colón': 'Colón', 'coclé': 'Coclé',
               'chiriquí': 'Chíriqui', 'darién': 'Darién', 'panamá': 'Panamá', 'veraguas': 'Veraguas',
               'herrera': 'Herrera', 'los santos': 'Los Santos', 'comarca kuna yala': 'Comarca Kuna Yala', 'comarca emberá': 'Comarca Emberá', 'comarca ngöbe bugle': 'Comarca Ngobe Bugle'}
regions = regions.replace({'prov' : region_dict})

regions = regions.reset_index().rename(columns = {'prov': 'm', 'hogar' : 'j'})

#regions['j'] = '2008' + regions['j'].map(str)
regions['j'] = regions['j'].map(str)
regions = regions.set_index('j')

df = regions

df.to_parquet('other_features.parquet')
