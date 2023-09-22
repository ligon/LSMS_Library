#!/usr/bin/env python

import sys
sys.path.append('../../_/')
import pandas as pd
import numpy as np
import json
import dvc.api
from lsms import from_dta

with dvc.api.open('../Data/PERSONA.DTA', mode='rb') as dta:
    df = from_dta(dta, convert_categoricals=False)

provinces = {1: 'Bocas Del Toro', 2: 'Coclé', 3: 'Colón', 4: 'Chíriqui', 5: 'Darién', 6: 'Herrera', 7: 'Los Santos', 8: 'Panamá', 9: 'Veraguas'}
df = df.replace({'provinci': provinces})
regions = df.groupby('form').agg({'provinci': 'first'})
regions = regions.reset_index()
regions = regions.rename(columns = {'provinci': 'm', 'form' : 'j'})

#regions['j'] = '1997' + regions['j'].map(str)
regions['j'] = regions['j'].map(str)
regions = regions.set_index('j')

df = regions

df.to_parquet('other_features.parquet')