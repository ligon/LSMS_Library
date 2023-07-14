#!/usr/bin/env python

import pandas as pd
import pyreadstat
import numpy as np
import json
import dvc.api


fs = dvc.api.DVCFileSystem('../../')
fs.get_file('/Panama/2003/Data/E03BASE.DTA', '/tmp/E03BASE.DTA')
regional_info, meta_r = pyreadstat.read_dta('/tmp/E03BASE.DTA', apply_value_formats = True, formats_as_category = True)

regions = regional_info.groupby('form').agg({'prov' : 'first'})
regions.index = regions.index.map(str)

region_dict = {'Comarca de San Blas': 'Comarca Kuna Yala'}

regions = regions.replace({'prov' : region_dict})

regions = regions.reset_index().rename(columns = {'prov': 'm', 'form' : 'j'})

#regions['j'] = '2003' + regions['j'].map(str)
regions['j'] = regions['j'].map(str)
regions = regions.set_index('j')

df = regions

df.to_parquet('other_features.parquet')
