#!/usr/bin/env python
import sys
sys.path.append('../../../_')
from local_tools import to_parquet

import pandas as pd
import pyreadstat
import numpy as np
import json
import dvc.api


fs = dvc.api.DVCFileSystem('../../')
fs.get_file('/Panama/2003/Data/E03BASE.DTA', '/tmp/E03BASE.DTA')
regional_info, meta_r = pyreadstat.read_dta('/tmp/E03BASE.DTA', apply_value_formats = True, formats_as_category = True)

regions = regional_info.groupby('form').agg({'prov': 'first', 'area1':'first'})
regions.index = regions.index.map(str)

regions = regions.reset_index().rename(columns = {'prov':'m', 'form':'j', 'area1':'Rural'})

#regions['j'] = '2003' + regions['j'].map(str)
regions['j'] = regions['j'].map(str)
regions['Rural'] = (regions.Rural==2) + 0
regions = regions.set_index(['j','m'])

to_parquet(regions,'other_features.parquet')
