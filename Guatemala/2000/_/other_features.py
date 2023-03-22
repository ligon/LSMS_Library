#!/usr/bin/env python3

import sys
sys.path.append('../../../_/')
import pandas as pd
import pyreadstat
import numpy as np
import json
import dvc.api
from lsms import from_dta
from lsms.tools import get_household_identification_particulars
from local_tools import other_features

fs = dvc.api.DVCFileSystem('../../')
fs.get_file('/Guatemala/2000/Data/ECV09P05.DTA', '/tmp/ECV09P05.DTA')
df, meta = pyreadstat.read_dta('/tmp/ECV09P05.DTA', apply_value_formats = True, formats_as_category = True)

myvars = dict(j='hogar',
              Rural='area',
              m='region')

of = df[list(myvars.values())].rename(columns={v:k for k,v in myvars.items()})

of['Rural'] = (of['Rural']=='rural') + 0
of['m'] = of.m.str.title()
of['t'] = '2000'

of['j'] = of.j.astype(str).apply(lambda s: s.split('.')[0])

of = of.groupby(['j','t','m']).head(1)

of = of.set_index(['j','t','m'])

of.to_parquet('other_features.parquet')
