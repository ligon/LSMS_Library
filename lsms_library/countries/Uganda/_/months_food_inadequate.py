#!/usr/bin/env python
"""Compile months_food_inadequate (Family C) across Uganda UNPS waves.

Concatenates the per-wave (i, t) parquets and walks household ids onto
the panel-canonical ids.  Only waves whose GSEC17 'Welfare and Food
Security' module carries the food-deprivation item are present:
2009-10, 2010-11, 2011-12, 2013-14, 2015-16.  (2005-06 GSEC17 is a
land-rights module; 2018-19 / 2019-20 lack the item.)
"""
from lsms_library.local_tools import to_parquet, get_dataframe
import sys
sys.path.append('../../_/')
import pandas as pd
from uganda import id_walk
import json

WAVES = ['2009-10', '2010-11', '2011-12', '2013-14', '2015-16']

x = {}
for t in WAVES:
    print(t, file=sys.stderr)
    x[t] = get_dataframe('../' + t + '/_/months_food_inadequate.parquet')

x = pd.concat(x.values())

updated_ids = json.load(open('updated_ids.json'))
x = id_walk(x, updated_ids)

x = x.reset_index().set_index(['i', 't'])
x = x.astype({'MonthsInadequate': 'Int64', 'AnyInadequate': 'boolean'})

to_parquet(x, '../var/months_food_inadequate.parquet')
