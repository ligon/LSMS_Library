#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_')
from uganda import nonfood_expenditures

round = '2019-20'

myvars = dict(fn='../Data/HH/gsec15c.dta',
              item='CEC02',
              HHID='hhid',
              purchased='CEC05',
              away=None,
              produced='CEC07',
              given='CEC09')

x = nonfood_expenditures(**myvars)

# `t` is stamped by the WAVE script, mirroring the sibling food_acquired path
# (2013-14/_/food_acquired.py):  uganda.nonfood_expenditures returns the long
# (i, j) x Expenditure frame, and this is the only place that knows the wave.
x['t'] = round
x = x.reset_index().set_index(['t', 'i', 'j'])

assert x.index.is_unique, 'Non-unique (t, i, j) index!  Fix me!'
assert len(x) > 0, 'nonfood_expenditures produced no rows'

to_parquet(x, 'nonfood_expenditures.parquet')
