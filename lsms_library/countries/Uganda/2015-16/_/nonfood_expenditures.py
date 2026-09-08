#!/usr/bin/env python
from lsms_library.local_tools import to_parquet, get_dataframe
import sys
sys.path.append('../../_')
from uganda import nonfood_expenditures

round = '2015-16'

myvars = dict(fn='../Data/gsec15c.dta',
              item='itmcd',
              HHID='hhid',
              purchased='h15cq5',
              away=None,
              produced='h15cq7',
              given='h15cq9')

x = nonfood_expenditures(**myvars)

# "Wrong" hhid variable; get correct one from gsec1.  The helper's household
# level is `i` (GH #817: it used to be miscalled `j`, which is now the item).
ids = get_dataframe('../Data/gsec1.dta')[['HHID','hh']]

ids = ids.set_index('hh').squeeze().to_dict()

flat = x.reset_index()
unmapped = (~flat['i'].isin(ids)).sum()
assert unmapped == 0, (
    f'{unmapped} of {len(flat)} rows carry an hh id absent from gsec1; '
    f'`replace` would leave them on the wrong id silently.')
flat['i'] = flat['i'].replace(ids)
x = flat.set_index(['i', 'j'])

# `t` is stamped by the WAVE script, mirroring the sibling food_acquired path
# (2013-14/_/food_acquired.py):  uganda.nonfood_expenditures returns the long
# (i, j) x Expenditure frame, and this is the only place that knows the wave.
x['t'] = round
x = x.reset_index().set_index(['t', 'i', 'j'])

assert x.index.is_unique, 'Non-unique (t, i, j) index!  Fix me!'
assert len(x) > 0, 'nonfood_expenditures produced no rows'

to_parquet(x, 'nonfood_expenditures.parquet')
