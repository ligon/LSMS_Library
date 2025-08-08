#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_')
from cotedivoire import household_characteristics

t = '1988-89'

myvars = dict(fn='../Data/SEC01A.DAT', HHID='NH',
              age='AGEY',sex=('SEX',lambda s: 'm' if s==1 else 'f'),
              months_spent='MON')

z = household_characteristics(**myvars)

z.columns.name = 'k'
z.index.name = 'j'

z['t'] = t
z['m'] = "Cote d'Ivoire"

z = z.reset_index().set_index(['j','t','m'])

to_parquet(z, 'household_characteristics.parquet')

