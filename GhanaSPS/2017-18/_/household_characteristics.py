from lsms_library.local_tools import to_parquet
#!/usr/bin/env python
import sys
sys.path.append('../../_')
from ghana_panel import household_characteristics

t = '2017-18'

myvars = dict(fn='../Data/01b2_roster.dta', HHID='FPrimary',
              age='ageyears',sex=('gender',lambda s: s.lower()[0]),
              months_spent=None)

z = household_characteristics(**myvars)

z.columns.name = 'k'
z.index.name = 'j'

z['t'] = t
z['m'] = "Ghana"

z = z.reset_index()
z['j'] = z['j'].astype(str)
z = z.set_index(['j','t','m'])
to_parquet(z, 'household_characteristics.parquet')