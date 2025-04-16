from lsms_library.local_tools import to_parquet
#!/usr/bin/env python
import sys
sys.path.append('../../_')
from cotedivoire import household_characteristics

myvars = dict(fn='../Data/F01A.DAT',HHID='HID',
              age='AGEY',sex=('SEX',lambda s: 'm' if s==1 else 'f'),
              months_spent='MON')

z = household_characteristics(**myvars)

z.columns.name = 'k'
z.index.name = 'j'

z['t'] = '1985-86'
z['m'] = "Cote d'Ivoire"

z = z.reset_index().set_index(['j','t','m'])

to_parquet(z, 'household_characteristics.parquet')

