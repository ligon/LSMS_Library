from lsms_library.local_tools import to_parquet
#!/usr/bin/env python
import sys
sys.path.append('../../_')
from cotedivoire import food_expenditures

myvars = dict(fn='CotedIvoire/1985-86/Data/F12A.DAT',item='FOODCD',HHID='HID',
              purchased='CFOODB')

x = food_expenditures(**myvars)

x['t'] = '1985-86'
x['m'] = "Cote d'Ivoire"

x = x.reset_index().set_index(['j','t','m'])

to_parquet(x, 'food_expenditures.parquet')

