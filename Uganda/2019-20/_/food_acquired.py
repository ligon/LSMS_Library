from lsms_library.local_tools import to_parquet
#!/usr/bin/env python
import sys
sys.path.append('../../_/')
from uganda import food_acquired

fn = '../Data/HH/gsec15b.dta'

myvars = {'units':'CEB03C',
          'item':'CEB01',
          'HHID':'hhid',
          'market_home':'CEB14a',
          'market_away':'CEB14b',
          'market_own':'CEB14c',
          'farmgate':'CEB15',
          'value_home':'CEB07',
          'value_away':'CEB09',
          'value_own':'CEB11',
          'value_inkind':'CEB013',
          'quantity_home':'CEB06',
          'quantity_away':'CEB08',
          'quantity_own':'CEB10',
          'quantity_inkind':'CEB012'}

df = food_acquired(fn,myvars)

to_parquet(df, 'food_acquired.parquet')
