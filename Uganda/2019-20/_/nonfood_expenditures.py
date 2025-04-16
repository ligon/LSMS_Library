from lsms_library.local_tools import to_parquet
#!/usr/bin/env python
import sys
sys.path.append('../../_')
from uganda import nonfood_expenditures

myvars = dict(fn='../Data/HH/gsec15c.dta',
              item='CEC02',
              HHID='hhid',
              purchased='CEC05',
              away=None,
              produced='CEC07',
              given='CEC09')

x = nonfood_expenditures(**myvars) 

to_parquet(x, 'nonfood_expenditures.parquet')