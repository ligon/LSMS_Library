from lsms_library.local_tools import to_parquet
#!/usr/bin/env python
import sys
sys.path.append('../../_')
from uganda import nonfood_expenditures

myvars = dict(fn='../Data/GSEC15C.dta',
              item='itmcd',
              HHID='HHID',
              purchased='h15cq5',
              away=None,
              produced='h15cq7',
              given='h15cq9')

x = nonfood_expenditures(**myvars) 
to_parquet(x, 'nonfood_expenditures.parquet')
