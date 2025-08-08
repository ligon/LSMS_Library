#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_')
from uganda import nonfood_expenditures

myvars = dict(fn='../Data/GSEC15C.dta',
              item='CEC02',
              HHID='hhid',
              purchased='CEC05',
              away=None,
              produced='CEC07',
              given='CEC09')

x = nonfood_expenditures(**myvars) 
to_parquet(x, 'nonfood_expenditures.parquet')
