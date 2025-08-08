#!/usr/bin/env python
from lsms_library.local_tools import to_parquet
import sys
sys.path.append('../../_')
from uganda import nonfood_expenditures

myvars = dict(fn='../Data/GSEC15c.dta',
              item='h15cq2',
              HHID='hh',
              purchased='h15cq5',
              away=None,
              produced='h15cq7',
              given='h15cq9')

x = nonfood_expenditures(**myvars) 
to_parquet(x, 'nonfood_expenditures.parquet')
