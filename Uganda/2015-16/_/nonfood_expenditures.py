#!/usr/bin/env python
import sys
sys.path.append('../../_')
from uganda import nonfood_expenditures

myvars = dict(fn='../Data/gsec15c.dta',
              item='itmcd',
              HHID='hhid',
              purchased='h15cq5',
              away=None,
              produced='h15cq7',
              given='h15cq9')

x = nonfood_expenditures(**myvars) 
x.to_parquet('nonfood_expenditures.parquet')
