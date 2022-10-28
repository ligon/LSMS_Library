#!/usr/bin/env python
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
x.to_parquet('nonfood_expenditures.parquet')
