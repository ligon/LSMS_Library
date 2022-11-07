#!/usr/bin/env python
import sys
sys.path.append('../../_')
from uganda import food_expenditures

myvars = dict(fn='../Data/HH/gsec15b.dta',
              item='CEB01',
              HHID='hhid',
              purchased='CEB07',
              away='CEB09',
              produced='CEB11',
              given='CEB013')

x = food_expenditures(**myvars)  # Uses wave-specific hhids


x.to_parquet('food_expenditures.parquet')

