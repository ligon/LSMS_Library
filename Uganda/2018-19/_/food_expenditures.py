#!/usr/bin/env python
import sys
sys.path.append('../../_')
from uganda import food_expenditures

myvars = dict(fn='Uganda/2018-19/Data/GSEC15B.dta',item='CEB01',HHID='hhid',
              purchased='CEB05',
              away='CEB07',
              produced='CEB09',
              given='CEB011')

x = food_expenditures(**myvars)

x.to_parquet('food_expenditures.parquet')

