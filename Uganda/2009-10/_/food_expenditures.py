#!/usr/bin/env python
import sys
sys.path.append('../../_')
from uganda import food_expenditures

myvars = dict(fn='../Data/GSEC15b.dta',item='h15bq2',HHID='hh',
              purchased='h15bq5',
              away='h15bq7',
              produced='h15bq9',
              given='h15bq11')

x = food_expenditures(**myvars)

# File includes one observation with bogus item code 170; drop!

x=x.stack('i')
new = x.reset_index('i')
new = new.loc[new['i']!='Pineapple']
x = new.reset_index().set_index(['j','i']).squeeze().unstack('i')

x.to_parquet('food_expenditures.parquet')

