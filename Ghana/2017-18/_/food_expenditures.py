#!/usr/bin/env python
import sys
sys.path.append('../../_')
from ghana_panel import food_expenditures

myvars = dict(fn='Ghana/2017-18/Data/11a_foodconsumption_prod_purch.dta',item='foodname',HHID='FPrimary',
              purchased='purchasedcedis',
              produced='producedcedis',
              given='receivedgiftcedis')

x = food_expenditures(**myvars)

x.to_parquet('food_expenditures.parquet')

