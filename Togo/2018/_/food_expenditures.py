#!/usr/bin/env python
import sys
sys.path.append('../../_')
from togo import food_expenditures

myvars = dict(fn='Togo/2018/Data/Togo_survey2018_fooditems_forEthan.dta',item='food_item',HHID='hhid',
              purchased='item_value',
              produced=None,
              given=None)

x = food_expenditures(**myvars)

x.to_parquet('food_expenditures.parquet')

