#!/usr/bin/env python
import sys
sys.path.append('../../_')
from cotedivoire import food_expenditures

myvars = dict(fn='CotedIvoire/1985-86/Data/F12A.DAT',item='FOODCD',HHID='HID',
              purchased='CFOODB')

x = food_expenditures(**myvars)

x.to_parquet('food_expenditures.parquet')

