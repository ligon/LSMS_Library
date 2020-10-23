#!/usr/bin/env python
import sys
sys.path.append('../../_')
from cotedivoire import household_demographics

myvars = dict(fn='CotedIvoire/1985-86/Data/F01A.DAT',HHID='HID',
              age='AGEY',sex=('SEX',lambda s: 'm' if s==1 else 'f'),
              months_spent='MON')

z = household_demographics(**myvars)

z.columns.name = 'k'
z.index.name = 'j'

z.to_parquet('household_demographics.parquet')

