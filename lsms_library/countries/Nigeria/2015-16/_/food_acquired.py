#!/usr/bin/env python
"""Extract raw food acquisition data for Nigeria 2015-16 (GHS-Panel W3).

Source files:
  - sect10b_harvestw3.csv  (post-harvest, 2016Q1)
  - sect7b_plantingw3.csv  (post-planting, 2015Q3)

W3 questionnaire -- same numbering as W2 (GH #591):
  q2a/q2b  total quantity consumed (7 days) + unit
  q3a/q3b  QUANTITY PURCHASED + unit                 <- q4 pays for this
  q4       amount spent on [ITEM] (Naira)
  q5a/q5b  consumption that came from PURCHASES + unit
  q6a/q6b  consumption that came from OWN PRODUCTION + unit
  q7a/q7b  consumption that came from GIFTS + unit
Identity q5a + q6a + q7a == q2a holds in ~100% of rows.

Until GH #591 this script mapped q5a -> `Produced` (see 2012-13 for the
mechanism); 2016Q1 kept 426 price rows out of a 138k-row food_acquired.

GH #1502 (ruling @ligon 2026-11-12, "Wire W2 and W3"): both rounds read the
shipped food_conv_w3.dta on the row's item code and its source's own unit code
(and, on code 900 'other specify', that source's own Other text, q3b_os /
q6b_os / q7b_os), at the household's OWN-ZONE column -- the value W4's per-row
*_cvn reproduces -- giving Quantity_kg on non-metric units
(nigeria.food_conversion_table).
"""
import sys

import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import to_parquet
from nigeria import food_acquired_for_wave

# --- Harvest (2016Q1) ---
harvest = food_acquired_for_wave(
    '../Data/sect10b_harvestw3.csv', '2016Q1',
    sources={
        'purchased': {'quantity': 's10bq3a', 'unit': 's10bq3b',
                      'other': 's10bq3b_os'},
        'produced':  {'quantity': 's10bq6a', 'unit': 's10bq6b',
                      'other': 's10bq6b_os'},
        'inkind':    {'quantity': 's10bq7a', 'unit': 's10bq7b',
                      'other': 's10bq7b_os'},
    },
    expenditure='s10bq4',
    conversion='../Data/food_conv_w3.dta')

# --- Planting (2015Q3) ---
planting = food_acquired_for_wave(
    '../Data/sect7b_plantingw3.csv', '2015Q3',
    sources={
        'purchased': {'quantity': 's7bq3a', 'unit': 's7bq3b',
                      'other': 's7bq3b_os'},
        'produced':  {'quantity': 's7bq6a', 'unit': 's7bq6b',
                      'other': 's7bq6b_os'},
        'inkind':    {'quantity': 's7bq7a', 'unit': 's7bq7b',
                      'other': 's7bq7b_os'},
    },
    expenditure='s7bq4',
    conversion='../Data/food_conv_w3.dta')

df = pd.concat([harvest, planting]).sort_index()

to_parquet(df, 'food_acquired.parquet')
