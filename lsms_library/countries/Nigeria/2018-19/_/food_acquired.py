#!/usr/bin/env python
"""Extract raw food acquisition data for Nigeria 2018-19 (GHS-Panel W4).

Source files:
  - sect10b_harvestw4.csv  (post-harvest, 2019Q1)
  - sect7b_plantingw4.csv  (post-planting, 2018Q3)

W4 questionnaire -- renumbered AGAIN (GH #591).  The per-source unit
columns are gone (q5a/q6a/q7a are "of [QTY] consumed, how much came
from ...", so all three are in the CONSUMPTION unit q2b), and the purchase
question moved into its own block with its own unit and kg factor:
  q2a/q2b/q2_cvn  total consumed (7 days) + unit + Kg/L factor
  q5a             ... of which came from PURCHASES   (unit q2b)
  q6a             ... of which came from OWN PRODUCTION (unit q2b)
  q7a             ... of which came from GIFTS/OTHER (unit q2b)
  q8              did the HH purchase any [ITEM] in the past 30 days?
  q9a/q9b/q9_cvn  quantity of the MOST RECENT PURCHASE + unit + Kg/L factor
  q10             amount spent on that purchase       <- pays for q9a

Two defects fixed here:
  1. q5a was mapped to `Produced`.  It is the quantity consumed out of
     PURCHASES; own production is q6a.  (Same inversion as W2/W3.)
  2. q10 (the value of the most recent purchase) was paired with the
     residual (q2a - q5a).  It pays for q9a, in unit q9b -- which differs
     from the consumption unit q2b in 13% (PH) / 10% (PP) of rows.

The purchased row therefore carries the PURCHASE TRANSACTION (q9a, q9b,
q9_cvn, q10): every stored number is survey-reported and
Expenditure/Quantity is a real price.  Validated against the independent
community_prices survey (median survey/community unit-value ratio 0.95 on
matched (t, j, u) cells; 93% within 25%).  Note the reference-period
asymmetry this leaves: the purchased row is a 30-day most-recent purchase
while produced / in-kind are 7-day consumption.  Imputing a 7-day
purchased value (q5a x price) was rejected -- it would fabricate a number
no respondent reported.  See nigeria.food_acquired_for_wave.
"""
import sys

import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import to_parquet
from nigeria import food_acquired_for_wave

# --- Harvest (2019Q1) ---
harvest = food_acquired_for_wave(
    '../Data/sect10b_harvestw4.csv', '2019Q1',
    sources={
        'purchased': {'quantity': 's10bq9a', 'unit': 's10bq9b',
                      'kg_factor': 's10bq9_cvn'},
        'produced':  {'quantity': 's10bq6a', 'unit': 's10bq2b',
                      'kg_factor': 's10bq2_cvn'},
        'inkind':    {'quantity': 's10bq7a', 'unit': 's10bq2b',
                      'kg_factor': 's10bq2_cvn'},
    },
    expenditure='s10bq10')

# --- Planting (2018Q3) ---
planting = food_acquired_for_wave(
    '../Data/sect7b_plantingw4.csv', '2018Q3',
    sources={
        'purchased': {'quantity': 's7bq9a', 'unit': 's7bq9b',
                      'kg_factor': 's7bq9_cvn'},
        'produced':  {'quantity': 's7bq6a', 'unit': 's7bq2b',
                      'kg_factor': 's7bq2_cvn'},
        'inkind':    {'quantity': 's7bq7a', 'unit': 's7bq2b',
                      'kg_factor': 's7bq2_cvn'},
    },
    expenditure='s7bq10')

df = pd.concat([harvest, planting]).sort_index()

to_parquet(df, 'food_acquired.parquet')
