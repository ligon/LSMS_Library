#!/usr/bin/env python
"""Extract raw food acquisition data for Nigeria 2010-11 (GHS-Panel W1).

Item-level food consumption decomposed by acquisition source, before any
unit conversion.

Source files:
  - sect10b_harvestw1.csv  (post-harvest, 2011Q1)
  - sect7b_plantingw1.csv  (post-planting, 2010Q3)

W1 questionnaire:
  q2a/q2b  total quantity consumed (7 days) + unit
  q3a/q3b  ... of which came from PURCHASES + unit   <- q4 pays for this
  q4       amount spent on [ITEM] (Naira)
  q5a/q5b  ... of which came from OWN PRODUCTION + unit
  q6a/q6b  ... of which came from GIFTS + unit
Identity q3a + q5a + q6a == q2a holds in 92% (PH) / 98% (PP) of rows.

Before GH #591 this script built the purchased row as the residual
(q2a - q5a), which silently folded GIFTS (q6a) into `purchased` and used
the residual -- not the survey's own q3a -- as the price denominator.  We
now take each source's REPORTED quantity and REPORTED unit.  See
nigeria.food_acquired_for_wave for the wave-by-wave questionnaire
renumbering that made W2-W4 catastrophically wrong.

GH #1035 (amended ruling @ligon 2026-10-19): the post-harvest round's sized
unit codes come from the agriculture card, which states each container's
weight.  A row recording a FRACTION of a sized mass container
(0 < Quantity < 1) is served the stated kilograms as `Quantity_kg`, stamped
`Nigeria::food_acquired::w1-card-weight`; every other row keeps
`Quantity_kg` NA and the price-ratio inference.  See mapping.py.
"""
import sys

import pandas as pd

sys.path.append('../../_/')
from lsms_library.local_tools import to_parquet
from nigeria import food_acquired_for_wave
from mapping import stamp_w1_card_weight

# --- Harvest (2011Q1) ---
# The source path and variables stay LITERALS here: the cache hash folds the
# DVC fingerprint of every data file a script names as a string literal.
# mapping.W1_PH_* repeats them for the derivation's `inputs` callable, and
# tests/test_gh1035_nigeria_w1_stated_weights.py keeps the two equal.  The
# stated container weight is attached AFTER the build's duplicate-key
# collapse, so the rule reads the served Quantity (GH #1035).
harvest = stamp_w1_card_weight(food_acquired_for_wave(
    '../Data/sect10b_harvestw1.csv', '2011Q1',
    sources={
        'purchased': {'quantity': 's10bq3a', 'unit': 's10bq3b'},
        'produced':  {'quantity': 's10bq5a', 'unit': 's10bq5b'},
        'inkind':    {'quantity': 's10bq6a', 'unit': 's10bq6b'},
    },
    expenditure='s10bq4'))

# --- Planting (2010Q3) ---
planting = food_acquired_for_wave(
    '../Data/sect7b_plantingw1.csv', '2010Q3',
    sources={
        'purchased': {'quantity': 's7bq3a', 'unit': 's7bq3b'},
        'produced':  {'quantity': 's7bq5a', 'unit': 's7bq5b'},
        'inkind':    {'quantity': 's7bq6a', 'unit': 's7bq6b'},
    },
    expenditure='s7bq4')

df = pd.concat([harvest, planting]).sort_index()

to_parquet(df, 'food_acquired.parquet')
