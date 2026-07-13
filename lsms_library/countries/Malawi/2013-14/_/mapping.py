#!/usr/bin/env python
import pandas as pd
import numpy as np

# --- food_security (8-item adult food-insecurity scale) -----------------
# HH_MOD_T_13 (IHPS 2013) carries an 8-item adult food-insecurity battery
# in hh_t13..hh_t20.  NOTE: this is NOT the standard FAO FIES wording/order
# used in IHS4/IHS5 (2016-17, 2019-20); the items differ in order and the
# "healthy diet" concept is captured by a lack-of-money item.  The mapping
# to the canonical FAO-FIES columns (best-fit by concept) is:
#   hh_t13 worried (would run out)        -> Worried
#   hh_t15 lacked money/resources for food-> HealthyDiet
#   hh_t16 diet based on only a few kinds -> FewFoods
#   hh_t17 did not eat breakfast/lunch/...-> SkippedMeal
#   hh_t18 ate less than you should       -> AteLess
#   hh_t14 ran out of food               -> RanOut
#   hh_t19 hungry but didn't eat          -> Hungry
#   hh_t20 ate only one meal / went a day -> WholeDay
# Value labels: {1: YES, 2: NO}.  Binarize yes->True, no->False; missing->NA.
# Recall period: last 12 months.

def _fies_yesno(x):
    """Map a single item (string label) to a nullable boolean."""
    if pd.isna(x):
        return pd.NA
    s = str(x).strip().lower()
    if s in ('yes', '1', 'true', 'oui'):
        return True
    if s in ('no', '2', 'false', 'non'):
        return False
    return pd.NA

def fies_yesno(row):
    """Row-wise wrapper: data_info passes a single-column row (Series)."""
    return _fies_yesno(row.iloc[0])

def fies_score(row):
    """Count of True across the 8 items in a row; NA only if all 8 missing."""
    vals = [_fies_yesno(v) for v in row]
    if all(pd.isna(v) for v in vals):
        return pd.NA
    return int(sum(1 for v in vals if v is True))

