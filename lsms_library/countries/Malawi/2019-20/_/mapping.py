#!/usr/bin/env python
import pandas as pd
import numpy as np
from lsms_library.local_tools import format_id

def cs_i(value):
    return 'cs-19-'+format_id(value.iloc[0])

# --- FAO FIES (food_security) -------------------------------------------
# HH_MOD_T carries the FAO Food Insecurity Experience Scale 8-item battery
# in vars hh_t13..hh_t20 (FAO order).  Value labels: {NO, YES, DON'T KNOW,
# REFUSED}.  Binarize yes->True, no->False; DON'T KNOW / refused / NaN -> NA.
# Recall period: last 12 months.

def _fies_yesno(x):
    """Map a single FIES item (string label) to a nullable boolean."""
    if pd.isna(x):
        return pd.NA
    s = str(x).strip().lower()
    if s in ('yes', '1', 'true', 'oui'):
        return True
    if s in ('no', '2', 'false', 'non'):
        return False
    # "DON'T KNOW", "refused", and anything unexpected -> missing
    return pd.NA

def fies_yesno(row):
    """Row-wise wrapper: data_info passes a single-column row (Series)."""
    return _fies_yesno(row.iloc[0])

def fies_score(row):
    """Count of True across the 8 FIES items in a row.

    NA only when every one of the 8 items is missing; otherwise NA items
    are treated as not-affirmed (count of True), matching the brief's
    'count of True across the 8 items' definition.
    """
    vals = [_fies_yesno(v) for v in row]
    if all(pd.isna(v) for v in vals):
        return pd.NA
    return int(sum(1 for v in vals if v is True))