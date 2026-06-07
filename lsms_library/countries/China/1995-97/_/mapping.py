#!/usr/bin/env python
"""Formatting functions for China 1995-97 wave.

The household ID (hid) encodes geography:
  hid = village_code * 100 + household_number
  village_code = county(1 digit) + town(1 digit) + village(1 digit)
  e.g., hid=10101 -> village 101, household 01

Province mapping (from NPT0101.DTA community questionnaire):
  Counties 1-3 -> Province 7 (Hebei)
  Counties 4-6 -> Province 8 (Liaoning)
"""


def v(value):
    """Extract 3-digit village code (cluster/PSU) from household ID."""
    return int(value.iloc[0]) // 100


def Region(value):
    """Derive province from household ID.

    First digit of hid is the county (1-6).
    Counties 1-3 are in Province 7; Counties 4-6 are in Province 8.
    """
    county = int(value.iloc[0]) // 10000
    if county <= 3:
        return 7
    else:
        return 8


def PlotArea(x):
    """Clean S05B plot area (question 2, reported in mu).

    A handful of rows carry the Stata sentinel ~1.75e+100 (extreme
    encoding for a missing/refused value); coerce anything outside a
    plausible plot range to NA.
    """
    import pandas as pd
    s = pd.to_numeric(x, errors='coerce')
    return s.where((s > 0) & (s < 1000))


def AreaUnit(x):
    """S05B plot areas are reported in mu (the Chinese land unit)."""
    return 'mu'
