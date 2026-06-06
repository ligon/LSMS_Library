# Formatting functions for South Africa 1993.
import pandas as pd


def Int_t(value):
    '''Build interview date from (year1, month1, day1).

    year1 is a 2-digit offset from 1900 (e.g. 93 -> 1993); month1/day1 are
    numeric. Returns pd.NaT on any missing/invalid component.
    '''
    y, m, d = value.iloc[0], value.iloc[1], value.iloc[2]
    if pd.isna(y) or pd.isna(m) or pd.isna(d):
        return pd.NaT
    try:
        return pd.Timestamp(year=1900 + int(y), month=int(m), day=int(d))
    except (ValueError, TypeError):
        return pd.NaT
