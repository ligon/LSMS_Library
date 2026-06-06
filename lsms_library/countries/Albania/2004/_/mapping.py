# Formatting functions for Albania 2004.
import pandas as pd


def Int_t(value):
    '''Parse m0_date (int YYYYMMDD, e.g. 20040622) into a datetime.'''
    v = value.iloc[0]
    if pd.isna(v):
        return pd.NaT
    return pd.to_datetime(str(int(v)), format='%Y%m%d', errors='coerce')
