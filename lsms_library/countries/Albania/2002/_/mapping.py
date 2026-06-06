# Formatting functions for Albania 2002.
import pandas as pd
import lsms_library.local_tools as tools


def i(value):
    """Household id = PSU (psu) - HH-within-PSU (hh).

    `hh` alone is only the within-cluster number (14 unique); the canonical
    household identity (matching sample()'s `1-2` form) is format_id(psu) +
    '-' + format_id(hh), so _join_v_from_sample resolves v correctly.
    """
    return tools.format_id(value.iloc[0]) + '-' + tools.format_id(value.iloc[1])


def Int_t(value):
    '''Parse m0_date (int YYYYMMDD, e.g. 20020717) into a datetime.'''
    v = value.iloc[0]
    if pd.isna(v):
        return pd.NaT
    return pd.to_datetime(str(int(v)), format='%Y%m%d', errors='coerce')
