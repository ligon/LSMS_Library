# Formatting functions for Albania 2005.
import pandas as pd
import lsms_library.local_tools as tools


def Int_t(value):
    '''Parse m0_date (int YYYYMMDD) into a datetime.'''
    v = value.iloc[0]
    if pd.isna(v):
        return pd.NaT
    return pd.to_datetime(str(int(v)), format='%Y%m%d', errors='coerce')


def i(value):
    """Household id for 2005: PSU (m0_q00) - HH-within-PSU (m0_q01).

    Matches the canonical household identity built by this wave's
    sample.py (``format_id(m0_q00) + '-' + format_id(m0_q01)``), so the
    framework's _join_v_from_sample() resolves ``v`` correctly.
    """
    return tools.format_id(value.iloc[0]) + '-' + tools.format_id(value.iloc[1])
