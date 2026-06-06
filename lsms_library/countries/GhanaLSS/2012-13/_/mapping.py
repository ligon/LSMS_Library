# Formatting Functions for Ghana 2012-13
import pandas as pd
import lsms_library.local_tools as tools


def v(value):
    '''
    Formatting cluster variable
    '''
    return tools.format_id(value)


def Int_t(value):
    '''
    Build interview date from numeric (ddate, mdate, ydate).
    '''
    d, m, y = value.iloc[0], value.iloc[1], value.iloc[2]
    if pd.isna(d) or pd.isna(m) or pd.isna(y):
        return pd.NaT
    s = f"{int(y)}-{int(m)}-{int(d)}"
    return pd.to_datetime(s, format='%Y-%m-%d', errors='coerce')
