# Formatting Functions for Ghana 2016-17
import pandas as pd
import lsms_library.local_tools as tools


def v(value):
    '''
    Formatting cluster variable
    '''
    return tools.format_id(value)


def Int_t(value):
    '''
    Build interview date from (ddate, mdate, ydate).

    mdate is a month *name* (e.g. "October"); ddate and ydate are numeric.
    '''
    d, m, y = value.iloc[0], value.iloc[1], value.iloc[2]
    if pd.isna(d) or pd.isna(m) or pd.isna(y):
        return pd.NaT
    s = f"{int(y)}-{str(m).strip()}-{int(d)}"
    return pd.to_datetime(s, errors='coerce')
