import pandas as pd
from lsms_library.local_tools import format_id


def i(value):
    """Format composite household id from ppid + hid."""
    parts = [str(int(value.iloc[k])) for k in range(len(value))]
    return format_id('-'.join(parts))


def Int_t(value):
    """Build interview date from (dayint, moint, yrint).

    yrint is a 2-digit year (95 -> 1995). Returns pd.NaT on any
    missing/invalid component.
    """
    d, m, y = value.iloc[0], value.iloc[1], value.iloc[2]
    if pd.isna(d) or pd.isna(m) or pd.isna(y):
        return pd.NaT
    y = int(y)
    y = y + 1900 if y < 100 else y
    try:
        return pd.Timestamp(year=y, month=int(m), day=int(d))
    except (ValueError, TypeError):
        return pd.NaT
