import pandas as pd

from lsms_library.local_tools import format_id


def i(value):
    """Format composite household id from popkrug + naselje + dom."""
    parts = [str(int(value.iloc[k])) for k in range(len(value))]
    return format_id('-'.join(parts))


def to_float(value):
    """Coerce a scalar to float (s30 Value loads as object dtype)."""
    return pd.to_numeric(value, errors='coerce')


def Int_t(value):
    """Build interview date from (dana, mesa, goda) = (day, month, year)."""
    d, m, y = value.iloc[0], value.iloc[1], value.iloc[2]
    if pd.isna(d) or pd.isna(m) or pd.isna(y):
        return pd.NaT
    try:
        return pd.Timestamp(year=int(y), month=int(m), day=int(d))
    except (ValueError, TypeError):
        return pd.NaT
