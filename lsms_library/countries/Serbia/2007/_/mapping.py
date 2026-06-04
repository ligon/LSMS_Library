import pandas as pd

from lsms_library.local_tools import format_id


def i(value):
    """Format composite household id from popkrug + naselje + dom."""
    parts = [str(int(value.iloc[k])) for k in range(len(value))]
    return format_id('-'.join(parts))


def to_float(value):
    """Coerce a scalar to float (s30 Value loads as object dtype)."""
    return pd.to_numeric(value, errors='coerce')
