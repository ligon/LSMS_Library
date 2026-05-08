# Formatting functions for Timor-Leste 2007-08
import pandas as pd


def Int_t(value):
    """Combine ``intday``, ``intmonth``, ``intyear`` into a date.

    The 2007-08 ``basicvars.dta`` records the interview date as three
    integer columns (intday 1-31, intmonth 1-12, intyear 2007-2008).
    Receives them as a length-3 :class:`pandas.Series` per row.

    NaN / out-of-range components → :class:`pandas.NaT`.
    """
    try:
        d = int(value.iloc[0])
        m = int(value.iloc[1])
        y = int(value.iloc[2])
    except (AttributeError, IndexError, TypeError, ValueError):
        return pd.NaT
    try:
        return pd.Timestamp(year=y, month=m, day=d).date()
    except (ValueError, OverflowError):
        return pd.NaT
