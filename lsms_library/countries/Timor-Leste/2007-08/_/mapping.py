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


def _scalar(value):
    """Unwrap a length-1 Series row (df_data_grabber passes a row when the
    source is given as a single-element list) to its scalar value."""
    if isinstance(value, pd.Series):
        return value.iloc[0]
    return value


def AreaUnit(value):
    """Constant area unit for plot_features.

    The 2007-08 land module records plot area (``q09a04``) in square
    metres.  ``value`` (the area itself) is ignored; we return the
    constant unit string, with :class:`pandas.NA` where the area is
    missing so the unit isn't asserted for a plot with no recorded area.
    """
    if pd.isna(_scalar(value)):
        return pd.NA
    return 'square meters'


def Irrigated(value):
    """Map the ``q09a09`` irrigation-method label to a boolean.

    ``q09a09`` ("Irrigation of the plot") is a labelled categorical whose
    values are 'Not irrigated' or an irrigation source ('River', 'Spring',
    'Tube well', 'Ditch, canal', 'Pond, tank', 'Mixed', 'Other').
    'Not irrigated' -> ``False``; any irrigation source -> ``True``;
    missing -> :class:`pandas.NA`.
    """
    v = _scalar(value)
    if pd.isna(v):
        return pd.NA
    return str(v).strip().lower() != 'not irrigated'
