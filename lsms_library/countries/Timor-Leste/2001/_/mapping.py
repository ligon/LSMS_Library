# Formatting functions for Timor-Leste 2001
import pandas as pd


def Int_t(value):
    """Parse the ``s00_vis1`` interview-date column.

    The 2001 cover-page file ``S00.DTA`` records the first-visit
    interview date as a six-digit integer encoded ``DDMMYY`` (e.g.
    ``260901`` = 26 September 2001).  Convert to a :class:`datetime.date`.

    NaN / non-finite values pass through as :class:`pandas.NaT`.
    """
    if pd.isna(value):
        return pd.NaT
    try:
        s = f"{int(value):06d}"
    except (TypeError, ValueError):
        return pd.NaT
    dd, mm, yy = int(s[:2]), int(s[2:4]), int(s[4:6])
    # 2001 wave: two-digit years <= 30 → 20xx, else 19xx (defensive;
    # in practice every observation has yy=01 or yy=02).
    year = 2000 + yy if yy <= 30 else 1900 + yy
    try:
        return pd.Timestamp(year=year, month=mm, day=dd).date()
    except (ValueError, OverflowError):
        return pd.NaT
