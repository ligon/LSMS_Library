import pandas as pd


def District(x):
    """Coerce numeric District code (float-stringified from Stata) to int-string.

    h1aq1 in GSEC1.dta is stored as a numeric code (e.g. 101.0, 102.0).
    Without explicit coercion, df_data_grabber leaves myvars as float and the
    result stringifies to '101.0'.  This function strips the .0 suffix.
    """
    if pd.isna(x):
        return pd.NA
    try:
        return str(int(float(x)))
    except (ValueError, TypeError):
        return str(x)
