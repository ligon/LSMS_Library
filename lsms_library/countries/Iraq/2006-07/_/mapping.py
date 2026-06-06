# Formatting functions for Iraq 2006-07.
import pandas as pd


def Rural(value):
    '''Urban/Rural from the xstrat label (e.g. "baghdad - rural").

    Every stratum encodes urbanicity as a "... - rural" / "... - urban ..."
    suffix; this avoids duplicating the 50+-entry strata mapping the sample
    block uses. Constant within each cluster (verified).
    '''
    s = value.iloc[0]
    if pd.isna(s):
        return pd.NA
    return 'Rural' if 'rural' in str(s).lower() else 'Urban'
