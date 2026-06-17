# Formatting Functions for Guinea-Bissau
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools


def i(value):
    '''Formatting household id from (grappe, menage).'''
    if isinstance(value, (pd.Series, np.ndarray, list, tuple)):
        return tools.format_id(value.iloc[0]) + '0' + tools.format_id(value.iloc[1], zeropadding=2)
    return tools.format_id(value)


def interview_date(df):
    """Melt EHCVM per-visit interview start/end timestamps onto a `visit`
    index. q23/q24/q25 a/b = visit 1/2/3 start/end -> int_start/int_end[_v2/_v3].
    Delegates to local_tools.melt_visit_intervals -> 'Interview start' /
    'Interview end'; collapsing `visit` with `first` reproduces the legacy
    single-date table."""
    return tools.melt_visit_intervals(df)
