# Formatting Functions for CotedIvoire
import numpy as np
import pandas as pd
import lsms_library.local_tools as tools


def i(value):
    '''
    Formatting household id: concatenate grappe + zero-padded menage.
    Compound key [grappe, menage] -> string like "1001001".
    '''
    return tools.format_id(value.iloc[0]) + tools.format_id(value.iloc[1], zeropadding=3)


# GH #1562: the 2018-19 shocks hook and its coping labels moved to
# 2018-19/_/mapping.py, so an edit to them re-warms that wave alone, not every
# CotedIvoire table (this module is in every table's input hash).  2021-22 has
# its own hook.


def interview_date(df):
    """Melt EHCVM per-visit interview start/end timestamps onto a `visit`
    index. q23/q24/q25 a/b = visit 1/2/3 start/end -> int_start/int_end[_v2/_v3].
    Delegates to local_tools.melt_visit_intervals -> 'Interview start' /
    'Interview end'; collapsing `visit` with `first` reproduces the legacy
    single-date table."""
    return tools.melt_visit_intervals(df)
