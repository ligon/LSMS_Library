# Formatting functions for Liberia 2018-19
import pandas as pd
import numpy as np


def Age(value):
    '''
    Coerce age to numeric; non-numeric values (e.g. "don't know") become NaN.
    '''
    try:
        return float(value)
    except (ValueError, TypeError):
        return np.nan
