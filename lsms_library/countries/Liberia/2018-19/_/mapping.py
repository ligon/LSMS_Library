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


def shocks(df):
    '''Keep only experienced shocks; drop the redundant Experienced flag.

    NHFS Section 17 enumerates all 14 shock types for every household
    (S17_1 = "severely negatively affected in the past 12 months", yes/no),
    producing a full household x shock-type cross-product (~40k rows, mostly
    not-experienced placeholders).  In the canonical (t, i, Shock) table a row
    exists only for a shock the household actually experienced, so filter to
    Experienced == True and drop the column: its information is carried by the
    row's existence, it would otherwise be a constant-True column, and as a
    bool it is silently nulled on the cached-read path (GH #386) -- which is
    what made this wave's row count collapse from cache.
    '''
    df = df[df['Experienced'] == True]
    return df.drop(columns='Experienced')
