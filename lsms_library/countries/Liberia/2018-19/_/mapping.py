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


# Hectares per native plot-area unit (GH #879).  The canonical schema
# (lsms_library/data_info.yml) declares plot_features.Area in HECTARES,
# with AreaUnit the provenance label of the original unit.  NHFS Section
# 10 records parcel area (S10_4) with unit S10_4B in acres / lot(s) /
# hectares / other.  Acres and hectares convert exactly (1 acre =
# 0.404686 ha, the Nigeria/_/nigeria.py constant).  'lot(s)' is NOT a
# defined unit anywhere in the shipped documentation (the NHFS
# questionnaire gives no lot dimension) and 'other' names nothing, so
# those rows keep Area = NaN with the native unit still recorded in
# AreaUnit (honest, not fabricated).
_PLOT_AREA_HECTARES_PER_UNIT = {
    'acres': 0.404686,
    'hectares': 1.0,
}


def plot_features(df):
    """df_edit hook: convert ``Area`` to hectares per-row on ``AreaUnit``.

    Rows whose native unit has no factor in
    :data:`_PLOT_AREA_HECTARES_PER_UNIT` (lot(s), other, missing) get a
    NaN Area -- the schema's required column is in hectares and a guessed
    factor is worse than an honest gap (GH #879).  ``AreaUnit`` is left
    untouched: it is the provenance label of the unit the SURVEY
    recorded, not of the unit served.
    """
    df = df.copy()
    area = pd.to_numeric(df['Area'], errors='coerce')
    factor = df['AreaUnit'].astype(object).map(_PLOT_AREA_HECTARES_PER_UNIT)
    df['Area'] = area * pd.to_numeric(factor, errors='coerce')
    return df
