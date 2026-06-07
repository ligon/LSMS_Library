# Formatting functions for Niger 2014-15 (ECVMA2)
import pandas as pd


_FIES_ITEMS = ['Worried', 'HealthyDiet', 'FewFoods', 'SkippedMeal',
               'AteLess', 'RanOut', 'Hungry', 'WholeDay']


def _to_fies_bool(s):
    '''Coerce a FIES item column to nullable boolean.  The YAML
    ``mapping: {Oui: True, Non: False}`` runs through df_data_grabber,
    which leaves UNMAPPED values (NSP / Refus / NaN) UNCHANGED rather
    than turning them into NaN.  So we keep only genuine True/False and
    send everything else (residual strings, NaN) to pd.NA.'''
    return s.map(lambda x: True if x is True else (False if x is False else pd.NA)).astype('boolean')


def food_security(df):
    '''food_security post-processor: coerce the 8 FIES items to nullable
    boolean and compute FIES_score (count of True across the 8 items;
    NaN only when ALL 8 items are NaN).  See the food_security block in
    data_info.yml for the ECVMA2 MS14Q01..Q08 -> FAO item mapping.
    '''
    for c in _FIES_ITEMS:
        df[c] = _to_fies_bool(df[c])
    items = df[_FIES_ITEMS]
    all_na = items.isna().all(axis=1)
    score = items.eq(True).sum(axis=1)
    df['FIES_score'] = score.where(~all_na).astype('Int64')
    return df
