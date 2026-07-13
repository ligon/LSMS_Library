# Formatting functions for Niger 2014-15 (ECVMA2)
import sys
from pathlib import Path

# mapping.py is imported by the framework from an ARBITRARY cwd (importlib
# spec_from_file_location), so a relative '../../_/' does not resolve.  Anchor
# on __file__ instead: {Country}/{wave}/_/mapping.py -> parents[2] == {Country}.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / '_'))

import pandas as pd

# cluster_features is extracted from the household cover page (3617 rows / 270
# clusters) but declared at (t, v).  This wave is the one where the accidental
# groupby().first() collapse is actually WRONG: 2 clusters disagree on Region
# and 4 on District (enumerator typos), and first() resolves them by ROW ORDER.
# Collapse explicitly via the within-cluster majority, warning on each conflict
# (GH #323).
from niger import cluster_features_to_cluster_grain as cluster_features  # noqa: F401,E402


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
