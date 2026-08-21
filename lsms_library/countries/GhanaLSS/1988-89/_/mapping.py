# Formatting  Functions for Ghana 1988-89
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict
from importlib.resources import files

path = files('lsms_library')/'countries'/'GhanaLSS'/'1988-89'
_dirs = [f'{path}/_', f'{path}/../_/', f'{path}/../../_/']

# A bare tools.get_categorical_mapping(tablename=...) returns an EMPTY dict --
# it passes no value column, so df_data_grabber drops the Label column and the
# squeeze yields {}.  Every lookup then misses and silently produces pd.NA,
# with nothing raised.  That is what left Region/Birthplace/Relationship 100%
# NULL in this wave (and, downstream, the Generation/Distance/Affinity kinship
# columns that _expand_kinship derives from Relationship).
# tools.code_label_map passes Label='Label' and keys the result by BOTH the
# string and the int form of each code.  See GH #372/#377/#348.
region_dict = tools.code_label_map('region', _dirs)
rural_dict = tools.code_label_map('rural', _dirs)
# GLSS2 uses the same 14-code relationship scheme as GLSS1 (verified:
# REL takes codes 1..14 and code 1 occurs 3192 times = the household count).
relationship_dict = tools.code_label_map('relationship', _dirs)

def i(value):
    '''
    Formatting household id
    '''
    return tools.format_id(value)

def Sex(value):
    '''
    Formatting sex veriable
    '''
    return (lambda s: 'MF'[s-1])(value)

def Age(value):
    '''
    Formatting age variable
    '''
    return int(value)

def Birthplace(value):
    '''
    Formatting birthplace variable
    '''

    try:
        value_key = int(value)
    except ValueError:
        value_key = None
    return region_dict.get(value_key, pd.NA)

def Relationship(value):
    '''
    Formatting relationship variable
    '''

    return relationship_dict.get(value, pd.NA)

def v(value):
    '''
    Formatting cluster variable
    '''
    return tools.format_id(value)

def Region(value):
    '''
    Formatting region variable
    '''

    try:
        value_key = int(value)
    except ValueError:
        value_key = None
    return region_dict.get(value_key, pd.NA)

def cluster_features(df):
    '''Collapse the person-grain roster frame to (t, v) cluster grain.

    Mirrors 1987-88/_/mapping.py::cluster_features -- GLSS1 and GLSS2 have NO
    cluster-location variable.  Both waves read `Region` for cluster_features
    and `Birthplace` for household_roster from the SAME source column,
    Y01A.DAT:REGION, which is the *person's region of birth*, not where the
    cluster is.  So the cluster's region is INFERRED as the modal birth region
    among household members under 12 (young children are least likely to have
    migrated).  This is a documented approximation, not a measured value.

    Without this, the framework collapses the person-grain frame with
    groupby().first(), which takes an ARBITRARY person's birth region as the
    whole cluster's region.  Measured on this wave that destroyed 14,559 of
    14,924 rows across 167 conflicting clusters (GrainCollapseWarning), i.e.
    the majority of clusters held members born in more than one region.
    '''

    youngsters = df.query("Age<12")
    foo = youngsters.reset_index().groupby(['t', 'v', 'Region']).count()

    foo = foo.sort_values(by="Age", ascending=False).reset_index().drop_duplicates(subset=['t', 'v'], keep='first', inplace=False)
    foo = foo.sort_values(by='v')
    foo = foo.set_index(['t', 'v'])
    foo['Rural'] = pd.NA

    return foo[['Region', 'Rural']]

def Int_t(value):
    '''
    Build interview date from first-visit (DAY1, MO1, YR1).  YR1 is a
    2-digit year (e.g. 88, 89) -> 1988, 1989.  .DAT columns may arrive
    as strings or ints.
    '''
    def _to_int(x):
        if pd.isna(x):
            return None
        try:
            return int(float(x))
        except (TypeError, ValueError):
            return None
    d, m, y = _to_int(value.iloc[0]), _to_int(value.iloc[1]), _to_int(value.iloc[2])
    if d is None or m is None or y is None or m < 1 or m > 12 or d < 1 or d > 31:
        return pd.NaT
    if y < 100:
        y += 1900
    return pd.to_datetime(f"{y}-{m}-{d}", format='%Y-%m-%d', errors='coerce')

Visits = range(1,7)
