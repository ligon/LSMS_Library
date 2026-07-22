# Formatting  Functions for Ghana 1987-88
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict
from importlib.resources import files

path = files("lsms_library")/'countries'/'GhanaLSS'/'1987-88'
region_dict = tools.get_categorical_mapping(fn='categorical_mapping.org', tablename = 'region', dirs=[f'{path}/_/', f'{path}/../_', f'{path}/../../_'])

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
    return int(value) if value.isdigit() else pd.NA

def Birthplace(value):
    '''
    Formatting birthplace variable
    '''
    #needs mapping
    return region_dict.get(str(value), pd.NA)

def Relationship(value):
    '''
    Formatting relationship variable
    '''
    #needs mapping
    relationship_dict = tools.get_categorical_mapping(fn='categorical_mapping.org', tablename = 'relationship', dirs=[f'{path}/_/', f'{path}/../_', f'{path}/../../_'])

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

    return region_dict.get(str(value), pd.NA)


def cluster_features(df):

    '''
    Formatting dataframe for cluster features

    infers the region for each cluster via where most young kids have their birthplace as (less likely to move?)

    GH #323 -- DEAD CODE, AND A LANDMINE.  ``country.py`` dispatches this as the
    ``df_edit`` for the ``cluster_features`` table, but this wave no longer
    declares ``cluster_features`` (see ``data_info.yml``), so nothing calls it.
    Do NOT re-add that block: the "modal birthplace of a cluster's under-12s"
    heuristic below is a GUESS at the cluster's location, and it fails exactly
    where it matters (migrant-receiving clusters -- Greater Accra, mining/cocoa
    areas).  It is also silent about failing: ``region_dict`` is currently ``{}``
    for this wave, so ``Region`` is all-NA, the ``groupby(['t','v','Region'])``
    below drops every row, and the table builds as (0 rows, 2 cols).  Repair
    ``region_dict`` and this starts emitting fabricated cluster regions.
    '''

    youngsters = df.query("Age<12")
    foo = youngsters.reset_index().groupby(['t', 'v','Region']).count()

    foo = foo.sort_values(by = "Age", ascending=False).reset_index().drop_duplicates(subset=['t', 'v'], keep='first', inplace = False)
    foo = foo.sort_values(by = 'v')
    foo = foo.set_index(['t', 'v'])
    foo['Rural'] = pd.NA

    return foo[['Region', 'Rural']]

def Int_t(value):
    '''
    Build interview date from first-visit (DAY1, MO1, YR1).  YR1 is a
    2-digit year (e.g. 87, 88) -> 1987, 1988.  .DAT columns may arrive
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
