# Formatting  Functions for Ghana 1988-89
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict
from importlib.resources import files

path = files('lsms_library')/'countries'/'GhanaLSS'/'1988-89'
region_dict = tools.get_categorical_mapping(tablename = 'region', dirs=[f'{path}/_', f'{path}/../_/', f'{path}/../../_/'])

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
    relationship_dict = tools.get_categorical_mapping(tablename = 'relationship', dirs=[f'{path}/_', f'{path}/../../_/', f'{path}/../_/'])

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
