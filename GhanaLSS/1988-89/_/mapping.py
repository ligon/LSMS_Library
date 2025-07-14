# Formatting  Functions for Ghana 1988-89
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict
from importlib.resources import files

path = files('lsms_library')/'countries'/'GHANALSS'/'1988-89'
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
    return region_dict.get(value_key, np.nan)

def Relation(value):
    '''
    Formatting relationship variable
    '''
    relationship_dict = tools.get_categorical_mapping(tablename = 'relationship', dirs=[f'{path}/_', f'{path}/../../_/', f'{path}/../_/'])

    return relationship_dict.get(value, np.nan)

def Region(value):
    '''
    Formatting region variable
    '''

    try:
        value_key = int(value)
    except ValueError:
        value_key = None
    return region_dict.get(value_key, np.nan)

Visits = range(1,7)
