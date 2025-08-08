# Formatting  Functions for Ghana 1998-99
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict
from importlib.resources import files

path = files('lsms_library')/'countries'/'GHANALSS'/'1998-99'
region_dict = tools.get_categorical_mapping(tablename = 'region', dirs=[f'{path}/_', f'{path}/../_/', f'{path}/../../_/'])
rural_dict = tools.get_categorical_mapping(tablename = 'rural', dirs=[f'{path}/_', f'{path}/../_/', f'{path}/../../_/'])

def i(value):
    '''
    Formatting household id
    '''
    return tools.format_id(value.iloc[0])+tools.format_id(value.iloc[1],zeropadding=2)

def Sex(value):
    '''
    Formatting sex veriable
    '''
    return (lambda s: 'MF'[int(s)-1])(value)

def Age(value):
    '''
    Formatting age variable
    '''
    return int(value)

def Birthplace(value):
    '''
    Formatting birthplace variable
    '''
    if value > 1e99:
        return np.nan
    return region_dict.get(int(value), np.nan)

def Relation(value):
    '''
    Formatting relationship variable
    '''
    relationship_dict = tools.get_categorical_mapping(tablename = 'relationship', dirs=[f'{path}/_', f'{path}/../_/', f'{path}/../../_/'])
    return relationship_dict.get(value, np.nan)

def Region(value):
    '''
    Formatting region variable
    '''

    return region_dict.get((int(value)), np.nan)
    

def Rural(value):
    '''
    Formatting rural variable
    '''

    return rural_dict.get(value)

Visits = range(1,7)
