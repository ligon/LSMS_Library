# Formatting  Functions for Ghana 2016-17
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict

region_dict = tools.get_categorical_mapping(tablename = 'region', dirs=['../../_/', '../_/', './_/'])

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
    return region_dict.get(str(value), np.nan)

def Relation(value):
    '''
    Formatting relationship variable
    '''
    #needs mapping
    relationship_dict = tools.get_categorical_mapping(tablename = 'relationship', dirs=['../../_/', '../_/', './_/'])

    return relationship_dict.get(value, np.nan)

def Region(value):
    '''
    Formatting region variable
    '''

    return region_dict.get(str(value), np.nan)

Visits = range(1,7)