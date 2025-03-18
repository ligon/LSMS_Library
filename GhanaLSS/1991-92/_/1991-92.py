# Formatting  Functions for Ghana 2016-17
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict

region_dict = tools.get_categorical_mapping(tablename = 'region', dirs=['../../_/', '../_/', './_/', '.'])
rural_dict = tools.get_categorical_mapping(tablename = 'rural', dirs=['1991-92/_/', '../../1991-92/_/', '../1991-92/_/', '.', './_/'])

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
        print('extremely large value warning?', value)
        return np.nan
    return (lambda x: region_dict[f"{x:3.0f}".strip()])(value)

def Relation(value):
    '''
    Formatting relationship variable
    '''
    relationship_dict = tools.get_categorical_mapping(tablename = 'relationship', dirs=['../../_/', '../_/', './_/', '.'])
    return relationship_dict.get(value, np.nan)

def Region(value):
    '''
    Formatting region variable
    '''

    return (lambda x: region_dict[f"{x:3.0f}".strip()])(value)
    

def Rural(value):
    '''
    Formatting rural variable
    '''

    return rural_dict.get(value, np.nan)

Visits = range(1,7)