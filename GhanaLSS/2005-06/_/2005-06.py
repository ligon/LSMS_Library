# Formatting  Functions for Ghana 2005-06
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict

def i(value):
    '''
    Formatting household id
    '''
    return tools.format_id(value)

def Sex(value):
    '''
    Formatting sex veriable
    '''
    return value.upper()[0]

def Age(value):
    '''
    Formatting age variable
    '''
    return int(value)

def Birthplace(value):
    '''
    Formatting birthplace variable
    '''
    if isinstance(value, float) and np.isnan(value):
        return np.nan
    else:
        return value.title()
    
def Relation(value):
    '''
    Formatting relationship variable
    '''

    return value.title()

def Region(value):
    '''
    Formatting region variable
    '''

    return value.title()

def Rural(value):
    '''
    Formatting rural variable
    '''

    return value.title()

Visits = range(1,7)