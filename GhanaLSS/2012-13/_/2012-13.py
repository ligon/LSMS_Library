# Formatting  Functions for Ghana 2016-17
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict

def i(value):
    '''
    Formatting household id
    '''
    return tools.format_id(value[0])+'/'+tools.format_id(value[1],zeropadding=2)


def Sex(value):
    '''
    Formatting sex veriable
    '''
    return value[0].upper()[0]

def Age(value):
    '''
    Formatting age variable
    '''
    return int(value)

def Birthplace(value):
    '''
    Formatting birthplace variable
    '''
    return value.title() if isinstance(value,str) else np.nan

def Relation(value):
    '''
    Formatting relationship variable
    '''
    return value.title()

Visits = range(1,7)