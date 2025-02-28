# Formatting  Functions for Ghana 2016-17
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict

def i(value):
    '''
    Formatting household id
    '''
    return tools.format_id(value[0])+'0'+tools.format_id(value[1],zeropadding=2)

def pid(value):
    '''
    Formatting person id
    '''
    return tools.format_id(value[0])+'0'+tools.format_id(value[1],zeropadding=2)+'0'+tools.format_id(value[2],zeropadding=2)

def Sex(value):
    '''
    Formatting sex veriable
    '''
    return value[0].upper()[0]

def Age(value):
    '''
    Formatting age variable
    '''
    if pd.isna(value):
        return np.nan
    else:
        return int(value)


def Relation(value):
    '''
    Formatting relationship variable
    '''
    return value.title()


