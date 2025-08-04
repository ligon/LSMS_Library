# Formatting  Functions for Mali
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
    if pd.isna(value) or value == 'Manquant':
        return np.nan
    else:
        return value[0].upper()[0]

def Age(value):
    '''
    Formatting age variable
    '''
    if pd.isna(value) or value == 'Manquant' or value == 'NSP':
        return np.nan
    elif value =='95 ans & plus':
        return 95
    else:
        return int(value)

def Relation(value):
    '''
    Formatting relationship variable
    '''
    if pd.isna(value) or value == 'Manquant':
        return np.nan
    else:
        return value.title()

def Int_t(value):
    '''
    Formatting interview date
    '''   
    if pd.isna(value) or value == 'Manquant':
        return np.nan
    else:
        return pd.to_datetime(value, errors='coerce').date()
def interview_date(df):
    df  = pd.to_datetime(df.squeeze())
    return df