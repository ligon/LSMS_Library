# Formatting  Functions for Mali
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict

def i(value):
    '''
    Formatting household id
    '''
    return tools.format_id(value.iloc[0])+'0'+tools.format_id(value.iloc[1],zeropadding=2)

def pid(value):
    '''
    Formatting person id
    '''
    return tools.format_id(value.iloc[0])+'0'+tools.format_id(value.iloc[1],zeropadding=2)+'0'+tools.format_id(value.iloc[2],zeropadding=2)

def Sex(value):
    '''
    Formatting sex variable
    '''
    if pd.isna(value) or value == 'Manquant':
        return pd.NA
    else:
        return str(value).upper()[0]

def Age(value):
    '''
    Formatting age variable
    '''
    if pd.isna(value) or value == 'Manquant' or value == 'NSP':
        return pd.NA
    elif value =='95 ans & plus':
        return 95
    else:
        return int(value)

def Relationship(value):
    '''
    Formatting relationship variable
    '''
    if pd.isna(value) or value == 'Manquant':
        return pd.NA
    else:
        return value.title()

def Int_t(value):
    '''
    Formatting interview date
    '''   
    if pd.isna(value) or value == 'Manquant':
        return pd.NA
    else:
        return pd.to_datetime(value, errors='coerce').date()
def interview_date(df):
    df['Int_t'] = pd.to_datetime(df['Int_t'])
    return df