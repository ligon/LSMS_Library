# Formatting  Functions for Senegal 2018-19
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict

def v(value):
    '''
    Formatting cluster id
    '''
    id = value[0].astype(str) + value[1].astype(str)

    return tools.format_id(id)

def i(value):
    '''
    Formatting household id
    '''
    id = value[0].astype(str) + value[1].astype(str) + value[2].astype(str)

    return tools.format_id(id)

def Sex(value):
    '''
    Formatting sex veriable
    '''
    if value == 'Féminin':
        return 'f'
    if value == 'Masculin':
        return 'm'

def Age(value):
    '''
    Formatting birthplace variable
    '''
    
    value[2] = {'Janvier': 1, 'Février': 2, 'Mars': 3, 'Avril':4, 'Mai': 5, 'Juin': 6, 'Juillet': 7, 'Août': 8, 'Septembre': 9, 'Octobre': 10, 'Novembre': 11, 'Décembre': 12}.get(value[2])
    return list(value)

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
    if value:
        return str(value).title()

def Region(value):
    '''
    Formatting region variable
    '''
    return value

def Rural(value):
    '''
    Formatting rural variable
    '''
    return (value =='Rural') + 0.



def household_roster(df):
    '''
    Formatting dataframe to calculate ages
    '''

    df["Age"] = df.apply(lambda x: tools.age_handler(age = x["Age"][0], d = x["Age"][1], m = x["Age"][2], y = x["Age"][3], interview_date=x["interview_date"], interview_year=2018), axis = 1)
    df = df.drop('interview_date', axis = 'columns')
    
    return df