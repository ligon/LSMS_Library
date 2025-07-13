# Formatting  Functions for Ghana 1987-88
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict
from importlib.resources import files

path = files("lsms_library")/'countries'/'GhanaLSS'/'1987-88'
region_dict = tools.get_categorical_mapping(fn='categorical_mapping.org', tablename = 'region', dirs=[f'{path}/_/', f'{path}/../_', f'{path}/../../_'])

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
    relationship_dict = tools.get_categorical_mapping(fn='categorical_mapping.org', tablename = 'relationship', dirs=[f'{path}/_/', f'{path}/../_', f'{path}/../../_'])

    return relationship_dict.get(value, np.nan)

def Region(value):
    '''
    Formatting region variable
    '''

    return region_dict.get(str(value), np.nan)


def cluster_features(df):

    '''
    Formatting dataframe for cluster features
    
    infers the region for each cluster via where most young kids have their birthplace as (less likely to move?)
    '''

    youngsters = df.query("Age<12")
    foo = youngsters.reset_index().groupby(['t', 'v','Region']).count()

    foo = foo.sort_values(by = "Age", ascending=False).reset_index().drop_duplicates(subset=['t', 'v'], keep='first', inplace = False)
    foo = foo.sort_values(by = 'v')
    foo = foo.set_index(['t', 'v'])
    foo['Rural'] = np.nan

    return foo[['Region', 'Rural']]

Visits = range(1,7)
