# Formatting  Functions for Ghana 2016-17
import pandas as pd
from lsms_library.local_tools import format_id


def strata(value):
    return format_id(value)

def Int_t(value):
    '''
    Formatting interview date
    ''' 
    # date = f'{value[0]}-{value[1]}-{value[2]}'
    date = f'{int(value.iloc[0])}-{int(value.iloc[1])}-{int(value.iloc[2])}'
    return pd.to_datetime(date, format='%Y-%m-%d', errors='coerce').date()