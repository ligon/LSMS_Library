import pandas as pd
from lsms_library.local_tools import format_id

def Int_t(value):
    '''
    Formatting interview date
    ''' 
    # date = f'{value[0]}-{value[1]}-{value[2]}'
    date = f'{int(value[0])}-{int(value[1])}-{int(value[2])}'
    return pd.to_datetime(date, format='%Y-%m-%d', errors='coerce').date()

def interview_date(df):
    df['visit'] = df.groupby(level='i')['Int_t'].rank(method='first').astype(int).astype(str)
    df = df.set_index('visit', append=True)
    df  = pd.to_datetime(df.squeeze())
    return df
