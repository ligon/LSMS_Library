import pandas as pd
from lsms_library.local_tools import format_id

# NOTE (2026-05-05, GH #169 Phase 2B): food_acquired canonical reshape NOT
# applied to this wave.  The data_info.yml mapping `Quantity: s13q02`
# is wrong — s13q02 is a Oui/Non (yes/no) column, not a quantity, so
# the canonical reshape (which expects numeric Quantity / Produced /
# Expenditure) raises a TypeError on string subtraction.  Until a Phase
# 3 audit identifies the correct quantity / expenditure / produced
# columns for the EACI 2014-15 instrument, this wave continues to emit
# the legacy non-canonical shape (mixed with canonical 2018-19 / 2021-22
# at the Country API level — known Phase-2-incomplete state).

def Int_t(value):
    '''
    Formatting interview date
    ''' 
    # date = f'{value[0]}-{value[1]}-{value[2]}'
    date = f'{int(value.iloc[0])}-{int(value.iloc[1])}-{int(value.iloc[2])}'
    return pd.to_datetime(date, format='%Y-%m-%d', errors='coerce').date()

def interview_date(df):
    df['visit'] = df.groupby(level='i')['Int_t'].rank(method='first').astype(int).astype(str)
    df = df.set_index('visit', append=True)
    df['Int_t'] = pd.to_datetime(df['Int_t'])
    return df
