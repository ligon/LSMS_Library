# Formatting  Functions for Ghana 1998-99
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict
from importlib.resources import files

path = files('lsms_library')/'countries'/'GhanaLSS'/'1998-99'
_dirs = [f'{path}/_', f'{path}/../_/', f'{path}/../../_/']

# A bare tools.get_categorical_mapping(tablename=...) returns an EMPTY dict --
# it passes no value column, so df_data_grabber drops the Label column and the
# squeeze yields {}.  Every lookup then misses and silently produces pd.NA.
# That is what left GLSS4 Region/Rural 100% NULL (and cluster_features with
# both of its columns dead).  Use the shared helper, which passes Label='Label'
# and keys the result by BOTH the string and the int form of each code.
# See lsms_library/local_tools.py::code_label_map and GH #372/#377/#348.
region_dict = tools.code_label_map('region', _dirs)
rural_dict = tools.code_label_map('rural', _dirs)

def i(value):
    '''
    Formatting household id
    '''
    return tools.format_id(value.iloc[0])+tools.format_id(value.iloc[1],zeropadding=2)

def Sex(value):
    '''
    Formatting sex veriable
    '''
    return (lambda s: 'MF'[int(s)-1])(value)

def Age(value):
    '''
    Formatting age variable
    '''
    return int(value)

def Birthplace(value):
    '''
    Formatting birthplace variable
    '''
    if value > 1e99:
        return pd.NA
    return region_dict.get(int(value), pd.NA)

# GLSS4's `rel` (SEC1.DTA) uses a NINE-code scheme, NOT the country-level
# GLSS1 FOURTEEN-code table.  Decoded from the GLSS4 Household Questionnaire
# p.7 col.3 via the wave-level `relationship` table; see the note there.
# Wave dir FIRST, so this can never fall through to the country-level GLSS1
# 14-code table.  See the long note on 1998-99/_/categorical_mapping.org.
_relationship_dict = tools.code_label_map('relationship', [f'{path}/_'])

def Relationship(value):
    '''
    Formatting relationship variable (GLSS4 9-code scheme).
    '''
    return _relationship_dict.get(value, pd.NA)

def Region(value):
    '''
    Formatting region variable
    '''

    return region_dict.get((int(value)), pd.NA)
    

def v(value):
    '''
    Formatting cluster variable
    '''
    return tools.format_id(value)

def strata(value):
    '''
    Formatting strata variable (region code to label)
    '''
    return region_dict.get(int(value), pd.NA)

def Rural(value):
    '''
    Formatting rural variable
    '''

    return rural_dict.get(value, pd.NA)

def Int_t(value):
    '''
    Build interview date from (dd, mm, yy).  yy is a 2-digit year
    (e.g. 98, 99) -> 1998, 1999.
    '''
    d, m, y = value.iloc[0], value.iloc[1], value.iloc[2]
    if pd.isna(d) or pd.isna(m) or pd.isna(y):
        return pd.NaT
    y = int(y)
    if y < 100:
        y += 1900
    s = f"{y}-{int(m)}-{int(d)}"
    return pd.to_datetime(s, format='%Y-%m-%d', errors='coerce')

Visits = range(1,7)
