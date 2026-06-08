# Formatting  Functions for Ghana 1991-92
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict
from importlib.resources import files

path = files('lsms_library')/'countries'/'GhanaLSS'/'1991-92'

def _code_label_map(tablename, dirs):
    '''Read a Code -> Label org table into a {int code: label} dict.

    ``tools.get_categorical_mapping`` builds its result from ``idxvars='Code'``
    with no myvars, so the framework's ``df_data_grabber`` drops every value
    column and the squeeze returns an EMPTY dict (the GH #377 / #372 root
    cause: region_dict resolved to {} and every Region/Birthplace came out
    NA).  Here we pass ``Label`` as a myvar so the value column survives, then
    return a code->label mapping keyed on the integer Code.
    '''
    def _as_int(k):
        # df_data_grabber stringifies the Code index via format_id; coerce back
        # to int so lookups can use int(value).  Non-numeric codes (e.g. the
        # country table's '.' None row) are skipped rather than aborting.
        try:
            return int(k)
        except (ValueError, TypeError):
            return None

    for d in dirs:
        if d[-1] != '/':
            d += '/'
        try:
            df = tools.df_data_grabber(d + 'categorical_mapping.org', 'Code',
                                       orgtbl=tablename, Label='Label')
            s = df['Label']
            out = {}
            for k, v in s.to_dict().items():
                code = _as_int(k)
                if code is not None and pd.notna(v):
                    out[code] = v
            return out
        except (FileNotFoundError, KeyError, ValueError):
            continue
    return {}

# GLSS3 (1991-92) region codes live in the *wave-level* categorical_mapping.org
# (1991-92/_/), because GLSS3 orders the two northern regions 9=Upper West,
# 10=Upper East -- the reverse of the country-level table.  Searching the wave
# dir first picks up the GLSS3-correct list; rural/relationship fall through to
# whichever file defines them.
_dirs = [f'{path}/_', f'{path}/../_/', f'{path}/../../_/']
region_dict = _code_label_map('region', _dirs)
rural_dict = _code_label_map('rural', _dirs)
relationship_dict = _code_label_map('relationship', _dirs)

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
    Formatting birthplace variable (s1q10, region codes 1-11).
    '''
    if pd.isna(value) or value > 1e99:
        return pd.NA
    return region_dict.get(int(value), pd.NA)

def Relationship(value):
    '''
    Formatting relationship variable
    '''
    if pd.isna(value) or value > 1e99:
        return pd.NA
    return relationship_dict.get(int(value), pd.NA)

def Region(value):
    '''
    Formatting region variable (POV_GH.region, codes 1-10).
    '''
    if pd.isna(value) or value > 1e99:
        return pd.NA
    return region_dict.get(int(value), pd.NA)


def v(value):
    '''
    Formatting cluster variable
    '''
    return tools.format_id(value)

def strata(value):
    '''
    Formatting strata variable (region code to label)
    '''
    if pd.isna(value) or value > 1e99:
        return pd.NA
    return region_dict.get(int(value), pd.NA)

def Rural(value):
    '''
    Formatting rural variable
    '''
    if pd.isna(value) or value > 1e99:
        return pd.NA
    return rural_dict.get(int(value), pd.NA)

def Int_t(value):
    '''
    Build interview date from (dd, mm, yy).  yy is a 2-digit year
    (e.g. 91, 92) -> 1991, 1992.
    '''
    d, m, y = value.iloc[0], value.iloc[1], value.iloc[2]
    if pd.isna(d) or pd.isna(m) or pd.isna(y):
        return pd.NaT
    y = int(y)
    if y < 100:
        y += 1900
    s = f"{y}-{int(m)}-{int(d)}"
    return pd.to_datetime(s, format='%Y-%m-%d', errors='coerce')

def Rooms(value):
    '''
    Number of rooms.  This wave uses a large float sentinel (~1.75e100) for
    missing/not-applicable; null it and coerce the rest to integer.
    '''
    if pd.isna(value) or value > 1e99:
        return pd.NA
    return int(value)

Visits = range(1,7)
