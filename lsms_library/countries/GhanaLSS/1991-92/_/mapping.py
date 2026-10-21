# Formatting  Functions for Ghana 1991-92
import pandas as pd
import numpy as np
import lsms_library.local_tools as tools
from collections import defaultdict
from lsms_library.paths import countries_root

path = countries_root()/'GhanaLSS'/'1991-92'

def _code_label_map(tablename, dirs):
    '''Read a Code -> Label org table into a {int code: label} dict.

    Delegates to ``tools.code_label_map`` (CONTENTS.org Trap 1: a bare
    ``get_categorical_mapping`` used to return an EMPTY dict; since GH #674 it
    raises TypeError), keeping this module's
    int-keyed contract: non-numeric codes (e.g. the country table's '.' None
    row) are skipped.  Used to be a private copy of the cascade that read only
    ``categorical_mapping.org`` at each rung, so it could not see a level's
    ``categorical_mapping/`` directory or the cross-country level (GH #953).
    '''
    out = {}
    for k, v in tools.code_label_map(tablename, dirs, dual_keys=False).items():
        try:
            out[int(k)] = v
        except (ValueError, TypeError):
            continue
    return out

# GLSS3 (1991-92) region codes live in the *wave-level* categorical_mapping.org
# (1991-92/_/), because GLSS3 orders the two northern regions 9=Upper West,
# 10=Upper East -- the reverse of the country-level table.  Searching the wave
# dir first picks up the GLSS3-correct list; rural falls through to whichever
# file defines it.  relationship is ALSO wave-level since 2026-09-07: GLSS3 uses
# GLSS4's nine-code scheme, not GLSS1's fourteen, and falling through to the
# country table mis-labelled codes 6-9 for 1,353 people (CONTENTS.org Trap 2).
# wave -> country; the cross-country level is appended by the resolver (GH #953).
_dirs = [f'{path}/_', f'{path}/../_/']
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


def individual_education(df):
    """GH #171: GLSS3 s2q2 blanks = never-attended (s2q1=2); null them so dropna
    removes them, consistent with the library's never-schooled handling."""
    col = "Educational Attainment"
    if col in df.columns:
        df = df.copy()
        blank = df[col].notna() & (df[col].astype(str).str.strip() == "")
        df.loc[blank, col] = pd.NA
    return df
