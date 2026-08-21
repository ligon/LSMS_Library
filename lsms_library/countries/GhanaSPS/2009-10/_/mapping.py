# Formatting functions for GhanaSPS 2009-10 (Wave 1).
import pandas as pd
import lsms_library.local_tools as tools


def v(value):
    '''Cluster (enumeration area) id, zero-padded to 3 digits.

    Wave 1 carries the EA directly as `id3` in key_hhld_info.dta: 334 distinct
    values in 1..342, i.e. a GLOBALLY unique EA number, not an EA-within-
    district counter.  Do NOT build a composite with id1/id2: EA 183 spans two
    district codes (12 households under id2=1 and 3 under id2=94), so
    (id1, id2, id3) yields 335 groups and splits one real EA in two.

    Zero-padded so wave 1 agrees with waves 2 and 3, which recover the EA as a
    3-character slice of the household id.
    '''
    if pd.isna(value):
        return pd.NA
    return str(int(float(value))).zfill(3)


def Rural(value):
    '''urbrur -> canonical Urban / Rural (2,010 / 2,999 households).

    Accepts either form: key_hhld_info.dta carries urbrur as a LABELLED Stata
    categorical, so get_dataframe's convert_categoricals returns the string
    'Urban'/'Rural' rather than the underlying 1/2.  Handle both, since a
    re-export without value labels would silently flip it to codes.
    '''
    if pd.isna(value):
        return pd.NA
    s = str(value).strip()
    if s in ('Urban', 'Rural'):
        return s
    try:
        return {1: 'Urban', 2: 'Rural'}.get(int(float(s)), pd.NA)
    except (TypeError, ValueError):
        return pd.NA


def strata(value):
    '''loc7 -- the survey's 7-way locality stratification.  Kept as a code:
    no label table for it ships with the data.'''
    return tools.format_id(value)


def household_roster(df):
    '''Table-level ``df_edit`` hook: read a blank Q27 as "not away".

    ``s1d_27`` ("months away in the last 12") is populated for only 52.7% of
    members, and the blanks are a RECORDING CONVENTION rather than a person
    attribute -- see the long note in this wave's ``data_info.yml``.  The
    short version: Q27-Q33 is a skip block (of the 8,933 members missing
    s1d_27, just 9 have any of Q28-Q33), and per-EA answer rates are bimodal,
    with the share of recorded values that are NON-ZERO falling monotonically
    from 95.9% in the EAs that recorded almost nobody to ~6% in the EAs that
    recorded everybody.  Teams that wrote down few people wrote down the AWAY
    ones; teams that wrote down everybody recorded the zeros too.

    Without this fill, ``roster_to_characteristics()`` drops every NaN member
    and 2,192 of 5,009 wave-1 households (43.8%) vanish from the derived
    ``household_characteristics``.

    The single out-of-range value (15 months in a 12-month window, hhno
    105144071/hhmid 2) is clipped to 12 rather than guessed at.  Left alone it
    would yield clip(12 - 15, 0, None) = 0 months present and drop the person
    silently.
    '''
    if 'MonthsAway' not in df.columns:
        return df
    months = pd.to_numeric(df['MonthsAway'], errors='coerce')
    df = df.copy()
    df['MonthsAway'] = months.fillna(0).clip(upper=12)
    return df
