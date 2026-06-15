"""Build people_last7days for CotedIvoire EHCVM 2018-19 (GAP 3, item-level).

Two source files (verified to match Niger's EHCVM 2018-19 layout):
  ../Data/Menage/s04_me_CIV2018.dta — the 7-day employment module
                                      (participation dummies s04q06/07/08).
  ../Data/Menage/s01_me_CIV2018.dta — the individual roster (age for
                                      working_age).
Merged on (grappe, menage, s01q00a) — the same person key household_roster
uses.  Grain (t, i, pid); pid = s01q00a.

EHCVM's 7-day module records only the three participation DUMMIES in an
ECVMA-comparable form (s04q06 farm, s04q07 own-business, s04q08 wage) and
working_age (roster Age >= 6).  It does NOT record productive-work hours or
the WB 7-day industry section code, so farm_hrs / SB_hrs / wage_hrs and
Industry are NA (declared for cross-wave schema parity).

i is CotedIvoire's EHCVM composite id (grappe + zero-padded(3) menage; NO
'E_' prefix — CotedIvoire predates the standard EHCVM list), inlined here so
this wave script is self-contained.  The EHCVM build is an inlined copy of
niger.py:people_last7days_ehcvm / _finish_people_last7days.
"""
import pandas as pd

from lsms_library.local_tools import get_dataframe, to_parquet, format_id


def _i(grappe, menage):
    g = format_id(grappe)
    m = format_id(menage, zeropadding=3)
    if g is None or m is None:
        return None
    return g + m


def _yn_bool(series, yes=1, no=2):
    """Map a 1=Oui / 2=Non item to nullable boolean (other codes -> NA)."""
    s = pd.to_numeric(series, errors='coerce')
    out = pd.Series(pd.NA, index=s.index, dtype='boolean')
    out = out.mask(s == yes, True)
    out = out.mask(s == no, False)
    return out


def people_last7days_ehcvm(s04, s01, t, pid_col='s01q00a', age_col='s01q04a',
                           dob_year_col='s01q03c', survey_year=None):
    """Build people_last7days for the EHCVM 2018-19 wave.  Inlined copy of
    niger.py:people_last7days_ehcvm: prefer reported years, else
    survey_year - birth_year; working_age = Age >= 6."""
    key = ['grappe', 'menage', pid_col]
    roster_cols = [age_col, dob_year_col]
    merged = s04[key + ['s04q06', 's04q07', 's04q08']].merge(
        s01[key + roster_cols], on=key, how='left')

    hh = merged.apply(lambda r: _i(r['grappe'], r['menage']), axis=1)
    pid = merged[pid_col].apply(format_id)

    age = pd.to_numeric(merged[age_col], errors='coerce')
    if survey_year is not None:
        birth = pd.to_numeric(merged[dob_year_col], errors='coerce')
        birth = birth.where((birth >= 1900) & (birth <= survey_year), pd.NA)
        age_from_dob = survey_year - birth
        age = age.where(age.notna(), age_from_dob)
    working_age = (age >= 6)
    working_age = working_age.where(age.notna(), pd.NA)

    df = pd.DataFrame({
        'i': hh.values,
        'pid': pid.values,
        'farm_work': _yn_bool(merged['s04q06']).values,
        'SOB_work': _yn_bool(merged['s04q07']).values,
        'wage_work': _yn_bool(merged['s04q08']).values,
        'working_age': working_age.values,
    })
    return df


def _finish_people_last7days(df, t):
    """Coerce dtypes, guarantee the full schema (NA where absent), drop rows
    with no individual key, build the (t, i, pid) index.  Inlined copy of
    niger.py:_finish_people_last7days."""
    df = df.copy()
    df['t'] = t
    df['pid'] = df['pid'].astype('string')
    for col in ['farm_work', 'SOB_work', 'wage_work', 'working_age']:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = df[col].astype('boolean')
    for col in ['farm_hrs', 'SB_hrs', 'wage_hrs']:
        if col not in df.columns:
            df[col] = pd.NA
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
    if 'Industry' not in df.columns:
        df['Industry'] = pd.NA
    df['Industry'] = df['Industry'].astype('string')
    df = df[df['i'].notna() & df['pid'].notna()]
    keep = ['t', 'i', 'pid', 'farm_work', 'SOB_work', 'wage_work',
            'farm_hrs', 'SB_hrs', 'wage_hrs', 'Industry', 'working_age']
    df = df[keep]
    df = (df.groupby(['t', 'i', 'pid'], dropna=False, as_index=False).first())
    df = df.set_index(['t', 'i', 'pid'])
    return df


s04 = get_dataframe('../Data/Menage/s04_me_CIV2018.dta', convert_categoricals=False)
s01 = get_dataframe('../Data/Menage/s01_me_CIV2018.dta', convert_categoricals=False)

df = people_last7days_ehcvm(s04, s01, '2018-19', survey_year=2018)
df = _finish_people_last7days(df, '2018-19')

assert len(df) > 0, 'people_last7days 2018-19 produced no rows'
to_parquet(df, 'people_last7days.parquet')
