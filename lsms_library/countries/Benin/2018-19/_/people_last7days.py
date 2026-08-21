"""Build people_last7days for Benin EHCVM 2018-19 (GAP 3, item-level).

Two source files:
  s04_me_ben2018.dta — the 7-day employment module (participation dummies).
  s01_me_ben2018.dta — the individual roster (age for working_age).
Merged on (grappe, menage, s01q00a) — the same person key household_roster
uses.  Grain (t, i, pid); pid = s01q00a; ``i`` = Benin EHCVM composite via
``benin.i()`` (100% sample intersection verified).

EHCVM's 7-day module records only the three participation DUMMIES in an
ECVMA-comparable form (s04q06 farm, s04q07 own-business, s04q08 wage) and
working_age (roster Age >= 6).  It does NOT record productive-work hours or
the WB 7-day industry section code, so farm_hrs / SB_hrs / wage_hrs and
Industry are NA (declared for cross-wave schema parity).

This script is SELF-CONTAINED: the build (cloned from the Niger EHCVM
reference ``people_last7days_ehcvm`` / ``_finish_people_last7days``) is
inlined; only ``benin.i()`` is imported so the household id cannot drift from
``sample()``.
"""
import sys

sys.path.append('../../_/')
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet
from benin import i as benin_i


def _yn_bool(series, yes=1, no=2):
    """Map a 1=Oui / 2=Non survey item to nullable boolean (other codes ->
    NA)."""
    s = pd.to_numeric(series, errors='coerce')
    out = pd.Series(pd.NA, index=s.index, dtype='boolean')
    out = out.mask(s == yes, True)
    out = out.mask(s == no, False)
    return out


def people_last7days_ehcvm(s04, s01, t, pid_col='s01q00a', age_col='s01q04a',
                           dob_year_col='s01q03c', survey_year=2018):
    """Build people_last7days from the EHCVM 7-day labor module (s04) merged
    to the s01 roster for working_age.
      s04q06 -> farm_work    s04q07 -> SOB_work    s04q08 -> wage_work
    AGE: prefer reported years (age_col); else survey_year - birth_year
    (dob_year_col, out-of-range / sentinel -> NA), mirroring household_roster's
    age_handler.  working_age = Age >= 6.  pid = pid_col."""
    key = ['grappe', 'menage', pid_col]
    roster_cols = [age_col, dob_year_col]
    merged = s04[key + ['s04q06', 's04q07', 's04q08']].merge(
        s01[key + roster_cols], on=key, how='left')

    hh = merged.apply(lambda r: benin_i(pd.Series([r['grappe'], r['menage']])),
                      axis=1)
    pid = merged[pid_col].apply(tools.format_id)

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
    """Common tail: coerce dtypes, guarantee the full schema column set (NA
    where this wave lacks a field), drop rows with no individual key, and
    build the (t, i, pid) index."""
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
    # --- (t, i, pid) key soundness reviewed 2026-07-21 -- GH #637 ----------
    # This collapse is a NO-OP on the shipped Benin data: there is nothing to
    # collapse, so `.first()`'s per-column skipna=True can form no composite
    # row here.  Measured on the SOURCE directly (this table is
    # `materialize: make`, so it builds out-of-process and an in-process
    # groupby probe cannot see it -- see the #637 thread):
    #   s04_me_ben2018 rows 35,042; duplicate (grappe, menage, s01q00a)
    #     groups 0
    #   s01_me_ben2018 rows 42,343; duplicate person-key groups 0
    #     -> the how='left' merge fans out 0 rows (merged == 35,042)
    #   benin.i() is INJECTIVE: 8,012 distinct (grappe, menage) pairs in the
    #     full roster -> 8,012 distinct ids (menage runs to 562, so the
    #     zero-padded-3 form is exercised; no collision)
    #   groups on (t, i, pid) 35,042 == rows 35,042  ->  0 duplicate groups
    # So the key merges no distinct persons -- the ONLY situation in which the
    # composite would be wrong (GH #637 correction thread; GH #323 D1: the fix
    # for a merged entity is the identifier, never a reducer).
    # Do NOT "fix" this to `.first(skipna=False)`: in this codebase NaN is
    # absence, not contradiction, and skipna=False would return <NA> for
    # values the survey actually recorded.
    # `dropna=False` governs NA *group keys* only; rows with NA i/pid are
    # already dropped above and `t` is a literal, so it too is inert here.
    df = (df.groupby(['t', 'i', 'pid'], dropna=False, as_index=False).first())
    df = df.set_index(['t', 'i', 'pid'])
    return df


s04 = get_dataframe('../Data/s04_me_ben2018.dta', convert_categoricals=False)
s01 = get_dataframe('../Data/s01_me_ben2018.dta', convert_categoricals=False)

df = people_last7days_ehcvm(s04, s01, '2018-19', survey_year=2018)
df = _finish_people_last7days(df, '2018-19')

assert len(df) > 0, 'people_last7days 2018-19 produced no rows'
to_parquet(df, 'people_last7days.parquet')
