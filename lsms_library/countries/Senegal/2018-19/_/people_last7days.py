"""Build people_last7days for Senegal EHCVM 2018-19 (GAP 3, item-level).

Cloned from the Niger EHCVM template (Niger/2018-19/_/people_last7days.py).
Two source files:
  s04_me_sen2018.dta — the 7-day employment module (participation dummies).
  s01_me_sen2018.dta — the individual roster (age for working_age).
Merged on (grappe, menage, s01q00a) — the same person key household_roster
uses.  Grain (t, i, pid); pid = s01q00a.

EHCVM's 7-day module records only the three participation DUMMIES in an
ECVMA-comparable form (s04q06 farm, s04q07 own-business, s04q08 wage) and
working_age (roster Age >= 6).  It does NOT record productive-work hours or
the WB 7-day industry section code, so farm_hrs / SB_hrs / wage_hrs and
Industry are NA (declared for cross-wave schema parity).

This script is SELF-CONTAINED — it inlines the Senegal household-id
formatter (matching sample() / livestock.py: grappe + '0' + zero-padded
menage, NO 'E_' prefix), the Oui/Non -> bool mapper, and the finishing
tail.  AGE: prefer reported years (s01q04a), else survey_year (2018) -
birth_year (s01q03c; 9999 / out-of-range sentinel -> NA), mirroring
household_roster's age_handler.  working_age = Age >= 6.
"""
import pandas as pd

import lsms_library.local_tools as tools
from lsms_library.local_tools import get_dataframe, to_parquet


def i(value):
    """Senegal EHCVM household id from (grappe, menage): grappe + '0' +
    zero-padded (2-digit) menage.  Matches sample() / livestock.py (NO
    'E_' prefix).  Built positionally (``.iloc``)."""
    g = tools.format_id(value.iloc[0])
    m = tools.format_id(value.iloc[1], zeropadding=2)
    if g is None or m is None:
        return None
    return g + '0' + m


def _yn_bool(series, yes=1, no=2):
    """Map a 1=Oui / 2=Non survey item to nullable boolean (other codes,
    e.g. 9 'no answer', -> NA)."""
    s = pd.to_numeric(series, errors='coerce')
    out = pd.Series(pd.NA, index=s.index, dtype='boolean')
    out = out.mask(s == yes, True)
    out = out.mask(s == no, False)
    return out


def people_last7days_ehcvm(s04, s01, t, pid_col='s01q00a', age_col='s01q04a',
                           dob_year_col='s01q03c', survey_year=None):
    """Build the (i, pid, farm_work, SOB_work, wage_work, working_age) frame.
    The 7-day dummies come from s04 (s04q06/07/08); Age is merged from the
    s01 roster on (grappe, menage, pid_col)."""
    key = ['grappe', 'menage', pid_col]
    roster_cols = [age_col, dob_year_col]
    merged = s04[key + ['s04q06', 's04q07', 's04q08']].merge(
        s01[key + roster_cols], on=key, how='left')

    hh = merged.apply(lambda r: i(pd.Series([r['grappe'], r['menage']])), axis=1)
    pid = merged[pid_col].apply(tools.format_id)

    age = pd.to_numeric(merged[age_col], errors='coerce')
    if survey_year is not None:
        birth = pd.to_numeric(merged[dob_year_col], errors='coerce')
        birth = birth.where((birth >= 1900) & (birth <= survey_year), pd.NA)
        age_from_dob = survey_year - birth
        age = age.where(age.notna(), age_from_dob)
    working_age = (age >= 6)
    # Keep working_age NA where age is entirely unknown (rather than False).
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
    where the wave lacks a field), drop rows with no individual key, build
    the (t, i, pid) index (one row per person)."""
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
    # This collapse is a NO-OP on the shipped Senegal data: there is nothing
    # to collapse, so `.first()`'s per-column skipna=True can form no
    # composite row here.  Measured on the SOURCE directly (this table is
    # `materialize: make`, so it builds out-of-process and an in-process
    # groupby probe cannot see it -- see the #637 thread):
    #   s04_me_sen2018 rows 66,120; duplicate (grappe, menage, s01q00a)
    #     groups 0
    #   s01_me_sen2018 rows 66,119; duplicate person-key groups 0
    #     -> the how='left' merge fans out 0 rows (merged == 66,120)
    #   i() is INJECTIVE: 7,156 distinct (grappe, menage) pairs -> 7,156
    #     distinct ids
    #   groups on (t, i, pid) 66,120 == rows 66,120  ->  0 duplicate groups
    # Separate, non-key observation from the same probe: exactly ONE s04
    # person-key -- (grappe 481, menage 9, line 17) -- has no s01 roster row,
    # so that person's Age (and hence working_age) is NA.  A one-row roster
    # gap, not a key defect; recorded so it is not rediscovered as one.
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


s04 = get_dataframe('../Data/s04_me_sen2018.dta', convert_categoricals=False)
s01 = get_dataframe('../Data/s01_me_sen2018.dta', convert_categoricals=False)

df = people_last7days_ehcvm(s04, s01, '2018-19', survey_year=2018)
df = _finish_people_last7days(df, '2018-19')

assert len(df) > 0, 'people_last7days 2018-19 produced no rows'
to_parquet(df, 'people_last7days.parquet')
