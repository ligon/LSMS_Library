"""Shared age-from-DOB helpers for Uganda waves.

Uganda's GSEC2 has had ``h2q8`` (age in completed years) and
``h2q9a/b/c`` (day, month, year of birth) since 2009-10.  When the
direct age is missing but the year of birth is present, we can still
recover Age via :func:`lsms_library.local_tools.age_handler`.  This
module centralises the sentinel-cleaning and per-row ``age_handler``
glue so each wave's ``_/{wave}.py`` is a thin shim that just supplies
``INTERVIEW_YEAR``.

The filename starts with an underscore so the country-level
formatting-function loader (which only looks at ``uganda.py`` and
``mapping.py``) ignores it.  Wave modules import via ``sys.path``
manipulation; see ``2013-14.py`` for the canonical pattern.

Sentinel handling:
  * day  (h2q9a):  99 / '99'                -> None
  * month (h2q9b): 'DK' / 'dk' / "Don't know" / 'NSP' / numeric 99
                   English month names ('October', 'August', ...)
                                              -> 1..12  (post-2018 waves)
  * year (h2q9c):  9999                     -> None  (also enforced by
                                              age_handler.is_valid)

GH #177.
"""
from __future__ import annotations

import pandas as pd
import lsms_library.local_tools as tools


_MONTH_DK_TOKENS = {'dk', "don't know", 'dont know', 'nsp', '99'}

_MONTH_NAME_TO_INT = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5,
    'june': 6, 'july': 7, 'august': 8, 'september': 9,
    'october': 10, 'november': 11, 'december': 12,
    # Common abbreviations
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6, 'jul': 7,
    'aug': 8, 'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dec': 12,
}


def clean_age(x):
    """Clean a single Age scalar; return ``None`` on missing/sentinel."""
    if pd.isna(x):
        return None
    try:
        v = int(float(x))
    except (TypeError, ValueError):
        return None
    return v if 0 <= v <= 130 else None


def clean_day(x):
    """Day-of-birth: drop sentinel 99 and out-of-range."""
    if pd.isna(x):
        return None
    try:
        v = int(float(x))
    except (TypeError, ValueError):
        return None
    if v == 99 or v < 1 or v > 31:
        return None
    return v


def clean_month(x):
    """Month-of-birth: handle DK tokens, English names, numeric 1..12."""
    if pd.isna(x):
        return None
    if isinstance(x, str):
        s = x.strip().lower()
        if s in _MONTH_DK_TOKENS or s == '':
            return None
        if s in _MONTH_NAME_TO_INT:
            return _MONTH_NAME_TO_INT[s]
        try:
            v = int(float(s))
        except (TypeError, ValueError):
            return None
    else:
        try:
            v = int(float(x))
        except (TypeError, ValueError):
            return None
    if v == 99 or v < 1 or v > 12:
        return None
    return v


def clean_year(x):
    """Year-of-birth: drop sentinel 9999 and out-of-range."""
    if pd.isna(x):
        return None
    try:
        v = int(float(x))
    except (TypeError, ValueError):
        return None
    if v == 9999 or v < 1900 or v > 2100:
        return None
    return v


def age_components(value):
    """Pre-process the YAML-list ``Age`` value for a single row.

    ``value`` is a 4-element pandas Series in the order
    ``[h2q8, h2q9a, h2q9b, h2q9c]`` = ``[age, day, month, year]``.
    Returns a list of cleaned numeric components or ``None``s.

    Note: invalid ``(d, m, y)`` calendar combinations (Feb 30, etc.)
    used to be dropped here as a workaround for a latent crash in
    ``age_handler``.  GH #205 lifted that guard into ``age_handler``
    itself, so the local version was removed.
    """
    age = clean_age(value.iloc[0])
    d = clean_day(value.iloc[1])
    m = clean_month(value.iloc[2])
    y = clean_year(value.iloc[3])
    return [age, d, m, y]


def run_household_roster(df, interview_year):
    """Reduce list-valued ``Age`` to a scalar using ``age_handler``.

    Wave-level ``household_roster`` post-processors call this with
    the calendar year that anchors the wave (e.g. ``2013`` for 2013-14).
    """
    def _age_from_row(row):
        comps = row['Age']
        return tools.age_handler(
            age=comps[0],
            d=comps[1],
            m=comps[2],
            y=comps[3],
            interview_year=interview_year,
        )

    df['Age'] = df.apply(_age_from_row, axis=1)
    return df
