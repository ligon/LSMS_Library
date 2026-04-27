"""Shared Age-rescue helpers for Ethiopia waves.

Two pipelines coexist in this file -- pick by wave:

A. ``age_components`` + ``run_household_roster``
   2-element ``[years, months]`` cleanup with years-or-months fallback.
   Used by 2011-12.  See GH #178.

B. ``primitives``  (``_coerce_int``, ``clean_age``, ``clean_day``,
   ``clean_month_english``, ``clean_year_gregorian``)
   Building blocks that 2013-14 and 2015-16 ``<wave>.py`` shims compose
   with the calendar conversion in ``lsms_library.calendars`` to
   feed ``age_handler``.  See the per-wave docstrings.

Why we need calendar conversion
===============================

* **2013-14** records DOB in the **Ethiopian calendar**: year as a
  2-digit Eth integer (``89`` = Eth 1989), month as Amharic name
  ('Yekatit', 'Hamle'), day in Eth-month range (1..30 or 1..6 for
  Pagume).  ``lsms_library.calendars`` converts to Gregorian before
  feeding ``age_handler``.
* **2015-16** records DOB in the **Gregorian calendar**: 4-digit
  year, English month name ('November', 'December', sometimes
  misspelled like 'Sebtember'), day 1..31.  No conversion needed,
  just sentinel cleanup.

Empirical rescue (years-or-months path, both pipelines apply):

    2011-12: 2998 missing -> 2777 rescued (92.6 %)
    2013-14:  748 missing ->   26 rescued ( 3.5 %)
    2015-16: 4023 missing ->   11 rescued ( 0.3 %)

The 2013-14 / 2015-16 DOB pipeline additionally provides
**DOB-derived fractional precision** for rows where age + DOB are
both present (~700-1000 rows per wave).

Filename starts with an underscore so the country-level
formatting-function loader (which checks ``ethiopia.py`` and
``mapping.py``) ignores it.

GH #178.
"""
from __future__ import annotations

import pandas as pd


# ---------------------------------------------------------------------------
# Pipeline A -- years/months fallback (2011-12)
# ---------------------------------------------------------------------------


def _coerce_int(x, lo, hi):
    """Return int(x) clamped to ``[lo, hi]``, or None on bad input."""
    if pd.isna(x):
        return None
    try:
        v = int(float(x))
    except (TypeError, ValueError):
        return None
    return v if lo <= v <= hi else None


def age_components(value):
    """Pre-process the YAML-list ``Age`` value for a single row.

    ``value`` is a 2-element pandas Series ``[years_col, months_col]``.
    Returns ``[years_or_None, months_or_None]`` with sentinels and out-
    of-range values mapped to ``None``.
    """
    years = _coerce_int(value.iloc[0], 0, 130)
    months = _coerce_int(value.iloc[1], 0, 1500)  # 1500 mo = 125 y
    return [years, months]


def run_household_roster(df):
    """Reduce list-valued ``Age`` to an int via years-or-months fallback."""
    def _age_from_row(row):
        years, months = row['Age']
        if years is not None:
            return years
        if months is not None:
            return months // 12
        return None

    df['Age'] = df.apply(_age_from_row, axis=1)
    return df


# ---------------------------------------------------------------------------
# Pipeline B -- primitives for the DOB pipeline (2013-14, 2015-16)
# ---------------------------------------------------------------------------


def clean_age(x):
    """Reported-age years; sentinel-strip and clamp to [0, 130]."""
    return _coerce_int(x, 0, 130)


def clean_age_months(x):
    """Reported-age months for under-5 children; clamp to [0, 1500]."""
    return _coerce_int(x, 0, 1500)


def clean_day(x):
    """Day-of-birth; clamp to [1, 31] (Eth Pagume separately validated)."""
    return _coerce_int(x, 1, 31)


_ENGLISH_MONTH_LOOKUP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5,
    'june': 6, 'july': 7, 'august': 8, 'september': 9,
    'october': 10, 'november': 11, 'december': 12,
    # common misspellings encountered in 2015-16 raw data
    'sebtember': 9, 'sept': 9, 'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
    'jun': 6, 'jul': 7, 'aug': 8, 'oct': 10, 'nov': 11, 'dec': 12,
}


def clean_month_english(x):
    """Parse a Gregorian English month name (with common misspellings)."""
    if pd.isna(x):
        return None
    if isinstance(x, str):
        s = x.strip().lower()
        if not s:
            return None
        if s in _ENGLISH_MONTH_LOOKUP:
            return _ENGLISH_MONTH_LOOKUP[s]
        # Fallback: numeric string
        try:
            v = int(float(s))
        except (TypeError, ValueError):
            return None
    else:
        try:
            v = int(float(x))
        except (TypeError, ValueError):
            return None
    return v if 1 <= v <= 12 else None


def clean_year_gregorian(x):
    """Year-of-birth (Gregorian); plausible range [1900, 2030]."""
    return _coerce_int(x, 1900, 2030)
