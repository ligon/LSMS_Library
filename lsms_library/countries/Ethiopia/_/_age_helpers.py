"""Shared Age-rescue helpers for Ethiopia waves.

Why this file isn't a wrapper around ``age_handler``
====================================================

GH #178 asked us to "adopt ``age_handler()``" along the lines of the
Niger / Togo / Uganda adoption pattern.  An empirical audit of the five
Ethiopia ESS GSEC2-equivalent files showed that fix doesn't apply
verbatim:

  * **2018-19, 2021-22** have no Age gap at all (``s1q03a`` is 100 %
    non-null in both waves).
  * **2011-12, 2013-14, 2015-16** do have gaps, but the only DOB
    triplet is ``hh_s1q04g_*`` — asked of a tiny ~3 % subset of rows
    and *only* when ``hh_s1q04_a`` is already filled, so it provides
    zero rescue.  (And the year column is in the **Ethiopian
    calendar** with month names like 'Yekatit' / 'Hamle' in 2013-14
    and Gregorian month names in 2015-16, so a real DOB rescue would
    require Ethiopian-Gregorian conversion plus dual month-name
    handling.)

What *does* close the gap is the parallel **Age in months** column
(``hh_s1q04_b`` / ``hh_s1q04b`` / ``s1q03b``), which the surveyor
fills out for under-5 children.  When ``age_years`` is missing and
``age_months`` is present, ``Age = months // 12`` is correct (years of
age is the floor of total months / 12).  Empirical rescue:

    2011-12: 2998 missing -> 2777 rescued (92.6 %)
    2013-14:  748 missing ->   26 rescued ( 3.5 %)
    2015-16: 4023 missing ->   11 rescued ( 0.3 %)

so 2814 of 7769 cross-wave Ethiopia gap rows recover.

The function below is what each adopting wave's
``<wave>.py::household_roster`` calls.  Filename starts with an
underscore so the country-level formatting-function loader (which
checks ``ethiopia.py`` and ``mapping.py``) ignores it.

GH #178.
"""
from __future__ import annotations

import pandas as pd


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
