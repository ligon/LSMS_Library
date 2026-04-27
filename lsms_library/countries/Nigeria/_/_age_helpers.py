"""Shared Age post-processor for Nigeria pp/ph wave scripts (#179).

Nigeria GHS-Panel waves use the script path (one ``_/household_roster.py``
per wave that emits a parquet via ``materialize: make``).  This helper
adopts ``age_handler`` so each wave script can:

  * extract a (possibly partial) date-of-birth triplet alongside Age,
  * concatenate the post-planting and post-harvest rounds,
  * call ``apply_age_handler(df, ...)`` to reduce the [age, d, m, y]
    columns to a single ``Age`` series via
    :func:`lsms_library.local_tools.age_handler`.

The DOB triplet is sparse in ph rounds (typical: 4-25 % coverage,
the column is a verification follow-up to the reported age) and
denser in pp rounds (typical: 80-100 %).  Where DOB is available
``age_handler`` returns a fractional DOB-derived age (more precise
than the integer reported age); where DOB is missing it falls back
to the integer reported age.  The 2010-11 / 2012-13 / 2015-16 pp
files have full day/month/year, 2018-19 pp has year-only, 2023-24
has month+year (month sparse).

Filename starts with an underscore so the country-level
formatting-function loader (which checks ``nigeria.py`` and
``mapping.py``) ignores it.

GH #179.
"""
from __future__ import annotations

from datetime import date as _date

import pandas as pd
import lsms_library.local_tools as tools


_MONTH_NAME_TO_INT = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4, 'may': 5,
    'june': 6, 'july': 7, 'august': 8, 'september': 9,
    'october': 10, 'november': 11, 'december': 12,
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'jun': 6, 'jul': 7,
    'aug': 8, 'sep': 9, 'sept': 9, 'oct': 10, 'nov': 11, 'dec': 12,
}


def _coerce_int(x, lo, hi):
    if pd.isna(x):
        return None
    try:
        v = int(float(x))
    except (TypeError, ValueError):
        return None
    return v if lo <= v <= hi else None


def _clean_age(x):
    return _coerce_int(x, 0, 130)


def _clean_day(x):
    return _coerce_int(x, 1, 31)


def _clean_month(x):
    """Month-of-birth: handle Stata 'N. NAME' categoricals, plain names,
    and raw integers.  Returns 1..12 or None."""
    if pd.isna(x):
        return None
    if isinstance(x, str):
        s = x.strip().lower()
        # Stata categorical: "9. sep" / "10. october" — try the leading
        # integer first, then fall through to name lookup if it fails.
        if '. ' in s:
            head, _, tail = s.partition('. ')
            try:
                v = int(head)
                if 1 <= v <= 12:
                    return v
            except ValueError:
                pass
            s = tail.strip()
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
    return v if 1 <= v <= 12 else None


def _clean_year(x):
    """Year-of-birth: drop sentinel 9999 / DK and out-of-range."""
    return _coerce_int(x, 1900, 2030)


def apply_age_handler(df, *, age_col='Age', day_col=None, month_col=None,
                     year_col=None, interview_year):
    """Compute ``df['Age']`` via ``age_handler`` and drop the DOB columns.

    Parameters
    ----------
    df : pd.DataFrame
        Output of the wave script's ``pd.concat([pp, ph])`` stage; must
        contain ``age_col`` and any ``day_col`` / ``month_col`` /
        ``year_col`` that are passed.
    age_col : str
        Column name holding the reported age in completed years.
    day_col, month_col, year_col : str | None
        Column names holding date-of-birth components.  Pass ``None``
        for components that aren't available in this wave (e.g.
        ``day_col=None`` for 2018-19 / 2023-24 pp).
    interview_year : int
        Calendar year that anchors the wave (e.g. ``2010`` for 2010-11);
        used by ``age_handler`` for the year-math fallback when the DOB
        is missing.

    Returns
    -------
    pd.DataFrame
        Same as ``df`` with ``age_col`` overwritten and the DOB columns
        dropped.
    """
    def _row(row):
        a = _clean_age(row[age_col]) if age_col in df.columns else None
        d = _clean_day(row[day_col]) if day_col else None
        m = _clean_month(row[month_col]) if month_col else None
        y = _clean_year(row[year_col]) if year_col else None
        # Drop d if (d, m, y) doesn't form a valid calendar date.
        if d is not None and m is not None and y is not None:
            try:
                _date(y, m, d)
            except ValueError:
                d = None
        return tools.age_handler(
            age=a, d=d, m=m, y=y, interview_year=interview_year,
        )

    df = df.copy()
    df[age_col] = df.apply(_row, axis=1)
    to_drop = [c for c in [day_col, month_col, year_col]
               if c and c in df.columns and c != age_col]
    if to_drop:
        df = df.drop(columns=to_drop)
    return df
