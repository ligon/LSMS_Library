"""Wave-level formatting helpers for Ethiopia 2013-14 (GH #178).

The ``Age`` list declared in ``data_info.yml`` is 6 elements wide:

    [age_yrs, age_mos, dob_day, dob_month_amharic, dob_year_eth_2dig,
     corrected_age]

DOB is recorded in the **Ethiopian calendar** -- 2-digit year,
Amharic month name (e.g. 'Yekatit'), Eth-month-range day.  We
convert to Gregorian via :mod:`lsms_library.calendars` before
feeding ``age_handler``.

Per-row precedence:
  1. If a Gregorian DOB can be reconstructed: feed (age, d, m, y) to
     ``age_handler`` -> DOB-derived fractional age (most precise).
  2. Else if ``corrected_age`` is filled: use it.
  3. Else if ``age_yrs`` is filled: use it.
  4. Else if ``age_mos`` is filled: use ``age_mos // 12``.
  5. Else None.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import lsms_library.local_tools as tools
from lsms_library.calendars import (
    disambiguate_two_digit_eth_year,
    ethiopian_to_gregorian,
    parse_ethiopian_month,
)

_HELPERS = Path(__file__).resolve().parent.parent.parent / "_" / "_age_helpers.py"
_spec = importlib.util.spec_from_file_location("_ethiopia_age_helpers", _HELPERS)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)


# Eth 2006 ~ Greg 2013/14 (the wave's interview window).
INTERVIEW_YEAR_GREG = 2013
INTERVIEW_YEAR_ETH = 2006


def Age(value):
    """Pre-process the 6-element ``Age`` list per row.

    Converts Ethiopian-calendar DOB to Gregorian (or ``None`` for
    any component that is unrecoverable).
    """
    age_yrs = _mod.clean_age(value.iloc[0])
    age_mos = _mod.clean_age_months(value.iloc[1])
    raw_day = value.iloc[2]
    raw_month_amharic = value.iloc[3]
    raw_year_eth = value.iloc[4]
    corrected = _mod.clean_age(value.iloc[5])

    # Best reported-age estimate for the Eth-year disambiguation tiebreaker.
    reported_age = corrected if corrected is not None else age_yrs

    eth_y = disambiguate_two_digit_eth_year(
        raw_year_eth,
        interview_eth_year=INTERVIEW_YEAR_ETH,
        reported_age=reported_age,
    )
    eth_m = parse_ethiopian_month(raw_month_amharic)
    if eth_m == 13:
        eth_d = _mod._coerce_int(raw_day, 1, 6)
    elif eth_m is not None:
        eth_d = _mod._coerce_int(raw_day, 1, 30)
    else:
        eth_d = None

    greg_d = greg_m = greg_y = None
    if eth_y and eth_m and eth_d:
        try:
            g = ethiopian_to_gregorian(eth_y, eth_m, eth_d)
        except ValueError:
            # Day exceeds month length (e.g. Pagume 6 in a non-leap
            # Eth year).  Fall back to day=1 of the same month.
            try:
                g = ethiopian_to_gregorian(eth_y, eth_m, 1)
            except ValueError:
                g = None
        if g is not None:
            greg_y, greg_m, greg_d = g.year, g.month, g.day

    return [age_yrs, age_mos, greg_d, greg_m, greg_y, corrected]


def household_roster(df):
    """Reduce list-valued ``Age`` to a scalar via DOB-aware fallback chain.

    ``age_handler`` derives a fractional Age only when ``interview_date``
    is a full date (not just a year).  We pass a mid-year synthetic
    date (Jul 1) so DOB-bearing rows get fractional precision; the
    +/- 0.5 year error from the unknown interview day is smaller than
    the +/- 0.5 year rounding error of the reported integer age.
    """
    # Pass mid-year as a "%m/%d/%Y" string -- age_handler's list-input
    # branch trips on ``pd.notna(list)`` (latent bug in age_handler;
    # safe to fix later).  String form works today.
    interview_date = f'07/01/{INTERVIEW_YEAR_GREG}'

    def _row(row):
        age_yrs, age_mos, d, m, y, corrected = row['Age']
        best_reported = corrected if corrected is not None else age_yrs
        if y is not None and m is not None:
            return tools.age_handler(
                age=best_reported, d=d, m=m, y=y,
                interview_date=interview_date,
                format_interv='%m/%d/%Y',
                interview_year=INTERVIEW_YEAR_GREG,
            )
        if best_reported is not None:
            return best_reported
        if age_mos is not None:
            return age_mos // 12
        return None

    df['Age'] = df.apply(_row, axis=1)
    return df
