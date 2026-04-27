"""Calendar conversion helpers.

This module currently exposes one calendar -- Ethiopian -- because it's
the only non-Gregorian calendar that surfaces in raw LSMS data we care
about.  Other calendars (Hijri, Coptic) can be added on demand.

Ethiopian calendar quick reference
==================================

* The Ethiopian calendar is roughly 7-8 years behind the Gregorian:
  Eth Year 1, 1, 1 = 29 Aug 8 CE Julian (= 27 Aug 8 CE Gregorian).
* It has 13 months -- 12 of 30 days each, plus a 13th month called
  Pagume of 5 days (6 in Ethiopian leap years).
* Ethiopian leap years are every 4 years where ``year % 4 == 3``.
* The Ethiopian new year (Meskerem 1) falls on Gregorian September 11
  in normal years, September 12 in years where the *preceding*
  Ethiopian year was a leap year (i.e., the prior Pagume had 6 days).

Month name reference (Amharic transliterations seen in raw data):

    1.  Meskerem  (~Sep 11 - Oct 10 Greg)
    2.  Tikimt    (~Oct 11 - Nov  9)
    3.  Hidar     (~Nov 10 - Dec  9)
    4.  Tahsas    (~Dec 10 - Jan  8)
    5.  Tir       (~Jan  9 - Feb  7)
    6.  Yekatit   (~Feb  8 - Mar  9)
    7.  Megabit   (~Mar 10 - Apr  8)
    8.  Miyazya   (~Apr  9 - May  8)
    9.  Ginbot    (~May  9 - Jun  7)
   10.  Sene      (~Jun  8 - Jul  7)
   11.  Hamle     (~Jul  8 - Aug  6)
   12.  Nehase    (~Aug  7 - Sep  5)
   13.  Pagume    (~Sep  6 - Sep 10, 5 days; 6 in leap years)

The conversion uses Julian Day Numbers as the pivot.  The forward
direction (Eth -> JDN) is closed-form arithmetic; the JDN -> Greg step
is Edward G. Richards' standard formula (Mapping Time, 2013, 25.18.5).

References:
* Ethiopian Calendar -- Wikipedia
* https://github.com/Senamiku/ethiopian-date-converter
* Richards, E. G. (2013), "Calendars", in S. E. Urban & P. K. Seidelmann
  (eds.) *Explanatory Supplement to the Astronomical Almanac*, 3rd ed.
"""
from __future__ import annotations

from datetime import date


# Julian Day Number of Eth 1/1/1 = 29 Aug 8 CE Julian = 27 Aug 8 CE Greg
_ETH_EPOCH_JDN = 1724221


# Canonical Ethiopian month order (1-indexed; 0th slot kept for clarity).
ETHIOPIAN_MONTHS: list[str] = [
    "",            # 0 unused
    "Meskerem", "Tikimt", "Hidar", "Tahsas", "Tir",
    "Yekatit", "Megabit", "Miyazya", "Ginbot", "Sene",
    "Hamle", "Nehase", "Pagume",
]


# Lowercase variant -> month integer.  Includes common transliteration
# spellings and short forms encountered in LSMS-Ethiopia raw data
# (`hh_s1q04g_2` in 2013-14 GHS Wave 2).
_ETH_MONTH_LOOKUP: dict[str, int] = {
    # Canonical
    "meskerem": 1, "tikimt": 2, "hidar": 3, "tahsas": 4, "tir": 5,
    "yekatit": 6, "megabit": 7, "miyazya": 8, "ginbot": 9, "sene": 10,
    "hamle": 11, "nehase": 12, "pagume": 13,
    # Common alternate spellings
    "meskarem": 1, "meskerm": 1, "tekemt": 2, "tikemt": 2,
    "tahesas": 4, "tahisas": 4, "ter": 5, "yekatet": 6,
    "meyazia": 8, "miyaza": 8, "miazia": 8, "ginbo": 9,
    "sine": 10, "senie": 10, "nehasse": 12, "pagumen": 13, "puagme": 13,
}


def parse_ethiopian_month(name) -> int | None:
    """Map an Ethiopian month name (Amharic transliteration) to 1..13.

    Returns ``None`` for unrecognised input or NaN/missing.
    """
    if name is None:
        return None
    if not isinstance(name, str):
        # Numeric input that already encodes the month number directly.
        try:
            v = int(float(name))
        except (TypeError, ValueError):
            return None
        return v if 1 <= v <= 13 else None
    s = name.strip().lower()
    if not s:
        return None
    return _ETH_MONTH_LOOKUP.get(s)


def is_ethiopian_leap_year(year: int) -> bool:
    """Ethiopian leap years: ``year % 4 == 0``.

    Per Wikipedia ("Ethiopian calendar"), Eth 8, 12, ..., 1996, 2000,
    2004, 2008, 2012 are leap.  Pagume (the 13th month) has 6 days in
    those years and 5 days otherwise.

    Note: this differs from the Gregorian leap rule -- Eth 2100 will be
    leap (% 4 == 0) while Greg 2100 is not (century-no-400-rule), so
    the calendars drift by one day in 2100+ -- but for the LSMS-Ethiopia
    data range (1900-2099) the rules align.
    """
    return year % 4 == 0


def ethiopian_to_jdn(year: int, month: int, day: int) -> int:
    """Convert an Ethiopian (year, month, day) to Julian Day Number.

    Validates that the inputs form a real Ethiopian date; raises
    ``ValueError`` otherwise.
    """
    if not (1 <= month <= 13):
        raise ValueError(f"Ethiopian month must be 1..13, got {month}")
    if month == 13:
        max_day = 6 if is_ethiopian_leap_year(year) else 5
    else:
        max_day = 30
    if not (1 <= day <= max_day):
        raise ValueError(
            f"Ethiopian day {day} out of range for "
            f"month {month} (max={max_day}, year={year})"
        )
    # Leap-day count: number of Eth leap years in [1, year-1] (the
    # current year's leap day, if any, has not yet occurred at month 1
    # day 1 — Pagume 6 falls at the *end* of the year).
    return (
        _ETH_EPOCH_JDN
        + 365 * (year - 1)
        + ((year - 1) // 4)
        + 30 * (month - 1)
        + day - 1
    )


def jdn_to_gregorian(jdn: int) -> date:
    """Convert a Julian Day Number to a Gregorian ``datetime.date``.

    Uses Edward G. Richards' formula (Mapping Time, 2013), valid for
    any JDN representing a Gregorian date after the calendar's start.
    """
    a = jdn + 32044
    b = (4 * a + 3) // 146097
    c = a - (146097 * b) // 4
    d = (4 * c + 3) // 1461
    e = c - (1461 * d) // 4
    m = (5 * e + 2) // 153
    g_day = e - (153 * m + 2) // 5 + 1
    g_month = m + 3 - 12 * (m // 10)
    g_year = 100 * b + d - 4800 + m // 10
    return date(g_year, g_month, g_day)


def ethiopian_to_gregorian(year: int, month: int, day: int) -> date:
    """Convert an Ethiopian (year, month, day) to a Gregorian ``date``.

    >>> ethiopian_to_gregorian(2000, 1, 1)
    datetime.date(2007, 9, 11)
    >>> ethiopian_to_gregorian(2007, 13, 6)   # Pagume 6 in a leap year
    datetime.date(2015, 9, 11)
    >>> ethiopian_to_gregorian(2008, 1, 1)    # day after the leap Pagume
    datetime.date(2015, 9, 12)
    """
    return jdn_to_gregorian(ethiopian_to_jdn(year, month, day))


def disambiguate_two_digit_eth_year(
    yy: int, *, interview_eth_year: int, reported_age: int | None = None,
) -> int | None:
    """Resolve a 2-digit Ethiopian year to its 4-digit form.

    LSMS-Ethiopia 2013-14 GHS Wave 2 records year of birth as a 2-digit
    integer (e.g. ``89`` for Eth 1989, ``1`` for Eth 2001).  The
    century is implicit; this helper picks the candidate that yields a
    plausible age in [0, 120], using ``reported_age`` as a tiebreaker
    when both centuries are plausible.

    Returns ``None`` if neither candidate yields a plausible age, or if
    the input is already 4-digit and out of range.
    """
    if yy is None:
        return None
    try:
        yy = int(yy)
    except (TypeError, ValueError):
        return None
    # 2-digit input: try both centuries.  Already-4-digit input passes
    # through but is still subject to the plausibility check below.
    if yy < 100:
        candidates = [1900 + yy, 2000 + yy]
    else:
        candidates = [yy]
    plausible: list[int] = []
    for c in candidates:
        a = interview_eth_year - c
        if 0 <= a <= 120:
            plausible.append(c)
    if not plausible:
        return None
    if len(plausible) == 1:
        return plausible[0]
    # Both centuries yield a plausible age (typical for yy in 0..13 with
    # interview around Eth 2006).  Pick the candidate closest to the
    # reported age when available; otherwise prefer the more recent.
    if reported_age is not None:
        try:
            ra = int(reported_age)
        except (TypeError, ValueError):
            ra = None
        if ra is not None and 0 <= ra <= 120:
            return min(plausible, key=lambda c: abs((interview_eth_year - c) - ra))
    # Default: pick the more recent (largest) century-candidate.
    return max(plausible)
