"""Tests for the Ethiopian calendar conversion in lsms_library.calendars."""
from datetime import date

import pytest

from lsms_library.calendars import (
    disambiguate_two_digit_eth_year,
    ethiopian_to_gregorian,
    ethiopian_to_jdn,
    is_ethiopian_leap_year,
    jdn_to_gregorian,
    parse_ethiopian_month,
)


# ---------------------------------------------------------------------------
# Forward conversion: Ethiopian -> Gregorian
# ---------------------------------------------------------------------------


class TestEthiopianToGregorian:
    """Cross-check known Ethiopian -> Gregorian conversions.

    Anchor points (from Wikipedia "Ethiopian calendar" and the
    well-documented Ethiopian Millennium):

      * Eth 2000/1/1 = 11 Sep 2007 Greg (Ethiopian Millennium)
      * Eth 2007/1/1 = 11 Sep 2014 Greg
      * Eth 2008/1/1 = 11 Sep 2015 Greg (Eth 2007 is normal, not leap)
      * Eth 2000/13/6 = 10 Sep 2008 Greg (Pagume 6 of Eth 2000 leap year)
      * Eth 2012/1/1 = 11 Sep 2019 Greg

    Eth leap years are ``year % 4 == 0`` (i.e., 2000, 2004, 2008, 2012),
    not the predecessors -- the Eth leap day (Pagume 6) lies in Sep just
    *before* the corresponding Greg leap year's Feb 29, so Eth and Greg
    leap counts stay in lockstep within the 1900-2099 range.
    """

    def test_eth_2000_new_year(self):
        """Ethiopian Millennium = 11 Sep 2007 Greg."""
        assert ethiopian_to_gregorian(2000, 1, 1) == date(2007, 9, 11)

    def test_eth_2007_new_year(self):
        assert ethiopian_to_gregorian(2007, 1, 1) == date(2014, 9, 11)

    def test_eth_2008_new_year(self):
        """Eth 2007 has 365 days (not a leap), so Eth 2008/1/1 = Sep 11, 2015."""
        assert ethiopian_to_gregorian(2008, 1, 1) == date(2015, 9, 11)

    def test_eth_2000_pagume_6(self):
        """Pagume 6 of Eth 2000 (leap year) -> 10 Sep 2008 Greg."""
        assert ethiopian_to_gregorian(2000, 13, 6) == date(2008, 9, 10)

    def test_eth_2012_new_year(self):
        assert ethiopian_to_gregorian(2012, 1, 1) == date(2019, 9, 11)

    def test_eth_1989_yekatit_1(self):
        """Spot value from Ethiopia 2013-14 GHS data: Eth 1989 Yekatit 1."""
        # Yekatit (month 6) starts ~ Feb 8 in Greg.
        result = ethiopian_to_gregorian(1989, 6, 1)
        assert result == date(1997, 2, 8)


class TestEthiopianLeapYear:
    """Eth leap years are ``year % 4 == 0``."""

    @pytest.mark.parametrize("year,expected", [
        (1999, False),
        (2000, True),    # leap
        (2001, False),
        (2002, False),
        (2003, False),
        (2004, True),    # leap
        (2007, False),
        (2008, True),    # leap
        (2011, False),
        (2012, True),    # leap
        (1, False),
        (4, True),       # earliest verifiable leap (year 4)
        (8, True),
    ])
    def test_leap_year_rule(self, year, expected):
        assert is_ethiopian_leap_year(year) is expected


class TestEthiopianValidation:
    """``ethiopian_to_jdn`` rejects impossible dates."""

    def test_rejects_month_zero(self):
        with pytest.raises(ValueError):
            ethiopian_to_jdn(2000, 0, 15)

    def test_rejects_month_14(self):
        with pytest.raises(ValueError):
            ethiopian_to_jdn(2000, 14, 1)

    def test_rejects_day_31_in_month_1(self):
        with pytest.raises(ValueError):
            ethiopian_to_jdn(2000, 1, 31)

    def test_rejects_day_6_in_pagume_non_leap(self):
        # Eth 2007 is not a leap year (2007 % 4 == 3 != 0); Pagume = 5 days.
        with pytest.raises(ValueError):
            ethiopian_to_jdn(2007, 13, 6)

    def test_accepts_day_6_in_pagume_leap(self):
        # Eth 2000 is a leap year; Pagume 6 is valid.
        ethiopian_to_jdn(2000, 13, 6)  # no raise


# ---------------------------------------------------------------------------
# Month-name parser
# ---------------------------------------------------------------------------


class TestParseEthiopianMonth:
    @pytest.mark.parametrize("name,expected", [
        ("Yekatit", 6),
        ("yekatit", 6),
        ("YEKATIT", 6),
        ("Hamle", 11),
        ("Tikimt", 2),
        ("Tahsas", 4),
        ("Hidar", 3),
        ("Tir", 5),
        ("Sene", 10),
        ("Megabit", 7),
        ("Pagume", 13),
        # alternate spellings
        ("Tekemt", 2),
        ("Tahesas", 4),
    ])
    def test_known_names(self, name, expected):
        assert parse_ethiopian_month(name) == expected

    @pytest.mark.parametrize("name", ["", "  ", "garbage", "January", None])
    def test_unknown_returns_none(self, name):
        assert parse_ethiopian_month(name) is None

    def test_numeric_passthrough(self):
        assert parse_ethiopian_month(7) == 7
        assert parse_ethiopian_month(7.0) == 7

    def test_numeric_out_of_range_returns_none(self):
        assert parse_ethiopian_month(0) is None
        assert parse_ethiopian_month(14) is None


# ---------------------------------------------------------------------------
# 2-digit year disambiguation
# ---------------------------------------------------------------------------


class TestDisambiguateTwoDigitEthYear:
    """LSMS Ethiopia 2013-14 records Eth year as 2-digit ints (89 = 1989)."""

    def test_eth_89_in_2006_resolves_to_1989(self):
        # Only 1989 is plausible (2089 would be future).
        assert disambiguate_two_digit_eth_year(89, interview_eth_year=2006) == 1989

    def test_eth_49_in_2006_resolves_to_1949(self):
        # 2049 future, 1949 plausible (age 57).
        assert disambiguate_two_digit_eth_year(49, interview_eth_year=2006) == 1949

    def test_eth_5_in_2006_resolves_to_2005(self):
        # 1905 → age 101 plausible; 2005 → age 1 plausible.  Default to
        # 2005 (more recent) when no reported age provided.
        assert disambiguate_two_digit_eth_year(5, interview_eth_year=2006) == 2005

    def test_eth_5_in_2006_with_high_reported_age_resolves_to_1905(self):
        # 1905 → age 101 matches reported; 2005 → age 1 doesn't.
        result = disambiguate_two_digit_eth_year(
            5, interview_eth_year=2006, reported_age=101,
        )
        assert result == 1905

    def test_eth_5_in_2006_with_low_reported_age_resolves_to_2005(self):
        result = disambiguate_two_digit_eth_year(
            5, interview_eth_year=2006, reported_age=1,
        )
        assert result == 2005

    def test_already_4_digit_passthrough(self):
        assert disambiguate_two_digit_eth_year(1989, interview_eth_year=2006) == 1989

    def test_implausibly_old_returns_none(self):
        # If both centuries yield ages outside [0, 120], return None.
        # E.g., Eth year 200 would be Eth 1200 or Eth 2200 — neither
        # plausible for a 2006 interview.
        assert disambiguate_two_digit_eth_year(
            200, interview_eth_year=2006,
        ) is None

    def test_garbage_input_returns_none(self):
        assert disambiguate_two_digit_eth_year(None, interview_eth_year=2006) is None
        assert disambiguate_two_digit_eth_year("abc", interview_eth_year=2006) is None


# ---------------------------------------------------------------------------
# JDN round-trip sanity
# ---------------------------------------------------------------------------


class TestJdnRoundTrip:
    """For each anchor date, jdn_to_gregorian(ethiopian_to_jdn(...)) must
    return the documented Gregorian date."""

    @pytest.mark.parametrize("eth,greg", [
        ((2000, 1, 1), (2007, 9, 11)),
        ((2007, 1, 1), (2014, 9, 11)),
        ((2008, 1, 1), (2015, 9, 11)),
        ((2000, 13, 6), (2008, 9, 10)),  # Pagume 6 of Eth 2000 leap year
        ((2012, 1, 1), (2019, 9, 11)),
        ((1989, 6, 1), (1997, 2, 8)),
    ])
    def test_round_trip(self, eth, greg):
        jdn = ethiopian_to_jdn(*eth)
        assert jdn_to_gregorian(jdn) == date(*greg)
