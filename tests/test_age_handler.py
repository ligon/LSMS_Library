"""Unit tests for age_handler() and age_handler_wrapper() in local_tools.py.

Covers:
  - Full DOB (year+month+day) + interview_date → exact float age
  - Year-only + interview_year → integer difference
  - Existing age column populated → passthrough
  - All inputs None/NaN → np.nan
  - Stata sentinel -1 in year / month → treated as missing → fallback to age or NA
  - age_handler_wrapper closure bug fix: interview_year as column name
"""

import numpy as np
import pandas as pd
import pytest

from lsms_library.local_tools import age_handler, age_handler_wrapper


# ---------------------------------------------------------------------------
# age_handler unit tests
# ---------------------------------------------------------------------------


class TestAgeHandlerDOBPlusInterviewDate:
    """Full DOB + interview_date → exact float age."""

    def test_full_dob_and_interview_date(self):
        """Person born 1990-06-15, interviewed 2020-06-15 → exactly 30.0 years."""
        result = age_handler(
            dob="1990-06-15",
            interview_date="2020-06-15",
            format_dob="%Y-%m-%d",
            format_interv="%Y-%m-%d",
            interview_year=2020,
        )
        assert abs(result - 30.0) < 0.01

    def test_mdy_dob_with_interview_date(self):
        """m/d/y components + interview_date → age via date arithmetic."""
        result = age_handler(
            m=3, d=20, y=1985,
            interview_date="2023-03-20",
            format_interv="%Y-%m-%d",
            interview_year=2023,
        )
        assert abs(result - 38.0) < 0.01

    def test_my_dob_no_day_uses_midmonth(self):
        """m/y only (no day) → uses 15th of month as proxy."""
        result = age_handler(
            m=6, y=2000,
            interview_date="2025-06-15",
            format_interv="%Y-%m-%d",
            interview_year=2025,
        )
        # born ~2000-06-15, interviewed 2025-06-15 → ~25 years
        assert abs(result - 25.0) < 0.05


class TestAgeHandlerYearOnly:
    """Year-only birth year + interview_year → integer difference."""

    def test_year_only(self):
        result = age_handler(y=1990, interview_year=2023)
        assert result == 33

    def test_year_only_same_year(self):
        result = age_handler(y=2000, interview_year=2000)
        assert result == 0

    def test_year_only_swapped_returns_nan(self):
        """Birth year > interview year is a data error: must return NaN, not
        a negative age.  Pre-2026-04-25 this returned -10; the year-math
        fallback now clamps to [0, 120] so swapped/garbage inputs surface
        as missing rather than poisoning downstream stats."""
        result = age_handler(y=2010, interview_year=2000)
        assert np.isnan(result)


class TestAgeHandlerPassthrough:
    """If age column already populated, return it without modification."""

    def test_age_passthrough_integer(self):
        result = age_handler(age=35, interview_year=2023)
        assert result == 35

    def test_age_passthrough_float(self):
        result = age_handler(age=35.7, interview_year=2023)
        assert result == 35

    def test_age_passthrough_zero(self):
        result = age_handler(age=0, interview_year=2023)
        # is_valid rejects 0 (int(float(0)) == 0 < 2100 but it's falsy only
        # in the pd.notna check, which passes for 0).
        # age=0 is a valid infant age; passthrough expected.
        assert result == 0


class TestAgeHandlerAllNone:
    """All inputs None / NaN → np.nan."""

    def test_all_none(self):
        result = age_handler(interview_year=2023)
        assert np.isnan(result)

    def test_nan_age_no_dob(self):
        result = age_handler(age=np.nan, interview_year=2023)
        assert np.isnan(result)

    def test_na_age_no_dob(self):
        result = age_handler(age=pd.NA, interview_year=2023)
        assert np.isnan(result)


class TestAgeHandlerStataSentinel:
    """LSMS/EHCVM missing-value sentinels (-1, -99, 9999, etc.) must
    be rejected at the source so countries don't need per-wave
    ``_clean_age`` lambdas or post-processing wrappers."""

    def test_sentinel_minus1_birth_year(self):
        """``y=-1`` is a Stata/EHCVM 'missing' sentinel.  ``is_valid``
        now lower-bounds at 0, so the year-math branch is skipped and
        the result is NaN instead of the year-arithmetic absurdity
        ``2023 - (-1) = 2024`` produced before 2026-04-25."""
        result = age_handler(y=-1, interview_year=2023)
        assert np.isnan(result)

    def test_sentinel_minus99_birth_year(self):
        """Same guard catches the other common sentinel ``-99``."""
        result = age_handler(y=-99, interview_year=2023)
        assert np.isnan(result)

    def test_sentinel_9999_birth_year(self):
        """EHCVM uses ``9999`` for missing year of birth (Senegal
        2018-19 has 7,284 such rows)."""
        result = age_handler(y=9999, interview_year=2023)
        assert np.isnan(result)

    def test_sentinel_minus1_pre_stripped_to_none(self):
        """Simulates correct upstream usage: -1 pre-converted to None.
        Same NaN result as the bare-sentinel case — countries that
        already pre-strip lose nothing."""
        y_raw = -1
        y_val = None if y_raw == -1 else y_raw
        result = age_handler(y=y_val, interview_year=2023)
        assert np.isnan(result)

    def test_sentinel_minus1_with_valid_age_fallback(self):
        """Reported age wins: ``y=-1`` no longer poisons the result
        when ``age`` is plausible."""
        result = age_handler(age=42, y=-1, interview_year=2023)
        assert result == 42

    def test_negative_reported_age_returns_nan(self):
        """A reported age of -1 (Nigeria 2012-13's leak; pre-2026-04-25
        was returned verbatim) must surface as NaN."""
        result = age_handler(age=-1, interview_year=2023)
        assert np.isnan(result)

    def test_implausibly_old_reported_age_returns_nan(self):
        """Nigeria's documented sentinels at the upper end (130, 150,
        999) must also return NaN — the 0-120 clamp is symmetric."""
        for sentinel in (130, 150, 999):
            result = age_handler(age=sentinel, interview_year=2023)
            assert np.isnan(result), f"age={sentinel} should be NaN"

    def test_zero_reported_age_passes(self):
        """``age=0`` is a valid infant age and must not be filtered."""
        result = age_handler(age=0, interview_year=2023)
        assert result == 0

    def test_age_120_passes(self):
        """The upper bound 120 is inclusive — verified human ages
        approach this; 121+ is implausible."""
        result = age_handler(age=120, interview_year=2023)
        assert result == 120

    def test_age_121_returns_nan(self):
        """Just over the bound."""
        result = age_handler(age=121, interview_year=2023)
        assert np.isnan(result)


class TestAgeHandlerBadInterviewDate:
    """Regression for #186: bare ``except:`` → narrow exception list.

    When ``interview_date`` is given without ``format_interv``, the function
    falls into a bare ``pd.to_datetime(interview_date)`` call.  The bare
    ``except:`` originally guarded this against parse failure but also
    swallowed ``SystemExit`` / ``KeyboardInterrupt``.  The narrowed
    ``(ValueError, TypeError, pd.errors.ParserError)`` clause must still
    catch unparseable strings so the function falls back to
    ``interview_year`` arithmetic without raising.
    """

    def test_unparseable_interview_date_falls_back_to_year_arithmetic(self):
        """Garbage interview_date with no format → caught, year path wins."""
        result = age_handler(
            interview_date="not-a-date",
            interview_year=2023,
            y=1990,
        )
        assert result == 33


class TestAgeHandlerWrapperClosureFix:
    """Regression test for the interview_year closure bug (GH #173 Step 2).

    Before the fix, passing ``interview_year='survey_year_col'`` (a string
    column name) would raise ``UnboundLocalError`` in Python 3 because the
    inner closure tried to rebind the outer variable.
    """

    def _make_df(self):
        return pd.DataFrame({
            "survey_year_col": [2020, 2021, 2022],
            "birth_year": [1990, 1985, 2000],
        })

    def test_interview_year_as_column_name(self):
        """interview_year='survey_year_col' must not raise UnboundLocalError."""
        df = self._make_df()
        result = age_handler_wrapper(
            df,
            interview_year="survey_year_col",
            y="birth_year",
        )
        expected = pd.Series([30, 36, 22])
        pd.testing.assert_series_equal(result, expected, check_names=False)

    def test_interview_year_as_integer(self):
        """interview_year as plain int continues to work after the fix."""
        df = self._make_df()
        result = age_handler_wrapper(
            df,
            interview_year=2023,
            y="birth_year",
        )
        expected = pd.Series([33, 38, 23])
        pd.testing.assert_series_equal(result, expected, check_names=False)
