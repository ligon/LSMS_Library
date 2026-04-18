"""Unit tests for age_intervals() / format_interval() / roster_to_characteristics().

Covers the age-bucketing convention: ``age_cuts`` is a tuple of strictly
increasing positive interior breakpoints, partitioning ages into
``len(age_cuts) + 1`` half-open buckets ``[0, c_0), [c_0, c_1), ...,
[c_{n-1}, inf)``.  Labels are compact ``"00-03"``-style when every
breakpoint is an integer, and explicit ``"[lo, hi)"``-style when any
breakpoint is fractional.
"""

import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.transformations import (
    age_intervals,
    format_interval,
    roster_to_characteristics,
)


# ---------------------------------------------------------------------------
# age_intervals — bin construction & validation
# ---------------------------------------------------------------------------


class TestAgeIntervalsDefault:
    """Default (4,9,14,19,31,51) reproduces historical demographic buckets."""

    def test_default_first_bucket_contains_0_through_3(self):
        intervals = age_intervals(pd.Series([0, 1, 2, 3]))
        assert all(iv.left == 0 and iv.right == 4 for iv in intervals)

    def test_default_bucket_boundaries_are_left_closed(self):
        # age == 4 must belong to [4, 9), not [0, 4)
        iv_3 = age_intervals(pd.Series([3])).iloc[0]
        iv_4 = age_intervals(pd.Series([4])).iloc[0]
        assert iv_3.left == 0 and iv_3.right == 4
        assert iv_4.left == 4 and iv_4.right == 9

    def test_default_top_bucket_is_unbounded(self):
        iv = age_intervals(pd.Series([51])).iloc[0]
        assert iv.left == 51 and iv.right == np.inf

    def test_ages_below_zero_become_nan(self):
        # pd.cut returns NaN for values below the lowest bin edge (0)
        result = age_intervals(pd.Series([-1, -0.5, 0, 1]))
        assert pd.isna(result.iloc[0])
        assert pd.isna(result.iloc[1])
        assert not pd.isna(result.iloc[2])


class TestAgeIntervalsFractional:
    """Fractional breakpoints are allowed and split buckets below 1 year."""

    def test_sub_year_infant_split(self):
        # (0.5, 1, 5) → [0, 0.5), [0.5, 1), [1, 5), [5, inf)
        ages = pd.Series([0.25, 0.75, 3.0, 10.0])
        intervals = age_intervals(ages, age_cuts=(0.5, 1, 5))
        lefts = [iv.left for iv in intervals]
        rights = [iv.right for iv in intervals]
        assert lefts == [0, 0.5, 1, 5]
        assert rights == [0.5, 1, 5, np.inf]


class TestAgeIntervalsBackCompat:
    """Legacy (0, 4, 9, ...) form still works but warns."""

    def test_leading_zero_emits_deprecation_warning(self):
        with pytest.warns(DeprecationWarning, match="leading 0 is deprecated"):
            age_intervals(pd.Series([5]), age_cuts=(0, 4, 9, 14, 19, 31, 51))

    def test_leading_zero_produces_same_buckets_as_new_default(self):
        ages = pd.Series(list(range(0, 80, 5)))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            legacy = age_intervals(ages, age_cuts=(0, 4, 9, 14, 19, 31, 51))
        modern = age_intervals(ages, age_cuts=(4, 9, 14, 19, 31, 51))
        # Compare (left, right) tuples — IntervalDtype equality is strict.
        legacy_pairs = [(iv.left, iv.right) for iv in legacy]
        modern_pairs = [(iv.left, iv.right) for iv in modern]
        assert legacy_pairs == modern_pairs


class TestAgeIntervalsValidation:
    """Invalid inputs raise clearly."""

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="at least one positive breakpoint"):
            age_intervals(pd.Series([1]), age_cuts=())

    def test_non_positive_raises(self):
        with pytest.raises(ValueError, match="must be > 0"):
            age_intervals(pd.Series([1]), age_cuts=(-1, 5))

    def test_non_increasing_raises(self):
        with pytest.raises(ValueError, match="strictly increasing"):
            age_intervals(pd.Series([1]), age_cuts=(5, 5, 10))

    def test_decreasing_raises(self):
        with pytest.raises(ValueError, match="strictly increasing"):
            age_intervals(pd.Series([1]), age_cuts=(10, 5))


# ---------------------------------------------------------------------------
# format_interval — compact vs. explicit label forms
# ---------------------------------------------------------------------------


def _iv(lo, hi):
    """Construct a left-closed, right-open pd.Interval."""
    return pd.Interval(lo, hi, closed='left')


class TestFormatIntervalCompact:
    """Compact style = historical column-name form."""

    def test_integer_span_over_one(self):
        assert format_interval(_iv(0, 4), compact=True) == "00-03"
        assert format_interval(_iv(4, 9), compact=True) == "04-08"
        assert format_interval(_iv(51, 99), compact=True) == "51-98"

    def test_integer_span_exactly_one(self):
        # [18, 19) contains only age 18 → label "18-18"
        assert format_interval(_iv(18, 19), compact=True) == "18-18"

    def test_unbounded_top(self):
        assert format_interval(_iv(51, np.inf), compact=True) == "51+"
        assert format_interval(_iv(4, np.inf), compact=True) == "04+"

    def test_fractional_bound_forces_explicit_even_when_compact_requested(self):
        # If either bound is fractional, compact is not representable; fall back.
        assert format_interval(_iv(0, 0.5), compact=True) == "[0, 0.5)"
        assert format_interval(_iv(0.5, 1), compact=True) == "[0.5, 1)"


class TestFormatIntervalExplicit:
    """Explicit half-open notation, used when any bound is fractional."""

    def test_integer_bounds_explicit(self):
        assert format_interval(_iv(0, 18), compact=False) == "[0, 18)"
        assert format_interval(_iv(18, 65), compact=False) == "[18, 65)"

    def test_fractional_bounds(self):
        assert format_interval(_iv(0, 0.5), compact=False) == "[0, 0.5)"
        assert format_interval(_iv(0.5, 1), compact=False) == "[0.5, 1)"

    def test_unbounded_top_uses_plus(self):
        # The +-suffix form is used for the unbounded bucket in both styles.
        assert format_interval(_iv(65, np.inf), compact=False) == "65+"


# ---------------------------------------------------------------------------
# roster_to_characteristics — end-to-end column naming
# ---------------------------------------------------------------------------


def _toy_roster():
    """Two households, six people with ages spanning multiple buckets."""
    idx = pd.MultiIndex.from_tuples(
        [
            ('2020', 'v1', 'h1', 'p1'),
            ('2020', 'v1', 'h1', 'p2'),
            ('2020', 'v1', 'h1', 'p3'),
            ('2020', 'v1', 'h2', 'p1'),
            ('2020', 'v1', 'h2', 'p2'),
            ('2020', 'v1', 'h2', 'p3'),
        ],
        names=['t', 'v', 'i', 'pid'],
    )
    return pd.DataFrame(
        {
            'Sex': ['M', 'F', 'M', 'F', 'M', 'F'],
            'Age': [35, 33, 2, 28, 0.4, 60],
        },
        index=idx,
    )


class TestRosterToCharacteristicsDefault:
    """Default buckets keep historical column names verbatim."""

    def test_default_column_names_are_compact(self):
        df = roster_to_characteristics(_toy_roster())
        # Toy roster covers M 00-03, M 31-50, F 19-30, F 31-50, F 51+
        # — exactly the buckets with non-zero counts under the default cuts.
        expected_subset = {
            'M 00-03', 'M 31-50',
            'F 19-30', 'F 31-50', 'F 51+',
            'log HSize',
        }
        assert expected_subset.issubset(set(df.columns))

    def test_default_bucket_counts(self):
        df = roster_to_characteristics(_toy_roster())
        # h1: M35 → M 31-50; F33 → F 31-50; M2 → M 00-03.
        # h2: F28 → F 19-30; M0.4 → M 00-03; F60 → F 51+.
        assert df.loc[('2020', 'v1', 'h1'), 'M 31-50'] == 1
        assert df.loc[('2020', 'v1', 'h1'), 'F 31-50'] == 1
        assert df.loc[('2020', 'v1', 'h1'), 'M 00-03'] == 1
        assert df.loc[('2020', 'v1', 'h2'), 'F 19-30'] == 1
        assert df.loc[('2020', 'v1', 'h2'), 'M 00-03'] == 1
        assert df.loc[('2020', 'v1', 'h2'), 'F 51+'] == 1


class TestRosterToCharacteristicsCustomCuts:
    """Custom integer cuts still use compact labels."""

    def test_boys_girls_men_women_shape(self):
        df = roster_to_characteristics(_toy_roster(), age_cuts=(18,))
        # Only buckets with at least one matching person will appear.  All
        # appearing bucket columns must be drawn from the Boys/Girls/Men/Women
        # set, not split further and not using any other label form.
        allowed = {'F 00-17', 'F 18+', 'M 00-17', 'M 18+', 'log HSize'}
        assert set(df.columns).issubset(allowed)
        # Toy roster covers three of the four sex×age buckets.
        must_appear = {'M 00-17', 'F 18+', 'M 18+', 'log HSize'}
        assert must_appear.issubset(set(df.columns))


class TestRosterToCharacteristicsFractional:
    """Fractional cuts switch the entire column-name set to explicit style."""

    def test_neonate_split_uses_explicit_labels(self):
        df = roster_to_characteristics(_toy_roster(), age_cuts=(0.5, 1, 5))
        # Every person-bucket label should use "[lo, hi)" form for bounded
        # buckets and "{lo}+" for the unbounded one.
        non_log_cols = [c for c in df.columns if c != 'log HSize']
        for col in non_log_cols:
            # Column is "Sex Label"; label is the portion after the first space.
            label = col.split(' ', 1)[1]
            assert label.startswith('[') or label.endswith('+'), (
                f"column {col!r} is neither explicit-bracket nor open-top"
            )
        # At least one expected explicit label must be present.
        assert any(c.endswith('[0, 0.5)') for c in df.columns)
        assert any(c.endswith('5+') for c in df.columns)
