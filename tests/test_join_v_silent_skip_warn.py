"""
Unit test for the silent-skip guard in Country._join_v_from_sample (GH #256).

When a roster's `i` doesn't intersect sample's `i` for a given wave,
the left-merge in ``_join_v_from_sample`` produces rows with 100% NaN v
for that wave.  Downstream groupby() in ``roster_to_characteristics``
and the food-derivation pipeline drops NaN-keyed rows by default,
silently swallowing the entire wave.

Tajikistan/1999 pre-PR #253 was the worked example: roster declared
``i: hhid`` but sample declared ``i: [pop_pt, hhid]``; result was
``(10306, 15)`` -- 3 waves -- with 1999 silently missing.  PR #253
fixed Tajikistan; PRs #244 (Guyana, Azerbaijan, Serbia and Montenegro)
and #255 (Serbia/2007) handled the rest.  The framework warning here
is the regression net so any future drift surfaces immediately.

These tests bypass the live ``sample()`` build by pre-populating
``Country._sample_v_cache`` with synthetic data, then call
``_join_v_from_sample`` directly.
"""
import warnings

import pandas as pd
import pytest

import lsms_library as ll


def _make_country() -> ll.Country:
    """Pick any country whose data_scheme declares both sample and
    household_roster.  Uganda fits and is always available in the
    skipif matrix.  We mock _sample_v_cache so no real I/O happens.
    """
    return ll.Country("Uganda")


def _set_synthetic_sample(country: ll.Country, sample_df: pd.DataFrame) -> None:
    """Pre-populate the v cache so _join_v_from_sample uses our synthetic data."""
    # _join_v_from_sample reads _sample_v_cache directly; populate it with
    # a DataFrame that has v and is indexed by (i, t) -- the same shape
    # ``sample()[['v']]`` would return.
    country._sample_v_cache = sample_df


def _roster(rows: list[tuple]) -> pd.DataFrame:
    """Construct a roster-shaped DataFrame from a list of (i, t) tuples."""
    return pd.DataFrame(
        {"value": list(range(len(rows)))},
        index=pd.MultiIndex.from_tuples(rows, names=["i", "t"]),
    )


def _sample(pairs: list[tuple]) -> pd.DataFrame:
    """Construct a sample-shaped DataFrame: index=(i, t), single 'v' column."""
    idx = pd.MultiIndex.from_tuples([(i, t) for i, t, _ in pairs], names=["i", "t"])
    return pd.DataFrame({"v": [v for _, _, v in pairs]}, index=idx)


def test_warns_on_fully_nan_wave():
    """A wave whose roster i doesn't intersect sample's i triggers the warning."""
    country = _make_country()

    # Two waves in roster: 2020-21 will match sample, 2021-22 will not.
    roster = _roster([
        ("hh1", "2020-21"),
        ("hh2", "2020-21"),
        ("hh3", "2021-22"),
        ("hh4", "2021-22"),
    ])

    # Sample has matching i for 2020-21 but unrelated i for 2021-22.
    sample = _sample([
        ("hh1", "2020-21", "cluster_A"),
        ("hh2", "2020-21", "cluster_B"),
        ("xx9", "2021-22", "cluster_C"),
        ("xx0", "2021-22", "cluster_D"),
    ])
    _set_synthetic_sample(country, sample)

    with warnings.catch_warnings(record=True) as wlist:
        warnings.simplefilter("always")
        result = country._join_v_from_sample(roster)

    silent_skip = [w for w in wlist if "100% NaN v" in str(w.message)]
    assert len(silent_skip) == 1, (
        f"Expected exactly one silent-skip warning; "
        f"got {len(silent_skip)}: {[str(w.message) for w in wlist]}"
    )
    msg = str(silent_skip[0].message)
    assert "2021-22" in msg, f"Warning should name the affected wave; got: {msg}"
    assert "2020-21" not in msg, f"Warning should NOT name the matching wave; got: {msg}"
    # Sanity: result is still a DataFrame and 2021-22 rows have NaN v
    nan_v_2021 = result.xs("2021-22", level="t")["value"]  # noqa: F841 -- existence check
    assert "v" in (result.index.names or []), \
        f"v should be in index names: {result.index.names}"


def test_no_warn_when_all_waves_match():
    """When every wave has full sample coverage, no warning fires."""
    country = _make_country()

    roster = _roster([
        ("hh1", "2020-21"),
        ("hh2", "2020-21"),
        ("hh3", "2021-22"),
        ("hh4", "2021-22"),
    ])
    sample = _sample([
        ("hh1", "2020-21", "cluster_A"),
        ("hh2", "2020-21", "cluster_B"),
        ("hh3", "2021-22", "cluster_C"),
        ("hh4", "2021-22", "cluster_D"),
    ])
    _set_synthetic_sample(country, sample)

    with warnings.catch_warnings(record=True) as wlist:
        warnings.simplefilter("always")
        country._join_v_from_sample(roster)

    silent_skip = [w for w in wlist if "100% NaN v" in str(w.message)]
    assert not silent_skip, (
        f"Should not warn when all waves match; got: "
        f"{[str(w.message) for w in silent_skip]}"
    )


def test_no_warn_on_partial_nan():
    """Partial NaN-v (e.g., movers / split-offs in Uganda 2009-10's hybrid-v)
    should NOT trigger the warning -- those are downstream-recovered by
    ``_add_market_index``'s HH-level fallback (see test_sample.py
    ``TestUganda2009MarketFallback``).
    """
    country = _make_country()

    # 2 of 4 roster rows match sample; 50% NaN v for the wave.
    roster = _roster([
        ("hh1", "2020-21"),
        ("hh2", "2020-21"),
        ("mover1", "2020-21"),  # split-off; not in sample
        ("mover2", "2020-21"),  # split-off; not in sample
    ])
    sample = _sample([
        ("hh1", "2020-21", "cluster_A"),
        ("hh2", "2020-21", "cluster_B"),
    ])
    _set_synthetic_sample(country, sample)

    with warnings.catch_warnings(record=True) as wlist:
        warnings.simplefilter("always")
        country._join_v_from_sample(roster)

    silent_skip = [w for w in wlist if "100% NaN v" in str(w.message)]
    assert not silent_skip, (
        f"Should not warn on partial NaN; got: "
        f"{[str(w.message) for w in silent_skip]}"
    )


def test_warns_only_for_fully_nan_waves_when_mixed():
    """When some waves are fully NaN and others partial / matched, only the
    fully-NaN ones appear in the warning message."""
    country = _make_country()

    roster = _roster([
        ("hh1", "2020-21"),  # matches
        ("hh2", "2020-21"),  # matches
        ("xx1", "2021-22"),  # 2021-22 fully missing from sample
        ("xx2", "2021-22"),
        ("hh5", "2022-23"),  # 2022-23 partial: 1 of 2 matches
        ("xx3", "2022-23"),
    ])
    sample = _sample([
        ("hh1", "2020-21", "cluster_A"),
        ("hh2", "2020-21", "cluster_B"),
        # 2021-22: nothing
        ("hh5", "2022-23", "cluster_E"),
    ])
    _set_synthetic_sample(country, sample)

    with warnings.catch_warnings(record=True) as wlist:
        warnings.simplefilter("always")
        country._join_v_from_sample(roster)

    silent_skip = [w for w in wlist if "100% NaN v" in str(w.message)]
    assert len(silent_skip) == 1, (
        f"Expected exactly one silent-skip warning; "
        f"got {len(silent_skip)}: {[str(w.message) for w in wlist]}"
    )
    msg = str(silent_skip[0].message)
    assert "2021-22" in msg, f"Warning should name 2021-22; got: {msg}"
    assert "2020-21" not in msg, f"Warning should not name fully-matched 2020-21; got: {msg}"
    assert "2022-23" not in msg, f"Warning should not name partially-matched 2022-23; got: {msg}"


def test_no_warn_when_sample_unavailable():
    """If sample() build failed (cache=False), the function returns df early
    and emits no silent-skip warning -- a different failure mode (sample
    unavailable) is logged via logger.info, not the silent-skip path."""
    country = _make_country()
    country._sample_v_cache = False  # sample() failed to build

    roster = _roster([
        ("hh1", "2020-21"),
        ("hh2", "2020-21"),
    ])

    with warnings.catch_warnings(record=True) as wlist:
        warnings.simplefilter("always")
        result = country._join_v_from_sample(roster)

    silent_skip = [w for w in wlist if "100% NaN v" in str(w.message)]
    assert not silent_skip, (
        f"Should not warn when sample is unavailable (cache=False); got: "
        f"{[str(w.message) for w in silent_skip]}"
    )
    # And the function should return df unchanged.
    assert result.index.names == ["i", "t"], (
        f"When cache is False, df should pass through unchanged; "
        f"got index names {result.index.names}"
    )
