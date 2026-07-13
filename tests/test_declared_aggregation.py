"""GH #323 -- declared aggregation policies for non-unique canonical indexes.

Two things are under test:

1. The *mechanism*: ``aggregation:`` in ``data_scheme.yml`` turns an undeclared,
   silent ``groupby().first()`` collapse into an enforced contract.  The guards
   must actually FIRE -- a guard that cannot fail is not a guard, so every
   raise-path here is exercised with an injected violation.

2. The *Kazakhstan instance*: KZ96*.dta are person-level files feeding
   household- and cluster-level tables.  ``first()`` silently returned the
   MINORITY value for cluster 126's ``Rural`` (50 households urban, 1 rural).
"""
import warnings

import pandas as pd
import pytest

from lsms_library.country import (
    _AGGREGATION_POLICIES,
    _collapse_with_policy,
    _declared_aggregation,
)


# ---------------------------------------------------------------------------
# The vocabulary
# ---------------------------------------------------------------------------

def test_unknown_policy_raises():
    """A typo must fail loudly, not fall back to the lossy legacy default."""
    with pytest.raises(ValueError, match="Unknown aggregation policy"):
        _declared_aggregation({"aggregation": "frist"})


def test_absent_policy_is_none():
    assert _declared_aggregation({"index": "(t, i)"}) is None
    assert _declared_aggregation(None) is None


def test_legacy_mapping_aggregation_is_not_a_323_policy():
    """``aggregation: {visit: first}`` predates GH #323 and means something else.

    Albania, Malawi, Senegal, Niger, Togo, Benin, Burkina_Faso, CotedIvoire and
    Guinea-Bissau all declare the mapping form on ``interview_date``.  It must
    NOT be read as a duplicate-collapse policy, and must not raise.
    """
    assert _declared_aggregation({"aggregation": {"visit": "first"}}) is None


def test_all_policies_are_implemented():
    """Every name in the vocabulary must be dispatchable."""
    df = pd.DataFrame({"x": [1, 1]}, index=pd.Index(["a", "a"], name="i"))
    for policy in _AGGREGATION_POLICIES:
        out = _collapse_with_policy(df, ["i"], policy, "t")
        assert len(out) == 1, policy


# ---------------------------------------------------------------------------
# `unique` -- verify-then-collapse.  The guard must FIRE.
# ---------------------------------------------------------------------------

def test_unique_collapses_a_value_preserving_projection():
    """Redundant person rows carrying their household's attributes."""
    df = pd.DataFrame(
        {"v": ["c1", "c1", "c2"], "Rural": ["Urban", "Urban", "Rural"]},
        index=pd.Index(["h1", "h1", "h2"], name="i"),
    )
    out = _collapse_with_policy(df, ["i"], "unique", "sample")
    assert len(out) == 2
    assert out.loc["h1", "v"] == "c1"


def test_unique_RAISES_when_the_projection_would_lose_information():
    """NON-VACUOUS GUARD: injected conflict must raise, not silently pick one."""
    df = pd.DataFrame(
        {"v": ["c1", "c9"]},  # same household, two different clusters
        index=pd.Index(["h1", "h1"], name="i"),
    )
    with pytest.raises(ValueError, match="value-preserving|disagree"):
        _collapse_with_policy(df, ["i"], "unique", "sample")


def test_unique_tolerates_nulls_within_a_group():
    """A missing value is not a conflict; it is just missing."""
    df = pd.DataFrame(
        {"v": ["c1", None]},
        index=pd.Index(["h1", "h1"], name="i"),
    )
    out = _collapse_with_policy(df, ["i"], "unique", "sample")
    assert out.loc["h1", "v"] == "c1"


# ---------------------------------------------------------------------------
# `dedupe` -- identical-or-raise.  The guard must FIRE.
# ---------------------------------------------------------------------------

def test_dedupe_drops_exact_duplicate_records():
    """Kazakhstan rn=805: byte-identical copies of persons 2/3/4."""
    df = pd.DataFrame(
        {"Sex": ["M", "M", "F"], "Age": [51, 51, 14]},
        index=pd.MultiIndex.from_tuples(
            [("805", 2), ("805", 2), ("805", 4)], names=["i", "pid"]
        ),
    )
    out = _collapse_with_policy(df, ["i", "pid"], "dedupe", "household_roster")
    assert len(out) == 2
    assert out.loc[("805", 2), "Age"] == 51


def test_dedupe_RAISES_on_a_non_identical_collision():
    """NON-VACUOUS GUARD: two DIFFERENT people sharing (i, pid) is not a dupe.

    This is the whole point: today's first() would silently keep one and drop
    the other.  A future source change that produces a real collision must
    become a loud error (class-2), not a silent wrong answer (class-1).
    """
    df = pd.DataFrame(
        {"Sex": ["M", "F"], "Age": [51, 14]},  # same (i, pid), different people
        index=pd.MultiIndex.from_tuples(
            [("805", 2), ("805", 2)], names=["i", "pid"]
        ),
    )
    with pytest.raises(ValueError, match="NOT identical"):
        _collapse_with_policy(df, ["i", "pid"], "dedupe", "household_roster")


# ---------------------------------------------------------------------------
# `mode` -- stated majority, never silent.
# ---------------------------------------------------------------------------

def test_mode_takes_the_majority_not_the_first_row():
    """The Kazakhstan cluster-126 shape: first row is the minority value."""
    df = pd.DataFrame(
        {"Rural": ["Rural"] + ["Urban"] * 50},  # minority FIRST
        index=pd.Index(["c126"] * 51, name="v"),
    )
    with pytest.warns(RuntimeWarning, match="disagree"):
        out = _collapse_with_policy(df, ["v"], "mode", "cluster_features")
    assert out.loc["c126", "Rural"] == "Urban", "mode must not return first()"


def test_mode_warns_naming_the_disagreeing_groups():
    df = pd.DataFrame(
        {"Rural": ["Rural", "Urban", "Urban"]},
        index=pd.Index(["c126", "c126", "c126"], name="v"),
    )
    with pytest.warns(RuntimeWarning) as rec:
        _collapse_with_policy(df, ["v"], "mode", "cluster_features")
    msg = str(rec[0].message)
    assert "c126" in msg and "Rural" in msg


def test_mode_is_silent_when_the_source_agrees():
    df = pd.DataFrame(
        {"Rural": ["Urban", "Urban"]},
        index=pd.Index(["c1", "c1"], name="v"),
    )
    with warnings.catch_warnings():
        warnings.simplefilter("error")  # any warning fails the test
        out = _collapse_with_policy(df, ["v"], "mode", "cluster_features")
    assert out.loc["c1", "Rural"] == "Urban"


def test_mode_preserves_gps_centroid_override():
    """Latitude/Longitude stay a mean, not a majority vote."""
    df = pd.DataFrame(
        {"Rural": ["Urban", "Urban"], "Latitude": [10.0, 20.0]},
        index=pd.Index(["c1", "c1"], name="v"),
    )
    out = _collapse_with_policy(
        df, ["v"], "mode", "cluster_features", col_overrides={"Latitude": "mean"}
    )
    assert out.loc["c1", "Latitude"] == 15.0


# ---------------------------------------------------------------------------
# Kazakhstan, end to end.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def kazakhstan():
    ll = pytest.importorskip("lsms_library")
    try:
        return ll.Country("Kazakhstan")
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"Kazakhstan unavailable: {exc}")


def test_kz_cluster_126_rural_is_the_majority_value(kazakhstan):
    """THE regression. Pre-fix this is 'Rural' -- the MINORITY of 51 households.

    KZ96SMP_PUF codes cluster 126 as urban for 50 households and rural for 1
    (rn=1264).  groupby().first() shipped 'Rural'.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cf = kazakhstan.cluster_features().reset_index()
    row = cf[cf["v"].astype(str).str.strip() == "126"]
    assert len(row) == 1
    assert row["Rural"].iloc[0] == "Urban"


def test_kz_no_silent_first_collapse_warning(kazakhstan):
    """No table may still be collapsing via the undeclared first() path."""
    for table in ("sample", "household_roster", "individual_education"):
        with warnings.catch_warnings(record=True) as rec:
            warnings.simplefilter("always")
            getattr(kazakhstan, table)()
        silent = [
            str(w.message) for w in rec
            if "collapsed via groupby().first()" in str(w.message)
        ]
        assert not silent, f"{table} still silently collapsing: {silent}"


def test_kz_entity_counts_are_preserved(kazakhstan):
    """The KLSS 1996 design: 1,996 households / 135 clusters."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        smp = kazakhstan.sample()
        cf = kazakhstan.cluster_features()
    assert smp.reset_index()["i"].nunique() == 1996
    assert len(cf) == 135


def test_kz_roster_drops_only_the_identical_duplicates(kazakhstan):
    """rn=805's 3 duplicate copies go; every real person stays.

    7,224 source rows - 3 identical copies - 1 blank placeholder row
    (rn=26/pid=5: sex, age, monhh all NaN) = 7,220.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        hr = kazakhstan.household_roster()
    assert len(hr) == 7220
    assert hr.index.is_unique
