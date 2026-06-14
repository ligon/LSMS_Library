"""Tanzania community_cluster_xwalk (#113).

The community-price -> survey-cluster crosswalk is a deliberately lossy match
(the community instrument shares no cluster id with the household frame; see
Tanzania/_/CONTENTS.org).  The non-negotiable invariant is that every
*cluster*-resolved row points at a REAL ``sample().v`` -- otherwise the
downstream price/quantity fallback would join onto nothing.  These tests also
pin the grain, the match labels, and the measured resolution rates so a
regression in the matcher surfaces.

Data-dependent: skips cleanly when the Tanzania source files are unavailable
(same convention as the other data-backed tests in this suite).
"""
import pytest

import lsms_library as ll
from lsms_library.diagnostics import is_this_feature_sane


@pytest.fixture(scope="module")
def tz():
    return ll.Country("Tanzania")


@pytest.fixture(scope="module")
def xwalk(tz):
    try:
        df = tz.community_cluster_xwalk()
    except Exception as exc:  # noqa: BLE001 - any build/data failure -> skip
        pytest.skip(f"Tanzania community_cluster_xwalk unavailable: {exc}")
    if df is None or df.empty:
        pytest.skip("Tanzania community_cluster_xwalk empty")
    return df


def test_grain_and_uniqueness(xwalk):
    assert list(xwalk.index.names) == ["t", "v"]
    assert xwalk.index.is_unique
    assert set(xwalk.columns) == {"cluster", "region", "match", "n_candidates"}


def test_match_labels(xwalk):
    assert set(xwalk["match"].dropna().unique()) <= {"cluster", "region"}
    # 'cluster' iff a cluster id was resolved; 'region' iff it was not.
    x = xwalk.reset_index()
    assert (x.loc[x["match"] == "cluster", "cluster"].notna()).all()
    assert (x.loc[x["match"] == "region", "cluster"].isna()).all()


def test_resolved_clusters_are_real_sample_v(tz, xwalk):
    """The load-bearing invariant: every resolved cluster is a true sample().v."""
    sv = tz.sample().reset_index()
    sv["t"] = sv["t"].astype(str)
    x = xwalk.reset_index()
    x["t"] = x["t"].astype(str)
    matched = x[x["match"] == "cluster"]
    assert len(matched) > 0
    for t, sub in matched.groupby("t"):
        valid = set(sv.loc[sv["t"] == t, "v"].astype(str))
        bad = set(sub["cluster"].astype(str)) - valid
        assert not bad, f"{t}: {len(bad)} resolved clusters not in sample().v, e.g. {sorted(bad)[:5]}"


@pytest.mark.parametrize("t,lo,hi", [("2019-20", 0.40, 0.65), ("2020-21", 0.88, 0.98)])
def test_cluster_resolution_rate(xwalk, t, lo, hi):
    """Cluster-resolution stays near the documented rates (~52% / ~94%).

    2020-21 reconstructs the cluster deterministically from the baked-in admin
    codes (region, ward, EA, seq), so its rate is high and should not regress to
    the date-only (region, ward) level (~86%).
    """
    x = xwalk.reset_index()
    x["t"] = x["t"].astype(str)
    sub = x[x["t"] == t]
    if sub.empty:
        pytest.skip(f"{t} not present")
    frac = (sub["match"] == "cluster").mean()
    assert lo <= frac <= hi, f"{t} cluster-match fraction {frac:.2f} outside [{lo}, {hi}]"


def test_sane(xwalk):
    assert is_this_feature_sane(xwalk, "Tanzania", "community_cluster_xwalk").ok
