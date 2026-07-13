"""Regression net for GH #323 -- Pakistan 1991 cluster_features.

THE BUG
-------
`cluster_features` was extracted from F00A.DTA (the household COVER SHEET) with
`idxvars: {v: clust, i: hid}`, emitting one row per HOUSEHOLD (4,957) into a
table whose declared index is (t, v) (301 clusters).  The surplus `i` level was
then collapsed away, silently, discarding 4,656 rows -- and the columns it
carried were not cluster attributes at all: `Region` was wired to the
household's RELIGION code and `Language` to the language OF THE INTERVIEW.

WHERE IT COLLAPSED (this determines what these tests can assert)
----------------------------------------------------------------
NOT in `_normalize_dataframe_index` -- that is the GH #323 site, and it *warns*.
For `cluster_features` the collapse happens EARLIER and SILENTLY, in
`Wave.cluster_features()` (country.py ~1168, added for GH #161)::

    if 'i' in df.index.names:
        agg = {c: ('mean' if c in ('Latitude','Longitude') else 'first') ...}
        df = df.groupby(level=keep_levels).agg(agg)     # no warning, ever

That branch justifies `.first()` on the premise that "Region/Rural/District are
invariant within a cluster by construction of the LSMS-ISA sampling design" -- a
precondition it never checks.  Pakistan violated it outright: `religion` varies
within 75/301 clusters and `langint` within 141/301, so `.first()` was not
deduplicating identical rows, it was electing one arbitrary household as
spokesperson for its whole cluster (1,101 households disagreed with the value
assigned to their cluster).  By the time `_normalize_dataframe_index` runs, the
frame is already 301 rows and unique, so the GH #323 warning NEVER fires -- warm
or cold.

CONSEQUENCE FOR TEST DESIGN
---------------------------
Two tempting tests are VACUOUS here and are deliberately not written:
  * asserting the API index is unique -- post-collapse it is *always* unique;
  * asserting no GH #323 warning fires -- it never fires for this table anyway.
The honest detector is at the EXTRACTION: the wave extraction must already be
cluster-grain, so the silent reducer has nothing to collapse.  That, plus the
semantic content of the columns, is what these tests assert.  All of them fail
on the pre-fix tree.
"""
from pathlib import Path

import pandas as pd
import pytest

import lsms_library as ll

# Value labels from Data/REGIONS.TXT -- the codebook shipped with the survey.
PROVINCES = {"Punjab", "Sind", "NWFP", "Balochistan"}
RURAL = {"Urban", "Rural"}

# Cluster 2202029 is a single-household cluster entirely absent from REGIONS.DTA:
# the survey never recorded its region.  It is deliberately NOT imputed.  Its
# province *is* inferable from the `clust` prefix, but that rule can only be
# validated on the clusters where province is already known and is untestable on
# the one cluster where it would actually be needed -- so using it would be a
# guess.  Honestly missing beats silently wrong (GH #323).
CLUSTER_WITHOUT_REGION = "2202029"

N_CLUSTERS = 301          # distinct `clust` in F00A.DTA
N_WITH_REGION = 300       # ... of which have a REGIONS.DTA row
N_HOUSEHOLDS = 4957       # distinct `hid` -- the grain the bug leaked


@pytest.fixture(scope="module")
def country():
    return ll.Country("Pakistan")


@pytest.fixture(scope="module")
def api(country):
    return country.cluster_features()


def test_extraction_is_cluster_grain(api):
    """THE structural regression test.

    Asserts on the wave-level artifact -- the frame that is FED to the silent
    reducer.  It must already be one row per CLUSTER.  Pre-fix it was one row per
    HOUSEHOLD (4,957 rows indexed (t, v, i)), which is exactly what gave
    `Wave.cluster_features()`'s `groupby().first()` something to collapse.  If
    `i` is ever reintroduced into this table's idxvars, this test fails and the
    silent reducer is back.

    Depends on `api` so the wave parquet is materialized before we read it.
    """
    from lsms_library.local_tools import data_root

    pq = Path(data_root()) / "Pakistan" / "1991" / "_" / "cluster_features.parquet"
    assert pq.exists(), f"wave-level cluster_features parquet not materialized at {pq}"
    df = pd.read_parquet(pq)

    assert "i" not in (df.index.names or []), (
        "cluster_features is extracted at HOUSEHOLD grain (an `i` index level); "
        "it will be silently collapsed by Wave.cluster_features() -- GH #323")
    assert "i" not in df.columns
    assert len(df) == N_CLUSTERS, (
        f"expected {N_CLUSTERS} cluster rows, got {len(df)} "
        f"({N_HOUSEHOLDS} would mean household grain leaked back in)")
    assert df.index.is_unique


def test_index_is_cluster_grain(api):
    """The API is keyed on (t, v), one row per cluster with region data."""
    assert list(api.index.names) == ["t", "v"]
    assert api.index.is_unique
    assert int(api.index.duplicated().sum()) == 0
    assert len(api) == N_WITH_REGION, (
        f"expected {N_WITH_REGION} clusters with region data, got {len(api)}")


def test_region_is_province_not_religion(api):
    """Region must be geography.

    Pre-fix, `Region` was wired to `religion`; its values were the strings
    "1.0"/"2.0"/"3.0", and 291 of 301 clusters were literally "1.0".
    """
    values = set(api["Region"].dropna().unique())
    assert values <= PROVINCES, f"Region carries non-province values: {values - PROVINCES}"
    # All four provinces must be represented -- guards against the column
    # degenerating back to a single constant value.
    assert values == PROVINCES, f"missing provinces: {PROVINCES - values}"


def test_rural_is_populated(api):
    """`Rural` is canonically `required: true`; pre-fix it was never populated."""
    assert "Rural" in api.columns
    assert set(api["Rural"].dropna().unique()) == RURAL
    assert api["Rural"].notna().all()


def test_language_is_gone(api):
    """`Language` was `langint`, the language OF THE INTERVIEW.

    It varies within 141/301 clusters, so it is not a cluster attribute at all;
    a modal reducer would invent an attribute the survey never measured.
    """
    assert "Language" not in api.columns


def test_cluster_without_region_is_absent_not_imputed(api):
    """The one cluster with no REGIONS.DTA row must not acquire a guessed province."""
    clusters = set(api.index.get_level_values("v"))
    assert CLUSTER_WITHOUT_REGION not in clusters, (
        f"cluster {CLUSTER_WITHOUT_REGION} has no region data in REGIONS.DTA; "
        "it must not be imputed (GH #323)")


def test_geography_is_constant_within_cluster(country):
    """The premise that makes the cluster-level reduction lossless.

    `province`/`urbrural` must be invariant within a cluster -- this is what
    `Wave.cluster_features()`'s `.first()` *assumes* and never checks, and what
    Pakistan's old religion/langint columns violated.  Verified here against the
    raw source so the assumption is enforced, not asserted in prose.
    """
    from lsms_library.local_tools import get_dataframe

    reg = get_dataframe(
        "lsms_library/countries/Pakistan/1991/Data/REGIONS.DTA"
    ).dropna(subset=["hhcode"])
    reg["clust"] = reg["hhcode"].astype("int64").astype(str).str[:7]
    for col in ("province", "urbrural"):
        varying = reg.groupby("clust")[col].nunique(dropna=True)
        assert (varying <= 1).all(), (
            f"{col} varies within {(varying > 1).sum()} cluster(s); it is not a "
            f"cluster-level attribute and must not be reduced with .first()")


def test_v_aligns_with_sample(country, api):
    """cluster_features owns `v`; every cluster it reports must exist in sample()."""
    sample_clusters = set(country.sample()["v"].dropna().unique())
    cf_clusters = set(api.index.get_level_values("v"))
    assert cf_clusters <= sample_clusters, (
        f"cluster_features reports clusters absent from sample(): "
        f"{sorted(cf_clusters - sample_clusters)[:5]}")
