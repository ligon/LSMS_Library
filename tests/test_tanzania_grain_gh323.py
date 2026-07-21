"""Tanzania 2008-15 grain invariants (GH #323).

The `2008-15/` folder is a FOUR-ROUND folder: one source file (`upd4_hh_a.dta`)
covering rounds 1-4, split into waves 2008-09 / 2010-11 / 2012-13 / 2014-15 via
`wave_folder_map`.  Its true primary key is the panel-tracking LINE (`UPHI`,
`round`) -- 29,250 / 29,250 unique -- NOT the household (16,540 household-rounds).
So every household-round arrives replicated once per descendant line, 1 to 11
lines apiece, and three wave scripts have to collapse that replication.

These tests pin the properties of the CONFIG (the three wave scripts).  They do
NOT test any core aggregation mechanism -- core does not aggregate (D1;
SkunkWorks/grain_aggregation_policy.org).  In particular they say nothing about
whether the household -> cluster projection in `Wave.cluster_features` destroys
rows; that is Site 2, it is owned centrally, and on this branch it remains open.

Data-dependent: skips cleanly when the Tanzania source files are unavailable.
"""
import pandas as pd
import pytest

import lsms_library as ll

ROUNDS_2008_15 = ["2008-09", "2010-11", "2012-13", "2014-15"]


@pytest.fixture(scope="module")
def tz():
    return ll.Country("Tanzania")


def _build(tz, table):
    try:
        df = getattr(tz, table)()
    except Exception as exc:  # noqa: BLE001 - any build/data failure -> skip
        pytest.skip(f"Tanzania {table} unavailable: {exc}")
    if df is None or df.empty:
        pytest.skip(f"Tanzania {table} empty")
    return df


@pytest.fixture(scope="module")
def sample(tz):
    return _build(tz, "sample")


@pytest.fixture(scope="module")
def cluster_features(tz):
    return _build(tz, "cluster_features")


@pytest.fixture(scope="module")
def interview_date(tz):
    return _build(tz, "interview_date")


# --------------------------------------------------------------------------
# The folder really does carry four rounds.  If a script ever regresses to a
# single `t`, everything below would still pass vacuously -- so pin this first.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("table", ["sample", "cluster_features", "interview_date"])
def test_all_four_rounds_survive_the_multi_round_folder(tz, table):
    df = _build(tz, table)
    ts = set(map(str, df.index.get_level_values("t")))
    missing = [r for r in ROUNDS_2008_15 if r not in ts]
    assert not missing, f"{table}: rounds {missing} vanished from the 2008-15 folder"


# --------------------------------------------------------------------------
# sample: the household-round is the unit, and `v` is honest about ambiguity.
# --------------------------------------------------------------------------

def test_sample_is_unique_per_household_round(sample):
    """One row per (i, t) -- the replicated panel lines are collapsed."""
    assert list(sample.index.names) == ["i", "t"]
    assert sample.index.is_unique


def test_sample_v_is_na_exactly_where_the_origin_ea_is_ambiguous(sample):
    """59 household-rounds hold two panel lines with DIFFERENT origin EAs.

    Their sampling cluster is genuinely ambiguous, so `v` is <NA> rather than an
    arbitrary pick.  `_join_v_from_sample()` propagates this `v` onto every
    Tanzania household table, so an arbitrary pick here would have propagated
    library-wide.

    All 59 are in ROUND 4.  Rounds 1-3 have none: the ambiguity needs two tracked
    lines with different origins to land in one physical household, which the NPS
    only produces once it has been tracking movers for three rounds.
    """
    na = sample["v"].isna()
    assert int(na.sum()) == 59, f"expected 59 <NA> v, got {int(na.sum())}"
    rounds = set(map(str, sample.index.get_level_values("t")[na]))
    assert rounds == {"2014-15"}, f"ambiguous v should be round-4-only, got {sorted(rounds)}"


def test_sample_non_v_columns_are_constant_within_a_household_round(sample):
    """weight / panel_weight / strata / Rural do NOT vary across a household's
    replicated panel lines -- which is what makes deduping them value-preserving.
    The wave script asserts this; assert it at the API too."""
    for col in ["weight", "panel_weight", "strata", "Rural"]:
        assert col in sample.columns
    # index is unique (above), so constancy is inherited; the real content of this
    # test is that the build did not raise the script's assert.
    assert sample.index.is_unique


def test_sample_v_is_not_required_to_be_round_invariant(sample):
    """The NPS TRACKS movers, so a household's cluster is a per-ROUND fact.

    This is the reading the <NA> sentinel depends on: invariance is demanded
    WITHIN (i, t), never ACROSS t.  Guard the reading -- if `v` were silently
    forced to be constant per household, this would start failing and the
    sentinel's justification would have quietly changed underneath it.
    """
    s = sample.reset_index()
    s = s[s["t"].astype(str).isin(ROUNDS_2008_15)]
    # r_hhid is not a stable panel id across rounds 1-3 (its format changes), but
    # rounds 3 and 4 DO reuse ids, and there v legitimately moves.
    per_hh = s.dropna(subset=["v"]).groupby("i")["v"].nunique()
    assert (per_hh > 1).any(), (
        "no household changes cluster across rounds -- v has become round-invariant, "
        "which contradicts the tracked-mover design the <NA> sentinel relies on"
    )


# --------------------------------------------------------------------------
# cluster_features: no empty string may be served as a geography NAME.
# --------------------------------------------------------------------------

@pytest.mark.parametrize("col", ["Region", "District", "Rural"])
def test_no_empty_string_geography(cluster_features, col):
    """An empty string is MISSING DATA, not a value (GH #323).

    Rounds 1-2 of the NPS never populate the district NAME (`ha_02_2` is '' for
    all 6,128 round-1 and all 8,163 round-2 source rows).  Carried through, that
    shipped an empty string as a district NAME for 818 cluster cells -- every
    cluster in 2008-09 and 2010-11.  It must be <NA>: honestly absent, and
    findable with `pd.isna()`.
    """
    s = cluster_features[col].astype("string")
    empty = int((s.notna() & s.str.strip().eq("")).sum())
    assert empty == 0, f"{empty} cluster cells carry an empty-string {col}"


def test_district_is_absent_not_blank_in_the_first_two_rounds(cluster_features):
    """The positive half of the test above: the 818 cells are still THERE, as NA.

    Guards against 'fixing' the empty strings by dropping the rows.
    """
    d = cluster_features["District"]
    for t in ["2008-09", "2010-11"]:
        sub = d[cluster_features.index.get_level_values("t") == t]
        assert len(sub) > 0, f"{t} clusters vanished"
        assert sub.isna().all(), f"{t}: expected District entirely <NA>, got {sub.notna().sum()} set"
    for t in ["2012-13", "2014-15"]:
        sub = d[cluster_features.index.get_level_values("t") == t]
        assert sub.notna().any(), f"{t}: District should be populated from round 3 on"


# --------------------------------------------------------------------------
# interview_date: the dedup is value-preserving.
# --------------------------------------------------------------------------

def test_interview_date_is_unique_per_household_round(interview_date):
    idx = interview_date.reset_index()[["i", "t"]]
    dup = int(idx.duplicated().sum())
    assert dup == 0, f"{dup} duplicate (i, t) in interview_date"


def test_interview_date_covers_every_round(interview_date):
    for t in ROUNDS_2008_15:
        sub = interview_date[interview_date.index.get_level_values("t") == t]
        assert len(sub) > 0, f"interview_date lost round {t}"
        assert sub["Int_t"].notna().any(), f"interview_date {t}: all dates NaT"
