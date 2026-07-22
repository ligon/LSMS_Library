"""Tanzania `cluster_features` cluster key (GH #323, Site 2).

Tanzania was the worst Site-2 offender in the corpus: 2,104 of 7,167
cluster-attribute cells contested (29.4%), 65.4% of them in 2012-13.  The cause
was not an ambiguous cluster code.  It was that every wave read the cluster's
Region / District / Rural off the household COVER PAGE -- the address where the
household was INTERVIEWED -- while the NPS is a TRACKING panel that follows
movers and split-offs and carries their ORIGINAL cluster `v` forward unchanged.
So a household that left the cluster was still being asked to describe it.

Two things are pinned here.

1.  **The identifier decodes.**  The NPS cluster id is a hierarchical geocode
    whose leading fields ARE the cluster's region and district.  Every test of
    residency rests on that, so it is asserted directly against the 2008-09
    frame -- round 1, the only pollution-free round, because nobody has moved
    yet.
2.  **The cluster grain is described from inside the cluster.**  Contested
    cells per wave, and the key agreements that make the table joinable.

These are CONFIG-level tests.  They exercise no core aggregation mechanism --
core does not aggregate (D1, SkunkWorks/grain_aggregation_policy.org).

Data-dependent: skips cleanly when the Tanzania source files are unavailable.
"""
import re
import warnings

import pandas as pd
import pytest

import lsms_library as ll

WAVES = ["2008-09", "2010-11", "2012-13", "2014-15", "2019-20", "2020-21"]
ATTRS = ["Region", "District", "Rural"]

# Contested-cell ceilings, per wave, measured cold (isolated LSMS_DATA_DIR,
# L2 rebuilt) on this branch.  The BEFORE column is what `development` produces.
#
#   wave      before   after
#   2008-09        0       0
#   2010-11      329      78
#   2012-13      803     147
#   2014-15      160      41
#   2019-20      257      72
#   2020-21      557      47
#
# The ceilings are the measured values.  They are upper bounds, not equalities:
# a change that lowers them further should not fail, but a regression toward the
# `development` numbers must.  The residue is characterised in
# Tanzania/_/CONTENTS.org -- district RENAMINGS and SPLITS carrying two names for
# one place, and EAs whose households are not all classified alike on urban /
# rural.  Neither is a key defect and neither is forced to zero here.
CONTESTED_CEILING = {
    "2008-09": 0,
    "2010-11": 78,
    "2012-13": 147,
    "2014-15": 41,
    "2019-20": 72,
    "2020-21": 47,
}

# Cluster counts per wave.  2019-20 FALLS 247 -> 147 (100 of its "clusters"
# existed only because split-off rows were pooled under a stray code) and
# 2020-21 RISES 418 -> 515 (the booster clusters stop being deleted on a NaN
# `clusterid` key).
CLUSTERS = {
    "2008-09": 409,
    "2010-11": 409,
    "2012-13": 409,
    "2014-15": 498,
    "2019-20": 147,
    "2020-21": 515,
}


@pytest.fixture(scope="module")
def tz():
    return ll.Country("Tanzania")


def _build(tz, table):
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = getattr(tz, table)()
    except Exception as exc:  # noqa: BLE001 - any build/data failure -> skip
        pytest.skip(f"Tanzania {table} unavailable: {exc}")
    if df is None or df.empty:
        pytest.skip(f"Tanzania {table} empty")
    return df


@pytest.fixture(scope="module")
def cluster_features(tz):
    return _build(tz, "cluster_features")


@pytest.fixture(scope="module")
def sample(tz):
    return _build(tz, "sample")


@pytest.fixture(scope="module")
def geocode():
    """The country module's geocode helpers, loaded the way the config does."""
    import importlib.util
    from lsms_library.paths import countries_root

    path = countries_root() / "Tanzania" / "_" / "tanzania.py"
    if not path.exists():           # pragma: no cover - config always shipped
        pytest.skip("Tanzania country module unavailable")
    spec = importlib.util.spec_from_file_location("_tz_geocode_probe", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------
# 1.  The identifier decodes.  Everything else rests on this.
# --------------------------------------------------------------------------

def test_geocode_region_field_is_documented_for_every_scheme(geocode):
    """The scheme is a property of the WAVE, never of the string's length.

    2019-20's 8-character '11014002' is the 9-digit '011014002' (DODOMA), NOT
    the 8-digit '11014002' (IRINGA).  Reading it under the wrong scheme
    relabels a whole region silently, so pin both readings.
    """
    assert geocode.cluster_region("11014002", scheme="sdd") == "DODOMA"
    assert geocode.cluster_region("11014002", scheme="nps") == "IRINGA"
    assert geocode.cluster_region("02-02-033-04-004", scheme="y5") == "ARUSHA"
    # The 2014-15 refresh sample is drawn under the 12-digit form, and the
    # 'nps' scheme has to pick by length within the same wave.
    assert geocode.cluster_region("20203304004", scheme="nps") == "ARUSHA"
    assert pd.isna(geocode.cluster_region(None))
    assert pd.isna(geocode.cluster_region(float("nan")))


def test_geocode_district_field(geocode):
    assert geocode.cluster_district_code("11014002", scheme="sdd") == "1"
    assert geocode.cluster_district_code("02-02-033-04-004", scheme="y5") == "2"


def test_every_region_code_has_exactly_one_name(geocode):
    """A code that named two regions would make the residency test meaningless."""
    names = list(geocode.TZ_REGION_BY_CODE.values())
    assert len(names) == len(set(names)), "a region name is claimed by two codes"
    assert all(re.fullmatch(r"\d{2}", c) for c in geocode.TZ_REGION_BY_CODE)


def test_the_geocode_agrees_with_the_pollution_free_frame(geocode):
    """Round 1 is the ground truth: nobody has moved, so every household's
    reported region IS its cluster's region.  If the crosswalk were wrong this
    would be the first thing to know."""
    from lsms_library.local_tools import get_dataframe
    from lsms_library.paths import countries_root

    fn = countries_root() / "Tanzania" / "2008-15" / "Data" / "upd4_hh_a.dta"
    try:
        raw = get_dataframe(str(fn))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Tanzania 2008-15 source unavailable: {exc}")
    r1 = raw[raw["round"] == 1]
    cid = r1["clusterid"].astype(str).str.replace(r"\.0$", "", regex=True)
    decoded = cid.map(lambda x: geocode.cluster_region(x, "nps"))
    reported = r1["ha_01_1"].map(geocode.normalize_place)
    agree = (decoded.map(geocode.normalize_place) == reported)
    assert agree.mean() == 1.0, (
        f"the cluster geocode disagrees with the round-1 frame for "
        f"{int((~agree).sum())} of {len(agree)} households"
    )


# --------------------------------------------------------------------------
# 2.  The cluster grain is described from inside the cluster.
# --------------------------------------------------------------------------

def _contested(flat):
    """Cluster-attribute cells whose households DISAGREE -- the #323 instrument.

    Counted on the PRE-COLLAPSE frame, because that is the only place the
    evidence exists: one line later `.first()` has reduced it and the parquet is
    written from the reduced frame.  Reading the finished table instead would
    report 0 unconditionally and prove nothing.
    """
    flat = flat.reset_index()
    for col in ["v"] + ATTRS:
        if col not in flat.columns:
            flat[col] = pd.NA
        flat[col] = flat[col].astype(str).str.strip().replace(
            {"nan": pd.NA, "None": pd.NA, "<NA>": pd.NA, "": pd.NA})
    g = flat.groupby("v", dropna=False)
    return {a: int((g[a].nunique(dropna=True) > 1).sum()) for a in ATTRS}


@pytest.fixture(scope="module")
def pre_collapse(tz):
    """Household-grain `cluster_features`, per wave, exactly as it reaches
    `Wave.cluster_features`'s projection onto `(t, v)`."""
    out = {}
    for wave in WAVES:
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                out[wave] = tz[wave].grab_data("cluster_features")
        except Exception as exc:  # noqa: BLE001
            pytest.skip(f"Tanzania {wave} cluster_features unavailable: {exc}")
    return out


@pytest.mark.parametrize("wave", WAVES)
def test_cluster_features_is_one_row_per_cluster(cluster_features, wave):
    sub = cluster_features.xs(wave, level="t")
    assert sub.index.is_unique, f"{wave}: duplicate v in cluster_features"
    assert len(sub) == CLUSTERS[wave], (
        f"{wave}: {len(sub)} clusters, expected {CLUSTERS[wave]}")


@pytest.mark.parametrize("wave", WAVES)
def test_contested_cells_stay_below_the_measured_ceiling(pre_collapse, wave):
    per_col = _contested(pre_collapse[wave])
    total = sum(per_col.values())
    assert total <= CONTESTED_CEILING[wave], (
        f"{wave}: {total} contested cluster-attribute cells "
        f"({per_col}), ceiling {CONTESTED_CEILING[wave]} -- households in a "
        f"cluster are disagreeing about the cluster again"
    )


def test_2008_09_is_the_uncontested_baseline(pre_collapse):
    """Round 1, before anyone has moved.  Every cluster's households agree on
    every attribute -- which is what makes them CLUSTER attributes, and what
    makes the later rounds' disagreement a movement artefact rather than an
    ambiguous code."""
    assert sum(_contested(pre_collapse["2008-09"]).values()) == 0


def test_the_frame_reaching_the_projection_is_still_household_grain(pre_collapse):
    """The mover exclusion is a ROW FILTER, not an early collapse.

    If the config reduced to `(t, v)` itself, every measurement above would read
    0 for free and the framework's grain audit would see nothing at all.  Keep
    the projection where it is audited.
    """
    for wave in WAVES:
        idx = pre_collapse[wave].index.names
        assert "i" in idx, f"{wave}: `i` is gone -- the config collapsed early"


# --------------------------------------------------------------------------
# 3.  The two keys must be the SAME key, or `_join_v_from_sample` matches
#     nothing.  This is what was broken outright in 2020-21.
# --------------------------------------------------------------------------

def test_cluster_features_v_is_a_key_sample_knows(cluster_features, sample):
    """Before this branch, ALL 417 of 2020-21's `(t, v)` pairs were unknown to
    `sample`: `cluster_features` keyed on `clusterid` while `sample` keyed on
    `y5_cluster`, so every 2020-21 household got a cluster with no row.

    The 2014-15 residue is PRE-EXISTING and lives on the sample side: 5 clusters
    are present in the wave-level `sample.parquet` but do not survive `id_walk`
    into `sample()`.  Pinned at its measured value so it cannot grow.
    """
    known = set(map(tuple, sample.reset_index()[["t", "v"]].astype(str).values))
    cv = cluster_features.reset_index()[["t", "v"]].astype(str).drop_duplicates()
    per_wave = {}
    for wave in WAVES:
        rows = cv[cv["t"] == wave]
        per_wave[wave] = sum(tuple(r) not in known for r in rows.values)
    expected = {w: 0 for w in WAVES}
    expected["2014-15"] = 5
    assert per_wave == expected, per_wave


def test_2020_21_is_keyed_on_y5_cluster(cluster_features):
    """`y5_cluster` is hyphen-delimited `RR-DD-WWW-EE-CCC`; `clusterid` is a
    bare number and is BLANK for all 545 booster households.  A regression to
    `clusterid` shows up as the wrong shape and 68 missing clusters.

    Three of the 515 ids are MALFORMED IN THE SOURCE -- '12-06-05-101-005',
    '12-072-02-24-001' and '55-01-02-251-005' carry the wrong field widths.
    They are carried through as-is rather than repaired by guesswork; the point
    of this test is the KEY, and every id still has a hyphen.
    """
    v = cluster_features.xs("2020-21", level="t").index.astype(str)
    assert v.str.contains("-").all(), (
        "2020-21 cluster ids are not in y5_cluster form: "
        f"{[x for x in v if '-' not in x][:5]}"
    )
    well_formed = v.str.fullmatch(r"\d{2}-\d{2}-\d{3}-\d{2}-\d{3}")
    assert int((~well_formed).sum()) <= 3, (
        f"more malformed y5_cluster ids than the 3 known: "
        f"{[x for x, ok in zip(v, well_formed) if not ok]}"
    )


def test_2020_21_booster_clusters_are_present(cluster_features):
    """`clusterid` is NaN for every booster household, so keying on it deleted
    them outright on a NaN key.  Keying on `y5_cluster` keeps them."""
    n = len(cluster_features.xs("2020-21", level="t"))
    assert n == 515, f"2020-21 has {n} clusters, expected 515"


# --------------------------------------------------------------------------
# 4.  The mover exclusion must not silently drop a cluster.
# --------------------------------------------------------------------------

def test_no_cluster_wave_is_lost_from_the_2008_15_folder(cluster_features):
    """Residency is tested per `(t, v)`, not per `v`: a cluster can have
    residents in one round and none in the next, and grouping on `v` alone lost
    12 cluster-waves.  Pin the per-round counts."""
    for wave in ["2008-09", "2010-11", "2012-13", "2014-15"]:
        n = len(cluster_features.xs(wave, level="t"))
        assert n == CLUSTERS[wave], f"{wave}: {n} clusters, expected {CLUSTERS[wave]}"


# Clusters whose finished Region is not the one their own id encodes.  The
# geocode decides RESIDENCY; it is never written into the table, so where the
# two part company the residents win -- which is what makes these exceptions
# legible instead of hidden:
#
#   2014-15  4  -- 2 are SIMIYU clusters still carrying their pre-2012 SHINYANGA
#                  code (Simiyu was carved out of Shinyanga in 2012, so the
#                  households are RIGHT and the id is stale); 2 are ids whose
#                  region field contradicts every household in them.
#   2020-21  9  -- 3 malformed ids (see above) plus 6 whose region field
#                  contradicts their households.
#   others   0.
REGION_VS_GEOCODE_CEILING = {
    "2008-09": 0, "2010-11": 0, "2012-13": 0, "2014-15": 4,
    "2019-20": 0, "2020-21": 9,
}


@pytest.mark.parametrize("wave", WAVES)
def test_region_is_the_region_the_cluster_id_claims(cluster_features, geocode, wave):
    """The end-to-end statement of the fix: after the collapse, a cluster's
    Region is the region carried by its own id -- for 2,374 of 2,387 clusters,
    and for ALL of them in four of the six waves.  Before this branch the
    finished Region was whichever household `.first()` happened to reach."""
    scheme = {"2019-20": "sdd", "2020-21": "y5"}.get(wave, "nps")
    sub = cluster_features.xs(wave, level="t").reset_index()
    decoded = sub["v"].map(lambda x: geocode.cluster_region(x, scheme))
    left = decoded.map(geocode.normalize_place)
    right = sub["Region"].map(geocode.normalize_place)
    mismatch = int(((left.notna()) & (right.notna()) & (left != right)).sum())
    assert mismatch <= REGION_VS_GEOCODE_CEILING[wave], (
        f"{wave}: {mismatch} clusters carry a Region that is not the one in "
        f"their own geocode (ceiling {REGION_VS_GEOCODE_CEILING[wave]})"
    )
