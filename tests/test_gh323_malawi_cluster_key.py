"""Malawi cluster key (GH #323, Site 2).

Malawi's `cluster_features` is declared at `(t, v)` but every wave extracts it
from the household cover page, so it is projected from household grain onto the
cluster.  Where the households of one `v` DISAGREE about the cluster's own
attributes, core resolves it with `groupby().first()` -- one arbitrary
household's answer, served as the cluster's.  These tests pin the CONFIG that
makes `v` a real cluster id.  They exercise no core aggregation (D1: core does
not aggregate).

Two different diseases were measured, and only one of them is a broken key:

* **2004-05 (IHS2) -- BROKEN KEY.**  `ea` is the EA's sequence number *within
  its Traditional Authority*: 110 distinct values for 564 enumeration areas, so
  ~5 unrelated EAs merged into one `v`.  Fixed by keying on `psu`, the fully
  qualified 8-digit EA code (region + district + TA + EA) that the same file
  already carries.  164 of 330 contested cells -> 0.

* **2013-14 / 2019-20 (IHPS panel halves) -- NOT a broken key.**  `ea_id` is the
  IHS3 baseline EA and the survey TRACKS movers, so a cluster's households
  genuinely live in different districts.  The decisive evidence is the GPS: in
  2013-14 the households of an EA that did NOT move share EXACTLY ONE
  coordinate, in all 204 EAs.  Every kilometre of within-EA spread is a tracked
  mover.  Re-keying on the current district would fracture real EAs, desynchronise
  `v` from `sample`, and destroy the panel's link to its own baseline.  So the
  residual contested cells are reported, not forced to zero.

Data-dependent: skips only when a Malawi source file is UNAVAILABLE
(``FileNotFoundError``; missing credentials are handled by
``tests/conftest.py``).  Any other exception, or an empty build, fails.

The repository and its CI logs are public, so every data assertion compares
scalar locals and every message carries counts only: pytest's assertion
rewrite would otherwise print the frames, EA codes and coordinates the
asserted expression touches (GH #1260 red team, 2026-10-23).
"""
import re

import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library.paths import countries_root

WAVES = ["2004-05", "2010-11", "2013-14", "2016-17", "2019-20"]

# The fully qualified Malawi EA code: region(1) + district(2) + TA(2) + EA(3).
EA_CODE = re.compile(r"^\d{8}$")


@pytest.fixture(scope="module")
def mw():
    return ll.Country("Malawi")


def _build(mw, table):
    try:
        df = getattr(mw, table)()
    except FileNotFoundError as exc:  # unavailable source -> skip; nothing else
        pytest.skip(f"Malawi {table} source unavailable: {exc}")
    if df is None or df.empty:
        pytest.fail(f"Malawi {table} built empty")
    return df


@pytest.fixture(scope="module")
def sample(mw):
    return _build(mw, "sample")


@pytest.fixture(scope="module")
def cluster_features(mw):
    return _build(mw, "cluster_features")


def _wave_config(wave):
    path = countries_root() / "Malawi" / wave / "_" / "data_info.yml"
    if not path.exists():
        pytest.skip(f"Malawi {wave} data_info.yml missing")
    with open(path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# 1.  2004-05: the broken key.
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
def test_2004_05_v_is_the_fully_qualified_ea_code_not_the_within_TA_sequence(sample):
    """IHS2 drew 564 EAs of 20 households each.  `ea` collapses them to 110."""
    s = sample.reset_index()
    v = s.loc[s["t"].astype(str) == "2004-05", "v"].dropna().astype(str)
    n_v = int(v.nunique())
    assert n_v == 564, (
        f"2004-05 should have 564 clusters (IHS2's EA count), got {n_v}. "
        "110 means `v` is still `ea`, the EA sequence number within its "
        "Traditional Authority, which merges ~5 real EAs per cluster."
    )
    n_bad = int((~v.map(lambda x: bool(EA_CODE.match(x)))).sum())
    assert n_bad == 0, (
        f"{n_bad} 2004-05 `v` values are not the 8-digit "
        "region+district+TA+EA code"
    )


@pytest.mark.requires_s3
def test_2004_05_cluster_features_keeps_every_district_and_the_urban_stratum(
    cluster_features,
):
    """What the broken key actually destroyed, at the API.

    Keyed on `ea`, `groupby().first()` served one household's answer for each of
    110 mega-clusters: 6 of Malawi's 26 districts disappeared from
    `cluster_features` outright, and every surviving cluster came out `Rural` --
    IHS2's entire urban stratum erased from the cluster table.
    """
    cf = cluster_features.reset_index()
    w = cf[cf["t"].astype(str) == "2004-05"]
    n_w = len(w)
    assert n_w == 564, f"expected 564 clusters, got {n_w}"
    n_dist = int(w["District"].nunique())
    assert n_dist == 26, (
        f"2004-05 covers 26 districts; cluster_features shows {n_dist}"
    )
    strata = sorted(set(w["Rural"].dropna().astype(str)))
    assert strata == ["Rural", "Urban"], (
        f"2004-05 cluster_features must retain both settlement strata; "
        f"got {strata}"
    )


@pytest.mark.requires_s3
def test_2004_05_source_shows_ea_is_not_a_cluster_id(mw):
    """The evidence, at source: `ea` merges districts, `psu` does not.

    This is the measurement that decided the key.  It is pinned so that a future
    reader does not have to take the diagnosis on trust, and so that a change of
    source file cannot silently invalidate it.
    """
    from lsms_library.local_tools import get_dataframe

    path = countries_root() / "Malawi" / "2004-05" / "Data" / "sec_a.dta"
    try:
        df = get_dataframe(str(path))
    except FileNotFoundError as exc:
        pytest.skip(f"Malawi 2004-05 sec_a.dta unavailable: {exc}")

    n_ea, n_psu = int(df["ea"].nunique()), int(df["psu"].nunique())
    assert (n_ea, n_psu) == (110, 564), f"(ea, psu) distinct values = {(n_ea, n_psu)}"
    # exactly 20 households per real EA -- the IHS2 design
    sizes = sorted(int(x) for x in df.groupby("psu").size().unique())
    assert sizes == [20], f"households per psu: {sizes}"

    def contested(key, cols):
        g = df.assign(**{key: df[key].astype(str)}).groupby(key, observed=True)
        return {c: int((g[c].nunique(dropna=True) > 1).sum()) for c in cols}

    cols = ["region", "dist", "reside"]
    good = contested("psu", cols)
    assert good == {"region": 0, "dist": 0, "reside": 0}, (
        f"psu must be a real cluster id: no household disagrees about its "
        f"cluster; contested clusters per column {good}"
    )
    bad = contested("ea", cols)
    n_bad = sum(bad.values())
    assert n_bad > 100, (
        f"`ea` is expected to be badly contested (measured 164 cells); got {bad}"
    )


# ---------------------------------------------------------------------------
# 2.  The invariant that makes the join work at all.
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", WAVES)
def test_sample_v_and_cluster_features_v_are_the_same_key(
    wave, sample, cluster_features
):
    """`_join_v_from_sample` joins `cluster_features` onto every household table
    through `sample.v`.  If the two `v` are built from different columns the
    join silently matches nothing, so this must hold in EVERY wave -- it is the
    reason the 2004-05 change had to be made in `sample` and `cluster_features`
    together."""
    s = sample.reset_index()
    cf = cluster_features.reset_index()
    sv = set(s.loc[s["t"].astype(str) == wave, "v"].dropna().astype(str))
    cv = set(cf.loc[cf["t"].astype(str) == wave, "v"].dropna().astype(str))
    if not sv and not cv:
        pytest.fail(f"{wave} absent from both sample and cluster_features "
                    f"(a dropped wave, GH #1281?)")
    # Counts, never the ids: a failure report is public.
    n_cf_only, n_s_only = len(cv - sv), len(sv - cv)
    assert n_cf_only == 0, (
        f"{wave}: {n_cf_only} cluster_features clusters unknown to sample"
    )
    assert n_s_only == 0, (
        f"{wave}: {n_s_only} sample clusters have no cluster_features row"
    )


@pytest.mark.requires_s3
def test_every_wave_uses_the_same_eight_digit_ea_keyspace(sample):
    """One `v` vocabulary across the whole country.

    IHS2's frame is a different census vintage from IHS3+ (only 3 codes in
    common), so the SETS differ -- but the FORMAT must not, or `v` stops being
    comparable and a cross-wave join silently produces nothing.
    """
    s = sample.reset_index()
    for wave in WAVES:
        v = s.loc[s["t"].astype(str) == wave, "v"].dropna().astype(str)
        if v.empty:
            continue
        n_bad = len(set(v[~v.map(lambda x: bool(EA_CODE.match(x)))]))
        assert n_bad == 0, f"{wave}: {n_bad} non-canonical cluster ids"


# ---------------------------------------------------------------------------
# 3.  The df_geo merges: no cartesian product, and the key actually matches.
# ---------------------------------------------------------------------------

# A test used to sit here pinning 2010-11 and 2019-20 as KNOWN-cartesian and
# deliberately unfixed, so PR #627's census would keep them as evidence.  Its
# own failure message named the condition for retiring it -- "if this now merges
# on `i` the cartesian has been fixed ... tell PR #627 it lost an example" -- and
# that has now happened: both waves merge on `i`, 183,812 and 171,230 phantom
# rows -> 0, values bit-for-bit unchanged.  What replaced it is the widened
# parametrize below (the two waves join the third in requiring `df_geo` to build
# `i` exactly as `df_main` does) plus the row-count assertions in
# tests/test_gh323_malawi_gb_cartesian.py, which is where the cartesian itself
# is measured.


@pytest.mark.parametrize("wave", ["2010-11", "2016-17", "2019-20"])
def test_geo_subframe_builds_i_exactly_as_the_main_frame_does(wave):
    """Both halves of IHS4/IHS5 live in one table, so `i` for the
    Cross_Sectional half is run through `cs_i` to keep it apart from the Panel's
    y{3,4}_hhid.  The geo block declared the RAW case_id, so its merge key never
    matched: 2016-17 ended up with GPS for 0 of its 880 clusters, plus 12,447
    orphan geo rows manufactured by the outer join.  The two blocks must build
    `i` identically or they drift apart again.

    Widened to all three household-keyed waves when 2010-11 and 2019-20 stopped
    merging on the cluster key (GH #627).  For those two the risk runs the other
    way from 2016-17's: there the mismatch matched NOTHING, here a mismatch
    would silently reintroduce the cartesian.  2010-11 declares a bare
    `case_id` on both sides; 2019-20 runs it through `cs_i` on both."""
    cfg = _wave_config(wave)["cluster_features"]
    main_i = cfg["df_main"]["idxvars"].get("i")
    geo_i = cfg["df_geo"]["idxvars"].get("i")
    assert geo_i is not None, f"{wave}: df_geo declares no `i` to merge on"
    assert geo_i == main_i, (
        f"{wave}: df_geo `i` ({geo_i!r}) must be built exactly like df_main's "
        f"({main_i!r})"
    )


@pytest.mark.requires_s3
def test_2016_17_clusters_actually_have_coordinates(cluster_features):
    """The consequence of the key fix above, measured at the API: 0 -> 779.

    Since GH #1260 the 101 panel-only EAs carry coordinates too (880 of 880);
    that is pinned in tests/test_gh1260_malawi_panel_geo.py."""
    cf = cluster_features.reset_index()
    w = cf[cf["t"].astype(str) == "2016-17"]
    if w.empty:
        pytest.fail("2016-17 absent from cluster_features (a dropped wave, GH #1281?)")
    have = int(w["Latitude"].notna().sum())
    assert have >= 700, (
        f"2016-17 should carry GPS for its 779 Cross_Sectional clusters "
        f"(and, since GH #1260, its 101 panel-only clusters); got {have}"
    )


# ---------------------------------------------------------------------------
# 4.  The residual, and why it is NOT re-keyed.
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
def test_2013_14_within_cluster_spread_is_tracked_movers_not_a_broken_key():
    """The decisive measurement, pinned.

    188 of 204 EAs have households reporting different coordinates, which looks
    exactly like the broken-key signature.  It is not: restrict to households
    that did NOT move (`dist_to_IHS3location` <= 1 km) and every one of the 204
    EAs collapses to EXACTLY ONE coordinate.  `LAT_DD_MOD` is the EA-level
    displaced fix of the household's CURRENT location, so all of the spread is
    panel dispersal.  `ea_id` is a real cluster id and must be left alone.

    If this ever fails, the diagnosis behind leaving 2013-14 / 2019-20 unre-keyed
    has changed and the decision must be revisited.

    It is also what licenses the GH #1260 hook in ``2013-14/_/mapping.py``,
    which serves each EA this single stayer coordinate (pinned in
    tests/test_gh1260_malawi_panel_geo.py).  The hook calls a household with
    no distance 'unrecorded', not a stayer; this test's ``fillna(0)`` calls it
    a stayer.  The two select the same households only because every 2013-14
    household has a distance -- asserted below, so the agreement is checked,
    not assumed.
    """
    from lsms_library.local_tools import get_dataframe

    root = countries_root() / "Malawi" / "2013-14" / "Data"
    try:
        cover = get_dataframe(str(root / "HH_MOD_A_FILT_13.dta"))
        geo = get_dataframe(str(root / "HouseholdGeovariables_IHPS_13.dta"))
    except FileNotFoundError as exc:
        pytest.skip(f"Malawi 2013-14 sources unavailable: {exc}")

    m = cover[["y2_hhid", "ea_id", "dist_to_IHS3location"]].merge(
        geo[["y2_hhid", "LAT_DD_MOD", "LON_DD_MOD"]], on="y2_hhid", how="inner"
    ).dropna(subset=["LAT_DD_MOD"])

    n_nodist = int(m["dist_to_IHS3location"].isna().sum())
    assert n_nodist == 0, (
        f"{n_nodist} 2013-14 households have no dist_to_IHS3location: "
        "`fillna(0)` here and the GH #1260 hook ('unrecorded' is not a "
        "stayer) now disagree on them"
    )
    stayers = m[m["dist_to_IHS3location"].fillna(0) <= 1]
    per_ea = stayers.groupby("ea_id")[["LAT_DD_MOD", "LON_DD_MOD"]].apply(
        lambda g: len(g.drop_duplicates())
    )
    n_split = int((per_ea > 1).sum())
    assert n_split == 0, (
        "households that did not move must share their EA's single displaced "
        f"coordinate; {n_split} EAs do not"
    )
    # and the movers really are the whole story
    n_movers = int((m["dist_to_IHS3location"].fillna(0) > 1).sum())
    assert n_movers > 1000, (
        f"expected ~1,340 tracked movers in IHPS 2013; got {n_movers}"
    )
