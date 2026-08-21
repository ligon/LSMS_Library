"""Guyana 1992: the declared index must be UNIQUE for every table (GH #323).

Guyana keys a household on the THREE-level (ED, SN, HH), where SN is the ED
sample-segment serial.  The config used to declare only (ED, HH), which is NOT a
household key: ED numbers are reused across segments, so 257 (ED,HH) buckets
fused 562 distinct real households.  ``_normalize_dataframe_index`` then
collapsed the resulting non-unique index with a silent ``groupby().first()``,
keeping one household's members and discarding the other's -- 888 of 7,827
roster rows, 311 of 1,819 housing rows, etc.

These assertions FAIL on the pre-fix config (they are what the fix is for):

    household_roster      6,939 rows -> 7,827   (888 people restored)
    individual_education  4,137 rows -> 4,633
    housing               1,508 rows -> 1,817
    sample                1,502 rows -> 1,807   (+ 488 phantom NaN-i rows gone)
    interview_date        1,502 rows -> 1,807
    assets               10,345 rows -> 11,227  (was SUMMING two households' durables)
    cluster_features        130 rows ->   168   (v is the (ED,SN) segment, not ED)

DISCRIMINATION, MEASURED (2026-08-20), not asserted.  Run against
`development`'s Guyana config via ``LSMS_COUNTRIES_ROOT``, in a cold isolated
``LSMS_DATA_DIR``: **14 of these 23 tests FAIL**; against this branch's config,
all 23 pass.  The 9 that pass either way are labelled as such in their own
docstrings, and each has a reason:

* ``test_declared_index_is_unique`` (x7) -- the collapse makes the returned
  index unique precisely BY destroying rows, so it cannot see #323.  It guards
  duplicates re-introduced AFTER normalize (the id_walk/``attrs`` class).
* ``test_covern_newid_is_the_triple`` -- reads only the ``.dta``; a
  ground-truth pin on the source, not a config guard.
* ``test_sample_has_no_phantom_households`` -- ``groupby(dropna=True)`` DELETES
  the 488 NaN-``i`` rows on the pre-fix config, so "no phantom row survives" is
  true either way; what differs is *how* (deleted silently by the collapse vs.
  dropped loudly by the declared hook).  The row count in
  ``test_row_counts_recovered['sample']`` is what catches that.

Prose in a CONTENTS.org is not enforcement; this file is.
"""
import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import data_root, get_dataframe
from lsms_library.paths import countries_root

# Every test here builds real country data, so it needs DVC -> S3.  Use the
# shared marker rather than a private ``_aws_creds_available`` copy: the
# credentials check lives in ``tests/conftest.py``, which explicitly forbids
# importing it (neither ``from conftest import ...`` nor
# ``from tests.conftest import ...`` resolves correctly under pytest's import
# mode here) and provides this marker as the import-free alternative.  An
# ungated module does not skip in the credential-free ``unit-tests`` CI job --
# it dies with ``botocore NoCredentialsError``.
pytestmark = pytest.mark.requires_s3


# table -> expected row count after the fix.
EXPECTED_ROWS = {
    "household_roster": 7827,
    "individual_education": 4633,
    "housing": 1817,
    "sample": 1807,
    "interview_date": 1807,
    "assets": 11227,
    "cluster_features": 168,
}


@pytest.fixture(scope="module")
def guyana():
    return ll.Country("Guyana")


@pytest.mark.parametrize("table", sorted(EXPECTED_ROWS))
def test_declared_index_is_unique(guyana, table):
    """No table may ship a duplicated canonical index.

    NOTE: this is NOT the primary #323 guard and it does NOT fail on the pre-fix
    config -- ``_normalize_dataframe_index`` collapses duplicates before we ever
    see the frame, so the returned index is unique precisely BECAUSE rows were
    silently discarded.  The real #323 guards are the row counts below (which do
    fail pre-fix).  This one catches duplicates re-introduced AFTER normalize --
    e.g. the id_walk/`attrs` double-application that bit Burkina Faso.
    """
    df = getattr(guyana, table)()
    dupes = df.index.duplicated()
    n = int(dupes.sum())
    assert n == 0, (
        f"Guyana {table}: {n} duplicate index tuple(s) on {list(df.index.names)}. "
        f"These are silently collapsed with groupby().first() (GH #323). "
        f"Examples: {list(df.index[dupes][:3])}"
    )


@pytest.mark.parametrize("table", sorted(EXPECTED_ROWS))
def test_row_counts_recovered(guyana, table):
    """Pin the recovered row counts, so a regression cannot pass quietly."""
    df = getattr(guyana, table)()
    assert len(df) == EXPECTED_ROWS[table], (
        f"Guyana {table}: expected {EXPECTED_ROWS[table]} rows, got {len(df)}."
    )


def _clear_guyana_caches(tables) -> None:
    """Physically drop L2-country (`var/`) and L2-wave parquets for Guyana.

    REQUIRED for the collapse-warning test below.  The framework's GH #323
    warning fires only on a COLD build: in warm operation the collapse is
    already baked into the cache, so the warning never fires again and a
    warm-cache assertion passes VACUOUSLY.  The bug hides behind the cache the
    bug poisoned -- which is how #323 stayed hidden in the first place.
    """
    root = data_root() / "Guyana"
    if not root.exists():
        return
    for table in tables:
        l1 = root / "var" / f"{table}.parquet"
        if l1.exists():
            l1.unlink()
        for wave_dir in root.iterdir():
            if not wave_dir.is_dir() or wave_dir.name == "var":
                continue
            l2 = wave_dir / "_" / f"{table}.parquet"
            if l2.exists():
                l2.unlink()


def test_no_silent_collapse_warning_on_cold_build(monkeypatch):
    """No ``GrainCollapseWarning`` for any Guyana table on a COLD build.

    Built COLD (caches cleared + LSMS_NO_CACHE) so the collapse path actually
    runs.  Without the cold build this assertion is vacuous: it would pass on
    the pre-fix config too, because the warm cache already contains the
    collapsed rows and no rebuild -- hence no warning -- ever happens.

    MATCH ON THE WARNING CATEGORY, NOT ON ITS TEXT.  This test originally
    grepped the message for the substring ``"duplicate tuple"``.  The GH #323
    audit work that landed on `development` rewrote the message -- it now reads
    "... (N conflicting index tuples)" -- so the substring stopped matching and
    the test passed VACUOUSLY: measured against `development`'s Guyana config
    it reported no offenders while SEVEN GrainCollapseWarnings were being
    emitted in the same run.  A green detector that cannot see is worse than no
    detector.  ``GrainCollapseWarning`` is a stable public symbol in
    ``lsms_library.country``; the wording of its message is not.
    """
    from lsms_library.country import GrainCollapseWarning

    monkeypatch.setenv("LSMS_NO_CACHE", "1")
    tables = sorted(EXPECTED_ROWS)
    _clear_guyana_caches(tables)

    country = ll.Country("Guyana")
    offenders = []
    for table in tables:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            getattr(country, table)()
        for w in caught:
            if issubclass(w.category, GrainCollapseWarning):
                offenders.append(f"{table}: {w.message}")
    assert not offenders, (
        f"{len(offenders)} Guyana table(s) still collapse a non-unique grain "
        f"with groupby().first() (GH #323):\n" + "\n".join(offenders))


def test_household_id_carries_the_segment(guyana):
    """i must be ED-SN-HH, and the ED-5 collision must be two distinct households.

    Under the old (ED, HH) id, ED=5/HH=2 fused SN=194's four-person household
    with SN=702's three-person household into one four-person chimera.
    """
    r = guyana.household_roster()
    ids = r.index.get_level_values("i")
    assert all(str(x).count("-") == 2 for x in ids[:50]), (
        f"household ids must be 'ED-SN-HH'; saw {list(ids[:5])}")

    a = r[ids == "5-194-2"]
    b = r[ids == "5-702-2"]
    assert len(a) == 4, f"5-194-2 should have 4 members, got {len(a)}"
    assert len(b) == 3, f"5-702-2 should have 3 members, got {len(b)}"


def test_sample_has_no_phantom_households(guyana):
    """WEIGHT.dta's 488 frame-only EDs must not appear as households.

    They have no household at all (i is NaN); merged in via the framework's
    outer join they used to collapse into a single phantom household.
    """
    s = guyana.sample()
    assert s.index.get_level_values("i").notna().all(), "phantom NaN-i household in sample"
    assert s["strata"].notna().all(), "phantom (strata-less) row in sample"
    # EDs 408 and 482 are absent from WEIGHT.dta: their 23 households keep a
    # NaN weight (loudly missing) and are never imputed.
    assert int(s["weight"].isna().sum()) == 23, (
        f"expected the 23 households in EDs 408/482 to have NaN weight, "
        f"got {int(s['weight'].isna().sum())}")


def test_cluster_is_the_segment_not_the_ed(guyana):
    """v = (ED, SN).  ED alone spans regions, so it is not a cluster.

    ED 5 / SN 194 is Region 4 urban; ED 5 / SN 702 is Region 10 rural.  Keyed on
    ED alone, cluster_features had to invent a Region for 537 households and a
    Rural for 274 -- and 287 / 143 of those were handed a value that is NOT
    their own (the two pairs answer different questions: decided-by-row-order
    vs. decided-wrong).
    """
    cf = guyana.cluster_features()
    assert len(cf) == 168
    assert cf["Region"].notna().all(), "Region undetermined for some segment"
    assert cf["Rural"].notna().all(), "Rural undetermined for some segment"

    v = cf.index.get_level_values("v")
    assert "5-194" in set(v) and "5-702" in set(v)
    got = cf.reset_index().set_index("v")
    assert got.loc["5-194", "Rural"] == "Urban"
    assert got.loc["5-702", "Rural"] == "Rural"


def test_cluster_attributes_are_the_households_own(guyana, covern):
    """Every household's cluster Rural is its OWN COVERN SECTOR.

    The VALUE half of the cluster fix, which no row count can see.  Measured on
    the pre-fix config: 143 households carried a Rural that is not their own and
    287 a Region that is not their own, because ``v: ED`` merged enumeration
    districts in different regions and something had to pick.  Post-fix Rural is
    exact (0 wrong) because SECTOR is homogeneous within all 168 (ED, SN)
    segments.

    Region is asserted as a CEILING, not exactly: RGN genuinely disagrees inside
    3 of the 168 segments (a lone stray household each, 10-vs-1 / 11-vs-1 /
    11-vs-1), and the mode reducer leaves exactly those 3 households with the
    majority's Region.  That is an irreducible source inconsistency, documented
    in Guyana/_/CONTENTS.org; the ceiling still catches a regression to v: ED.
    """
    cf = guyana.cluster_features().reset_index()
    s = guyana.sample().reset_index()
    lut = (s.merge(cf[["v", "Region", "Rural"]], on="v", how="left",
                   suffixes=("_s", "_c"))
             .drop_duplicates("i").set_index("i"))

    c = covern[["ED", "SN", "HH", "RGN", "SECTOR"]].dropna().astype("int64")
    n_parts = str(s["i"].iloc[0]).count("-") + 1
    c["i"] = ([f"{e}-{sn}-{h}" for e, sn, h in c[["ED", "SN", "HH"]].values]
              if n_parts == 3 else
              [f"{e}-{h}" for e, _sn, h in c[["ED", "SN", "HH"]].values])
    j = c.join(lut[["Region", "Rural_c"]], on="i")
    assert int(j["Region"].notna().sum()) == 1807, (
        "instrument failure: not every COVERN household joined to a cluster")

    truth_rural = j["SECTOR"].map({1: "Urban", 2: "Rural"})
    wrong_rural = int((j["Rural_c"].astype(str) != truth_rural.astype(str)).sum())
    assert wrong_rural == 0, (
        f"{wrong_rural} household(s) carry a Rural that is not their own COVERN "
        f"SECTOR -- v is not the sampling cluster (143 under v: ED)")

    wrong_region = int((j["Region"].astype(str) != j["RGN"].astype(str)).sum())
    assert wrong_region <= 3, (
        f"{wrong_region} household(s) carry a Region that is not their own "
        f"COVERN RGN (expected <= 3 residual; 287 under v: ED)")


# --------------------------------------------------------------------------
# Grafted from `fix/503-guyana` (the competing independent implementation),
# per the review on PR #668.  Row counts prove rows were RESTORED; these prove
# the VALUES are the survey's own.  `groupby().first()` skips NA per column, so
# a conflicting group collapses into a composite row assembled from the first
# non-null value of each column independently -- "a household that exists
# nowhere in the survey" (CLAUDE.md).  No row count can see that.
# --------------------------------------------------------------------------

def _source(name: str) -> pd.DataFrame:
    """Read a Guyana 1992 source file through the sanctioned reader."""
    path = countries_root() / "Guyana" / "1992" / "Data" / f"{name}.dta"
    return get_dataframe(str(path), convert_categoricals=False)


@pytest.fixture(scope="module")
def covern():
    return _source("COVERN")


@pytest.fixture(scope="module")
def rostern():
    return _source("ROSTERN")


def test_covern_newid_is_the_triple(covern):
    """COVERN.NEWID == ED*100000 + SN*100 + HH for every row.

    The survey's own household id, and the ground truth the whole fix rests on:
    the SOURCE says the household is the TRIPLE.  (Cast to int64 -- the raw
    columns are int16 and the multiplication overflows.)

    HONEST SCOPE: this reads only the `.dta` and therefore passes on the pre-fix
    config too.  It is a ground-truth pin, NOT a #323 regression guard -- it
    fails only if the source file changes under us.  The guards that
    discriminate are the row counts and the three tests below it.
    """
    c = covern[["ED", "SN", "HH", "NEWID"]].dropna().astype("int64")
    assert len(c) == 1807
    assert (c["NEWID"] == c["ED"] * 100000 + c["SN"] * 100 + c["HH"]).all()
    assert c["NEWID"].is_unique
    # ... and the PAIR is not the household: 1807 triples, only 1502 pairs.
    assert c.drop_duplicates(["ED", "SN", "HH"]).shape[0] == 1807
    assert c.drop_duplicates(["ED", "HH"]).shape[0] == 1502
    # 257 (ED,HH) buckets hold 562 real households; 562 - 257 = 305 = 1807-1502.
    g = c.groupby(["ED", "HH"]).size()
    assert int((g > 1).sum()) == 257
    assert int(g[g > 1].sum()) == 562


def test_no_chimera_households(guyana, rostern):
    """No API household holds members of two different real households.

    Each API person-row is pinned back to a source person by (Sex, Age); a
    household whose pinned members come from >= 2 real households is a chimera.

    INSTRUMENT VALIDITY (why the source key is built in the API's own form):
    the join key is assembled as 2-part 'ED-HH' or 3-part 'ED-SN-HH' according
    to whatever the API currently emits, while ``true_hh`` is ALWAYS the real
    triple.  Building the join key as a triple unconditionally would match
    nothing under the pre-fix 2-part id, the pinned frame would be empty, and
    the test would report zero chimeras VACUOUSLY -- a green light from an
    instrument that cannot see.  The `>= 0.95` pin-rate assertion below exists
    to make that failure mode loud rather than silent.
    """
    r = guyana.household_roster().reset_index()
    r["pid"] = r["pid"].astype(str)
    r["i"] = r["i"].astype(str)
    r["Age_i"] = pd.to_numeric(r["Age"], errors="coerce").round().astype("Int64")

    n_parts = r["i"].iloc[0].count("-") + 1
    assert n_parts in (2, 3)

    src = rostern[["ED", "SN", "HH", "PID", "SX", "AG"]].dropna().astype("int64")
    trip = src[["ED", "SN", "HH"]].values
    src["true_hh"] = [f"{e}-{s}-{h}" for e, s, h in trip]
    src["i"] = (src["true_hh"] if n_parts == 3
                else [f"{e}-{h}" for e, _s, h in trip])
    src["pid"] = src["PID"].astype(str)
    src["Sex_src"] = src["SX"].map({1: "M", 2: "F"})
    src["Age_src"] = src["AG"].astype("Int64")

    m = r.merge(src[["i", "pid", "Sex_src", "Age_src", "true_hh"]],
                on=["i", "pid"], how="left")
    pinned = m[(m["Sex"] == m["Sex_src"]) & (m["Age_i"] == m["Age_src"])]

    assert len(pinned) >= 0.95 * len(r), (
        f"instrument failure: only {len(pinned)}/{len(r)} API person-rows could "
        f"be pinned to a source person -- the chimera check would be vacuous")

    chimeras = [i for i, grp in pinned.groupby("i")
                if grp["true_hh"].nunique() > 1]
    assert not chimeras, (
        f"{len(chimeras)} household(s) hold members of >=2 real households, "
        f"e.g. {chimeras[:3]}")


def test_assets_keys_on_the_triple(guyana):
    """assets households are ED-SN-HH and every one exists in sample()."""
    a = guyana.assets()
    ids = set(map(str, a.index.get_level_values("i")))
    bad = sorted(i for i in ids if i.count("-") != 2)
    assert not bad, f"assets ids must be 'ED-SN-HH'; saw e.g. {bad[:5]}"
    universe = set(map(str, guyana.sample().index.get_level_values("i")))
    assert ids <= universe, (f"assets households absent from sample(): "
                             f"{sorted(ids - universe)[:5]}")


def test_assets_values_are_recorded_not_invented(guyana):
    """No assets Value pools durables across two DIFFERENT real households.

    ``_/assets.py`` collapses duplicate ``(t, i, j)`` by SUMMING.  That is
    correct only while ``i`` names ONE real household; under the pre-fix
    ``(ED, HH)`` id the "duplicates" being summed were DIFFERENT HOUSEHOLDS
    colliding on the key, so the sum fabricated a holding nobody reported.  A
    row count cannot tell "right values" from "wrong values, right shape".

    Scope, and why it is this scope: the check is restricted to the households
    that have exactly ONE ``DRBLS.dta`` row under the survey's own
    ``COVERN.NEWID`` identity.  For those, and only those, no legitimate
    within-household summation is possible, so every API ``Value`` must be a raw
    ``valiNN`` cell **of that household's own row**.  (Guyana's DRBLS is NOT
    one row per household: 114 households carry 2, 3 or 5 per-acquisition rows,
    and summing those is the intended, correct behaviour -- 140 post-fix Value
    cells are such legitimate sums.  A blanket "every Value is a raw cell"
    assertion, which is what ``fix/503-guyana`` wrote against its own different
    assets implementation, is therefore FALSE here and was not grafted as-is.)

    ``OTHER AUDIO-VISUAL`` is excluded: blocks 31/31a/31b share that item name
    and are legitimately summed within a single row.

    Discriminating, not vacuous: the source-side household id is built in
    whatever form the API emits (see ``test_no_chimera_households``), so on the
    pre-fix config a single-DRBLS-row household such as ``5-194-2`` maps to the
    API id ``5-2``, whose Values include ``5-702-2``'s -- values that are not in
    ``5-194-2``'s own row, and the test fails.
    """
    cov = _source("COVERN").dropna(subset=["NEWID"]).copy()
    cov["NEWID"] = cov["NEWID"].astype("int64")
    idmap = cov.drop_duplicates("NEWID").set_index("NEWID")[["ED", "SN", "HH"]]

    drb = _source("DRBLS").dropna(subset=["newid"]).copy()
    drb["newid"] = drb["newid"].astype("int64")
    drb = drb.join(idmap, on="newid", how="inner")

    a = guyana.assets().reset_index()
    a["i"] = a["i"].astype(str)
    n_parts = a["i"].iloc[0].count("-") + 1
    assert n_parts in (2, 3)
    drb["i"] = ([f"{int(e)}-{int(s)}-{int(h)}"
                 for e, s, h in zip(drb["ED"], drb["SN"], drb["HH"])]
                if n_parts == 3 else
                [f"{int(e)}-{int(h)}"
                 for e, h in zip(drb["ED"], drb["HH"])])

    # Households with exactly one real-household DRBLS row -- no within-household
    # sum is possible for these, in EITHER key form.
    drb["_triple"] = [f"{int(e)}-{int(s)}-{int(h)}"
                      for e, s, h in zip(drb["ED"], drb["SN"], drb["HH"])]
    tcount = drb["_triple"].value_counts()
    singles = drb[drb["_triple"].isin(set(tcount[tcount == 1].index))]
    assert len(singles) > 1000, (
        f"instrument failure: only {len(singles)} single-row households found")

    vali = [c for c in drb.columns if c.startswith("vali")]
    allowed: dict[str, set] = {}
    for _, row in singles.iterrows():
        vals = pd.to_numeric(row[vali], errors="coerce").dropna().astype(float)
        allowed.setdefault(row["i"], set()).update(vals.tolist())

    a = a[(a["j"] != "OTHER AUDIO-VISUAL") & (a["i"].isin(allowed))]
    assert len(a) > 1000, "instrument failure: no assets rows left to check"

    offenders = []
    for hh, grp in a.groupby("i"):
        ok = allowed[hh]
        for val in pd.to_numeric(grp["Value"], errors="coerce").dropna():
            if float(val) not in ok:
                offenders.append((hh, float(val)))
    assert not offenders, (
        f"{len(offenders)} assets Value cell(s) are not any value recorded for "
        f"that household in DRBLS -- a cross-household sum. "
        f"e.g. {offenders[:5]}")
