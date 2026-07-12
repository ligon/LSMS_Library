"""Guyana 1992: the household is (ED, SN, HH), not (ED, HH) — GH #503.

Guyana keyed households on ``i = [ED, HH]``.  That key CONFLATES distinct
households: ``COVERN.dta`` has 1807 rows holding 1807 unique ``(ED, SN, HH)``
but only 1502 unique ``(ED, HH)``.  ``_normalize_dataframe_index`` then collapsed
the collision with ``groupby().first()``.

The damage was not "rows dropped" — it was WRONG NUMBERS that looked fine:

* 562 real households were merged into a shared row; 101 surviving households
  were **chimeras** holding members of two different real households, and 191
  person-rows were grafted onto a household that was not theirs.
* ``assets`` **summed two households' durables together** — 220 (i, j) cells
  carried a Value that appears nowhere in the source.
* 287 households were assigned a Region that is not their own, and 143 the wrong
  Rural, because ``v: ED`` is not the sampling cluster (``(ED, SN)`` is).

Each test below is written against the SOURCE .dta, not against a golden number,
and each one FAILS on the pre-fix config.
"""

from __future__ import annotations

import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

warnings.simplefilter("ignore")

COUNTRY = "Guyana"
WAVE = "1992"


def _data(name: str) -> pd.DataFrame:
    """Read a Guyana 1992 source file through the sanctioned reader."""
    path = countries_root() / COUNTRY / WAVE / "Data" / f"{name}.dta"
    return get_dataframe(str(path), convert_categoricals=False)


@pytest.fixture(scope="module")
def guyana():
    return ll.Country(COUNTRY)


@pytest.fixture(scope="module")
def covern():
    return _data("COVERN")


@pytest.fixture(scope="module")
def rostern():
    return _data("ROSTERN")


def _triple_ids(df, ed="ED", sn="SN", hh="HH") -> set[str]:
    k = df[[ed, sn, hh]].dropna().astype("int64")
    return {f"{e}-{s}-{h}" for e, s, h in k.values}


# --------------------------------------------------------------------------
# 1. The survey declares its own household key.
# --------------------------------------------------------------------------

def test_covern_newid_is_the_triple(covern):
    """COVERN.NEWID == ED*100000 + SN*100 + HH for every row.

    This is the ground truth the whole fix rests on: the source itself says the
    household is the TRIPLE.  (Cast to int64 — the raw columns are int16 and the
    multiplication overflows.)
    """
    c = covern[["ED", "SN", "HH", "NEWID"]].dropna().astype("int64")
    assert len(c) == 1807
    assert (c["NEWID"] == c["ED"] * 100000 + c["SN"] * 100 + c["HH"]).all()
    # and the pair is NOT the household
    assert c.drop_duplicates(["ED", "SN", "HH"]).shape[0] == 1807
    assert c.drop_duplicates(["ED", "HH"]).shape[0] == 1502


# --------------------------------------------------------------------------
# 2. Every real household survives, under its own identity.
# --------------------------------------------------------------------------

def test_sample_keys_on_the_triple(guyana, covern):
    """sample() holds exactly the 1807 real households, keyed ED-SN-HH.

    Pre-fix: 1502 rows, keyed 'ED-HH'.  305 households erased.
    """
    s = guyana.sample()
    ids = set(s.index.get_level_values("i").astype(str))

    assert all(i.count("-") == 2 for i in ids), (
        "sample() household ids must be ED-SN-HH (3 parts); got e.g. "
        f"{sorted(ids)[:3]}"
    )
    assert ids == _triple_ids(covern)
    assert len(s) == len(covern) == 1807


def test_roster_keeps_every_person(guyana, rostern):
    """household_roster() holds every ROSTERN person, under the right household.

    Pre-fix: 6939 of 7827 person-rows, and ZERO of them keyed to their real
    household (the key did not encode SN at all).
    """
    r = guyana.household_roster().reset_index()
    api = {(str(i), str(p)) for i, p in zip(r["i"], r["pid"])}

    src = rostern[["ED", "SN", "HH", "PID"]].dropna().astype("int64")
    truth = {(f"{e}-{s}-{h}", str(p)) for e, s, h, p in src.values}

    assert len(src) == 7827
    assert api == truth, (
        f"roster person-set != source: {len(truth - api)} people missing, "
        f"{len(api - truth)} people attached to a household that is not theirs"
    )


# --------------------------------------------------------------------------
# 3. No chimeras: a household's members all belong to the same real household.
# --------------------------------------------------------------------------

def test_no_chimera_households(guyana, rostern):
    """No API household contains members of two different real households.

    Each API person-row is pinned back to a source person by (Sex, Age); a
    household whose pinned members come from >=2 real households is a chimera.
    Pre-fix: 101 chimeras, 191 grafted person-rows.

    NOTE — instrument validity.  The source-side join key is built in WHATEVER
    form the API currently uses (2-part 'ED-HH' or 3-part 'ED-SN-HH'), while
    ``true_hh`` is always the real triple.  Building it as a triple
    unconditionally would match nothing under the pre-fix 2-part key and the
    test would find zero chimeras VACUOUSLY — a green light from an instrument
    that cannot see.  With this form it correctly reports 101 chimeras pre-fix
    and 0 post-fix.
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

    # the instrument must be able to see: nearly every API person-row should
    # pin to some source person, whichever key form is in force.
    assert len(pinned) >= 0.95 * len(r), (
        f"instrument failure: only {len(pinned)}/{len(r)} API person-rows could "
        f"be pinned to a source person — the chimera check would be vacuous"
    )

    chimeras = [
        i for i, grp in pinned.groupby("i")
        if grp["true_hh"].nunique() > 1
    ]
    assert not chimeras, (
        f"{len(chimeras)} household(s) hold members of >=2 real households, "
        f"e.g. {chimeras[:3]}"
    )


# --------------------------------------------------------------------------
# 4. assets: no fabricated values (no cross-household summation).
# --------------------------------------------------------------------------

def test_assets_values_are_recorded_not_invented(guyana):
    """Every assets Value is a value actually recorded in DRBLS.

    Pre-fix, ``_/assets.py`` collapsed duplicate (t, i, j) by SUMMING — but the
    "duplicates" were DIFFERENT HOUSEHOLDS colliding on the ED-HH key.  225 asset
    households were the arithmetic sum of two real households, producing 220
    (i, j) cells whose Value appears nowhere in the source.

    DRBLS is WIDE (one row per household), so with the correct identity every
    Value must be a raw ``valiNN`` cell.  Blocks 31/31a/31b share the item name
    "OTHER AUDIO-VISUAL" and are legitimately summed within a household, so they
    are excluded.
    """
    drb = _data("DRBLS")
    recorded = set()
    for col in drb.columns:
        if col.startswith("vali"):
            recorded |= set(
                pd.to_numeric(drb[col], errors="coerce").dropna().astype(float)
            )

    a = guyana.assets().reset_index()
    a = a[a["j"] != "OTHER AUDIO-VISUAL"]
    vals = pd.to_numeric(a["Value"], errors="coerce").dropna()

    invented = vals[~vals.isin(recorded)]
    assert invented.empty, (
        f"{len(invented)} assets Value cell(s) are not any recorded DRBLS "
        f"value — a cross-household sum, e.g. {sorted(invented.unique())[:5]}"
    )


def test_assets_keys_on_the_triple(guyana):
    """assets households are ED-SN-HH and all exist in sample()."""
    a = guyana.assets()
    ids = set(a.index.get_level_values("i").astype(str))
    assert all(i.count("-") == 2 for i in ids)
    assert ids <= set(guyana.sample().index.get_level_values("i").astype(str))


# --------------------------------------------------------------------------
# 5. The cluster is (ED, SN), not ED.
# --------------------------------------------------------------------------

def test_cluster_is_ed_sn_not_ed(guyana, covern):
    """Every household's cluster Rural matches its own COVERN SECTOR.

    ED is not a cluster: SECTOR varies within 10 of the 130 EDs, so the
    ``.first()`` collapse in ``Wave.cluster_features`` handed 143 households the
    wrong Rural (and 287 the wrong Region).  Under v = (ED, SN) there are 168
    clusters and SECTOR varies within 0 of them.

    Region is NOT asserted to be exact: RGN still varies within 3 of the 168
    clusters, so 13 households keep a wrong Region.  That is an irreducible
    source inconsistency, documented in Guyana/_/CONTENTS.org — asserted as a
    ceiling here so a regression that reintroduces the ED cluster is caught.
    """
    cf = guyana.cluster_features().reset_index()
    assert cf["v"].nunique() == 168

    s = guyana.sample().reset_index()
    lut = s.merge(cf[["v", "Region", "Rural"]], on="v", how="left",
                  suffixes=("_s", "_c")).drop_duplicates("i").set_index("i")

    cv = covern[["ED", "SN", "HH", "RGN", "SECTOR"]].dropna().astype("int64")
    cv["i"] = [f"{e}-{s_}-{h}" for e, s_, h in cv[["ED", "SN", "HH"]].values]
    j = cv.join(lut[["Region", "Rural_c"]], on="i")

    truth_rural = j["SECTOR"].map({1: "Urban", 2: "Rural"})
    wrong_rural = (j["Rural_c"].astype(str) != truth_rural.astype(str)).sum()
    assert wrong_rural == 0, (
        f"{wrong_rural} household(s) carry a Rural that is not their own "
        f"COVERN SECTOR — v is not the sampling cluster"
    )

    wrong_region = (j["Region"].astype(str) != j["RGN"].astype(str)).sum()
    assert wrong_region <= 13, (
        f"{wrong_region} household(s) carry a Region that is not their own "
        f"COVERN RGN (expected <= 13 residual; 287 under the ED cluster)"
    )


# --------------------------------------------------------------------------
# 6. The GH#323 conflation warning must stay quiet, so it stays a detector.
# --------------------------------------------------------------------------

def test_cold_build_does_not_warn_about_duplicate_index(monkeypatch):
    """A cold build of sample/roster emits no GH#323 duplicate-index warning.

    The warning fires only on a COLD build (the collapsed frame is then cached,
    so every warm read is silent) — hence LSMS_NO_CACHE.  Pre-fix this warned
    about 792 duplicate tuples in ``sample`` and 888 in ``household_roster``.

    ``housing`` is deliberately NOT covered: HHCHAR contains one genuine
    duplicate record (two different households sharing ED 123 / SN 722 / HH 6),
    an irreducible source defect for which the warning is now CORRECT.  See
    Guyana/_/CONTENTS.org.
    """
    monkeypatch.setenv("LSMS_NO_CACHE", "1")
    c = ll.Country(COUNTRY)

    for table in ("sample", "household_roster"):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            getattr(c, table)()
        gh323 = [str(w.message) for w in caught if "GH #323" in str(w.message)]
        assert not gh323, f"{table} still collapses a non-unique index: {gh323}"
