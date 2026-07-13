"""GH #323: Serbia's cluster id `v` must be the composite (opstina, popkrug).

`popkrug` is a zero-padded serial number LOCAL TO A MUNICIPALITY ('0001',
'0002', ...), not a global cluster id -- the same popkrug recurs in many
opstina.  enumeration_district.dta holds 510 census districts but only 328
distinct popkrug.  Declaring `v: popkrug` therefore handed
`_normalize_dataframe_index` a non-unique (t, v) index, which it collapsed with
`groupby().first()`: 182 of the 510 clusters were silently discarded, and the
survivors' Region/Rural were assigned arbitrarily.  That is class-1 silently
WRONG, not merely missing -- 1,823 of 5,557 households (33%) came back attached
to the wrong region of the country and 1,015 to the wrong Rural class.

`sample.df_hh.myvars.v` and `cluster_features.idxvars.v` must move TOGETHER:
sample.v is the join key onto cluster_features (t, v).  Key one on bare popkrug
and the other on the composite and the join silently misses, giving every
household a NaN cluster.  test_no_household_has_nan_cluster_attributes pins
that failure mode; the others pin the collapse itself.
"""
from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

SERBIA = countries_root() / "Serbia"

# Ground truth, counted directly off enumeration_district.dta.
N_DISTRICTS = 510
N_DISTINCT_POPKRUG = 328
N_COLLAPSED = N_DISTRICTS - N_DISTINCT_POPKRUG  # 182


def _mapping_module():
    spec = importlib.util.spec_from_file_location(
        "serbia_mapping", SERBIA / "2007" / "_" / "mapping.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _read_source(stem: str) -> pd.DataFrame:
    """Load a Serbia source .dta through the normal access chain.

    The .dta files are DVC-managed and are NOT on disk in a fresh checkout, so
    a `Path.exists()` guard would skip these tests wherever they matter most --
    and a skip is not a pass.  Resolve the same way ``Country`` does: try the
    path relative to cwd, then relative to the DVC root, then absolute.
    """
    abs_path = SERBIA / "2007" / "Data" / f"{stem}.dta"
    candidates = []
    try:
        candidates.append(os.path.relpath(abs_path, Path.cwd()))
    except ValueError:
        pass
    candidates.append(str(abs_path.relative_to(countries_root().parent.parent)))
    candidates.append(str(abs_path))
    last: Exception | None = None
    for candidate in dict.fromkeys(candidates):
        try:
            return get_dataframe(candidate)
        except Exception as exc:  # noqa: BLE001 - fall through to next candidate
            last = exc
    pytest.skip(f"Serbia microdata unavailable ({type(last).__name__}: {last})")


@pytest.fixture(scope="module")
def ed() -> pd.DataFrame:
    return _read_source("enumeration_district")


@pytest.fixture(scope="module")
def serbia():
    return ll.Country("Serbia")


# --- the source fact that makes the bug a bug -------------------------------

def test_popkrug_alone_is_not_a_cluster_id(ed):
    """popkrug repeats across municipalities; (opstina, popkrug) does not."""
    assert len(ed) == N_DISTRICTS
    assert ed["popkrug"].nunique() == N_DISTINCT_POPKRUG
    assert ed[["opstina", "popkrug"]].drop_duplicates().shape[0] == N_DISTRICTS
    # Not a duplicated-rows artifact: the colliding districts genuinely differ.
    assert int(ed.duplicated().sum()) == 0


def test_collapsing_on_popkrug_would_destroy_payload(ed):
    """The collision is class-1 (WRONG), not class-2 (missing).

    Colliding popkrug carry *disagreeing* Region/Rural, so groupby().first()
    does not merely drop rows -- it mis-attributes the survivors.
    """
    g = ed.groupby("popkrug", observed=True)
    assert int((g["region2"].nunique() > 1).sum()) > 0
    assert int((g["tip"].nunique() > 1).sum()) > 0


# --- the composite key builder ----------------------------------------------

def test_v_builds_composite_and_strips_zero_padding():
    v = _mapping_module().v
    assert v(pd.Series(["70653", "0001"])) == "70653-1"
    # v is a strict prefix of the household id i = '{opstina}-{popkrug}-{dom}'.
    i = _mapping_module().i
    assert i(pd.Series(["70653", "0001", "1001"])).startswith(
        v(pd.Series(["70653", "0001"])) + "-"
    )


def test_v_is_none_when_a_key_part_is_missing():
    """A missing part must not raise (int(nan)) nor silently fabricate a key."""
    v = _mapping_module().v
    assert v(pd.Series([pd.NA, "0001"])) is None


# --- the API-level invariants (these FAIL pre-fix) ---------------------------

def test_cluster_features_keeps_every_enumeration_district(serbia):
    """510 districts in, 510 out.  Pre-fix this returned 328."""
    cf = serbia.cluster_features()
    assert len(cf) == N_DISTRICTS, (
        f"expected {N_DISTRICTS} census districts, got {len(cf)}; "
        f"{N_COLLAPSED} were being collapsed away by groupby().first()"
    )
    assert cf.index.is_unique
    assert int(cf.index.duplicated().sum()) == 0


def test_sample_v_matches_cluster_features_v(serbia):
    """sample.v is the join key onto cluster_features(t, v) -- same key space."""
    sm = serbia.sample()
    cf = serbia.cluster_features()
    assert sm["v"].nunique() == N_DISTRICTS
    sample_v = set(sm["v"].astype(str))
    cluster_v = set(cf.index.get_level_values("v").astype(str))
    assert sample_v <= cluster_v, (
        "sample carries v values absent from cluster_features -- the "
        "(t, v) join will miss and households will get NaN clusters"
    )


def test_no_household_has_nan_cluster_attributes(serbia):
    """Guards the half-fix: moving only one side of the join breaks it."""
    sm = serbia.sample().reset_index()
    cf = serbia.cluster_features().reset_index()
    for col in ("t", "v"):
        sm[col] = sm[col].astype(str)
        cf[col] = cf[col].astype(str)
    joined = sm[["i", "t", "v"]].merge(
        cf[["t", "v", "Region", "Rural"]], on=["t", "v"], how="left"
    )
    assert len(joined) == len(sm), "join multiplied household rows"
    assert int(joined["Region"].isna().sum()) == 0
    assert int(joined["Rural"].isna().sum()) == 0


def test_every_household_gets_its_own_districts_region_and_rural(serbia, ed):
    """The load-bearing check: attribution is right for the AMBIGUOUS rows.

    Compare, household by household, the Region/Rural the API surfaces against
    the row of enumeration_district.dta keyed by that household's OWN
    (opstina, popkrug).  Pre-fix, 1,823 households mismatched on Region and
    1,015 on Rural -- every one of them in a collided popkrug.
    """
    hh = _read_source("domacinstva")
    truth = hh[["opstina", "popkrug", "dom"]].merge(
        ed[["opstina", "popkrug", "region2", "tip"]],
        on=["opstina", "popkrug"], how="left", validate="m:1",
    )
    truth["i"] = truth.apply(
        lambda r: "-".join(str(int(r[k])) for k in ("opstina", "popkrug", "dom")),
        axis=1,
    )
    truth["Rural_true"] = truth["tip"].map({"urban": "Urban", "rural": "Rural"})
    truth = truth.rename(columns={"region2": "Region_true"}).set_index("i")

    sm = serbia.sample().reset_index()
    cf = serbia.cluster_features().reset_index()
    for col in ("t", "v"):
        sm[col] = sm[col].astype(str)
        cf[col] = cf[col].astype(str)
    api = (
        sm[["i", "t", "v"]]
        .merge(cf[["t", "v", "Region", "Rural"]], on=["t", "v"], how="left")
        .assign(i=lambda d: d["i"].astype(str))
        .set_index("i")
    )

    both = api.join(truth[["Region_true", "Rural_true", "popkrug"]], how="inner")
    assert len(both) == len(hh), "lost households joining API to source truth"

    # Restrict to the rows that were ACTUALLY ambiguous -- the households whose
    # popkrug collides across municipalities.  Agreement on the unambiguous
    # rows proves nothing; these are the ones the collapse could corrupt.
    collided = set(ed["popkrug"][ed["popkrug"].duplicated(keep=False)])
    at_risk = both[both["popkrug"].isin(collided)]
    assert len(at_risk) > 3000, "expected ~3,367 households in a collided popkrug"

    region_wrong = (
        at_risk["Region"].astype(str).str.strip().str.lower()
        != at_risk["Region_true"].astype(str).str.strip().str.lower()
    )
    rural_wrong = at_risk["Rural"].astype(str) != at_risk["Rural_true"].astype(str)
    assert int(region_wrong.sum()) == 0, (
        f"{int(region_wrong.sum())} households attributed to the wrong Region"
    )
    assert int(rural_wrong.sum()) == 0, (
        f"{int(rural_wrong.sum())} households attributed to the wrong Rural class"
    )
