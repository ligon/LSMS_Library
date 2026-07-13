"""Kosovo 2000 housing / cluster_features -- GH #323.

``DWELLING.dta`` section 3a is a ROSTER OVER DWELLING STRUCTURES: the enumerator
lists all 8 structure types and asks ``s3a_q01`` "Does your household use
[STRUCTURE]?" for each, so the source has 7,047 rows over 2,865 households.
Declared ``(t, i)`` on the YAML path, it reached ``_normalize_dataframe_index``
with 4,182 duplicate index tuples and was collapsed with ``groupby().first()``.
``GroupBy.first()`` is skipna PER COLUMN, so it did not pick a row -- it
fabricated a chimera: ``Type`` came from roster row 0 (usually a structure the
household had said "No" to) while ``Rooms``/``Tenure`` came from the first
non-null, i.e. the occupied structure.  524 of 2,850 single-occupancy households
were served a dwelling type they explicitly denied.  Separately, ``Electricity``
was wired to ``s3a_q01`` -- the occupancy screener itself -- so every published
value was meaningless.

Each test below fails on pristine ``development``.
"""
import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import get_dataframe

DATA = "lsms_library/countries/Kosovo/2000/Data/"


def _src(name):
    try:
        return get_dataframe(DATA + name)
    except Exception:  # pragma: no cover - microdata not available locally
        pytest.skip(f"Kosovo {name} not available (see CLAUDE.md)")


@pytest.fixture(scope="module")
def dwelling():
    d = _src("DWELLING.dta")
    d["i"] = d["hhid"].astype(str)
    return d


@pytest.fixture(scope="module")
def amenities():
    a = _src("AMENITIES.dta")
    a["i"] = a["hhid"].astype(str)
    return a


@pytest.fixture(scope="module")
def housing():
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = ll.Country("Kosovo").housing()
    except Exception:  # pragma: no cover
        pytest.skip("Kosovo.housing() could not be built (missing data or DVC error)")
    if df.empty:
        pytest.skip("Kosovo.housing() is empty")
    return df.reset_index().set_index("i")


def test_housing_index_is_unique(housing):
    """The (t, i) index must be unique WITHOUT a groupby().first() collapse."""
    assert housing.index.is_unique


def test_no_household_gets_a_dwelling_type_it_denied(dwelling, housing):
    """The core class-1 bug: 524 households were published with a dwelling type
    they answered 'No' to, because groupby().first() took Type from roster row 0."""
    said_no = dwelling[dwelling["s3a_q01"].astype(str) == "No"]
    denied = set(zip(said_no["i"], said_no["s3a_q0a"].astype(str)))
    published = set(zip(housing.index.astype(str), housing["Type"].astype(str)))
    assert not (denied & published), (
        f"{len(denied & published)} household(s) published with a dwelling type "
        f"they explicitly said 'No' to"
    )


def test_type_matches_the_occupied_structure(dwelling, housing):
    """For every household occupying exactly one structure, Type must be THAT
    structure -- there is no ambiguity to hide behind."""
    occ = dwelling[dwelling["s3a_q01"].astype(str) == "Yes"]
    n = occ.groupby("i").size()
    single = occ[occ["i"].isin(n[n == 1].index)].set_index("i")["s3a_q0a"].astype(str)
    common = [i for i in single.index if i in housing.index]
    assert common, "no overlap between source and API households"
    got = housing.loc[common, "Type"].astype(str)
    wrong = int((got.values != single.loc[common].values).sum())
    assert wrong == 0, f"{wrong} of {len(common)} single-occupancy households have the wrong Type"


def test_electricity_comes_from_amenities_not_the_occupancy_screener(amenities, housing):
    """`Electricity: s3a_q01` was a mis-mapped column -- s3a_q01 is "Does your
    household use [STRUCTURE]?", not electricity.  The real variable is
    AMENITIES.dta s3b_q07 ("Does your household have access to electricity?")."""
    truth = amenities.set_index("i")["s3b_q07"].astype(str)
    common = [i for i in housing.index.astype(str) if i in truth.index]
    got = housing.loc[common, "Electricity"].astype(str)
    wrong = int((got.values != truth.loc[common].values).sum())
    assert wrong == 0, f"{wrong} of {len(common)} households have a wrong Electricity value"


def test_rooms_is_summed_over_occupied_structures(dwelling, housing):
    """s3a_q04 is "rooms in [STRUCTURE]" -- a PER-STRUCTURE count -- so a
    household occupying two structures has the rooms of both.  `first` is
    provably the wrong reducer."""
    occ = dwelling[dwelling["s3a_q01"].astype(str) == "Yes"]
    n = occ.groupby("i").size()
    multi = n[n > 1].index
    assert len(multi), "expected some multi-structure households"
    for i in multi:
        if i not in housing.index:
            continue
        want = pd.to_numeric(occ[occ["i"] == i]["s3a_q04"], errors="coerce").sum()
        got = housing.loc[i, "Rooms"]
        assert float(got) == float(want), f"household {i}: Rooms {got} != sum {want}"


def test_undetermined_primary_structure_is_NA_not_guessed(dwelling, housing):
    """Where the argmax of s3a_q02 (area used) TIES, the primary structure is
    genuinely undetermined.  We must leave Type/Walls/Tenure <NA> rather than let
    idxmax() resolve the tie by ROW ORDER -- a positional guess.  Class-2
    (missing) beats class-1 (wrong).  Rooms/Electricity are tie-independent and
    must survive."""
    occ = dwelling[dwelling["s3a_q01"].astype(str) == "Yes"].copy()
    occ["sqm"] = pd.to_numeric(occ["s3a_q02"], errors="coerce")
    n = occ.groupby("i").size()
    tied = []
    for i in n[n > 1].index:
        g = occ[occ["i"] == i]
        if int((g["sqm"] == g["sqm"].max()).sum()) > 1:
            tied.append(i)
    assert tied, "expected at least one tied multi-structure household"
    for i in tied:
        if i not in housing.index:
            continue
        assert pd.isna(housing.loc[i, "Type"]), f"household {i}: tie resolved by row order"
        assert pd.notna(housing.loc[i, "Rooms"]), f"household {i}: lost tie-independent Rooms"


def test_cluster_features_region_rural_constant_within_cluster():
    """cluster_features reduces household-grain ID.dta (2,880 rows) to 360
    clusters.  That reduction is only safe because Region/Rural are constant
    within a PSU -- an invariant that was asserted in a code comment and
    enforced nowhere.  Enforce it."""
    idf = _src("ID.dta")
    counts = idf.groupby("psu")[["s0i_q07", "s0i_q09"]].nunique(dropna=False)
    bad = counts[(counts > 1).any(axis=1)]
    assert bad.empty, f"{len(bad)} cluster(s) are not homogeneous in Region/Rural"

    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            cf = ll.Country("Kosovo").cluster_features()
    except Exception:  # pragma: no cover
        pytest.skip("Kosovo.cluster_features() could not be built")
    assert cf.index.is_unique
    assert len(cf) == idf["psu"].nunique()
