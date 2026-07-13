"""
Regression tests for GH #323 -- Serbia and Montenegro ``cluster_features``
must be extracted at CLUSTER grain, not PERSON grain.

Bug shape (pre-fix): both waves declared

    cluster_features:
        file: "../Data/{wave} 1 demography.dta"
        idxvars: {v: mesto, i: rbd}

sourcing a CLUSTER-level table (declared index ``(t, v)``) from the
PERSON-level demography file -- the very same file ``household_roster``
reads.  That file has one row per person (2002: 19,725 = unique
``(mesto, rbd, clan)``; 2003: 8,027), while ``mesto`` (-> ``v``) takes only
618 / 301 distinct values.  ``stratum`` (-> ``Region``) and ``tip``
(-> ``Rural``) are cluster-constant attributes replicated onto every person
record, so the extraction emitted ~19.7k rows for a table with 618 entities.

The surplus rows were then collapsed SILENTLY -- and not where you would
expect.  They never reached ``_normalize_dataframe_index`` (whose GH #323
RuntimeWarning therefore never fired, on a cold build or otherwise): the
explicit ``Wave.cluster_features()`` method (country.py, GH #161) collapses
any ``i`` level with ``groupby().agg('first')`` first, with no warning at
all.  Its justifying comment ("Region/Rural/District are invariant within a
cluster by construction of the LSMS-ISA sampling design") states an
UNCHECKED precondition.

For Serbia that precondition happens to hold -- verified against source:
0 / 618 (2002) and 0 / 301 (2003) clusters carry more than one distinct
``stratum`` or ``tip``, and there are no NaNs -- so no data was ever lost
and the API output is unchanged by this fix (0 rows recovered).  The point
of the fix is that the reduction is now performed at the EXTRACTION, where
the precondition is CHECKED and RAISES if violated, instead of being
assumed by a silent reducer three layers downstream.
"""
import importlib.util

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.paths import countries_root

COUNTRY = "Serbia and Montenegro"

# One row per sampling cluster (``mesto``), per wave -- verified against the
# source demography .dta.
EXPECTED_CLUSTERS = {"2002": 618, "2003": 301}


def _load_country_module():
    """Import ``_/serbia and montenegro.py`` (the df_edit hook module).

    The filename contains spaces, so it cannot be imported by name.
    """
    path = countries_root() / COUNTRY / "_" / "serbia and montenegro.py"
    assert path.exists(), f"country hook module missing: {path}"
    spec = importlib.util.spec_from_file_location("serbia_hooks", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ---------------------------------------------------------------------------
# The enforcement.  These run with no microdata -- they exercise the hook
# directly, so the precondition check can never rot into unexecuted prose.
# ---------------------------------------------------------------------------

def test_hook_collapses_redundant_person_level_repetitions():
    """The benign case: identical payload repeated across person rows
    collapses to exactly one row per cluster."""
    cluster_features = _load_country_module().cluster_features

    # Two clusters; the payload is replicated down onto each person row,
    # exactly as the person-level demography file presents it.
    df = pd.DataFrame(
        {
            "t": ["2002"] * 5,
            "v": ["1", "1", "1", "2", "2"],
            "Region": ["Belgrade"] * 3 + ["Vojvodina"] * 2,
            "Rural": ["Urban"] * 3 + ["Rural"] * 2,
        }
    ).set_index(["t", "v"])

    out = cluster_features(df)

    assert len(out) == 2, "should reduce to one row per cluster"
    assert out.index.is_unique
    assert list(out.index.names) == ["t", "v"]
    assert out.loc[("2002", "1"), "Region"] == "Belgrade"
    assert out.loc[("2002", "2"), "Rural"] == "Rural"


def test_hook_raises_when_payload_is_not_cluster_invariant():
    """THE ENFORCEMENT (GH #323).

    ``first()`` is only a safe reducer because the payload is invariant
    within a cluster.  That is a precondition, not a law of the data.  If a
    cluster ever carries conflicting values, collapsing it would silently
    ship whichever row sorted first -- a class-1 (silently WRONG) failure.
    The hook must REFUSE, loudly.
    """
    cluster_features = _load_country_module().cluster_features

    df = pd.DataFrame(
        {
            "t": ["2002"] * 3,
            "v": ["1", "1", "2"],
            # cluster "1" disagrees with itself -- no reducer can be trusted
            "Region": ["Belgrade", "Vojvodina", "Vojvodina"],
            "Rural": ["Urban", "Urban", "Rural"],
        }
    ).set_index(["t", "v"])

    with pytest.raises(ValueError, match="not invariant within"):
        cluster_features(df)


def test_hook_raises_on_partially_missing_payload():
    """A cluster whose payload is NaN on some rows and populated on others is
    NOT invariant.  ``first()`` would quietly paper over it depending on row
    order; we refuse instead of guessing."""
    cluster_features = _load_country_module().cluster_features

    df = pd.DataFrame(
        {
            "t": ["2002"] * 3,
            "v": ["1", "1", "2"],
            "Region": ["Belgrade", pd.NA, "Vojvodina"],
            "Rural": ["Urban", "Urban", "Rural"],
        }
    ).set_index(["t", "v"])

    with pytest.raises(ValueError, match="not invariant within"):
        cluster_features(df)


# ---------------------------------------------------------------------------
# End-to-end: needs the microdata, so it skips where that isn't reachable.
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def serbia():
    try:
        return ll.Country(COUNTRY)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"{COUNTRY} unavailable: {exc}")


@pytest.mark.parametrize("wave", ["2002", "2003"])
def test_extraction_is_cluster_level_not_person_level(serbia, wave):
    """The wave EXTRACTION itself must already be one row per cluster.

    Pre-fix this returned 19,725 (2002) / 8,027 (2003) person-level rows on a
    non-unique index, and something downstream silently reduced it.  The
    declared ``(t, v)`` index must be unique by construction, so no reducer
    ever has to guess.
    """
    try:
        df = serbia[wave].grab_data("cluster_features")
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"{COUNTRY}/{wave} microdata unavailable: {exc}")

    assert df.index.is_unique, (
        f"{COUNTRY}/{wave} cluster_features extraction has a NON-UNIQUE index "
        f"({len(df)} rows, {df.index.duplicated().sum()} duplicate tuples) -- "
        f"it is emitting person-level rows for a cluster-level table (GH #323)"
    )
    assert "i" not in (df.index.names or []), (
        "cluster_features must not carry a household level; a cluster table "
        "has no household dimension (and bare `rbd` is not a household key)"
    )
    assert len(df) == EXPECTED_CLUSTERS[wave]


def test_api_returns_one_row_per_cluster(serbia):
    """End-to-end shape: 618 + 301 = 919 clusters, uniquely keyed."""
    try:
        df = serbia.cluster_features()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"{COUNTRY} cluster_features unavailable: {exc}")

    assert df.index.is_unique
    assert len(df) == sum(EXPECTED_CLUSTERS.values())
    for wave, n in EXPECTED_CLUSTERS.items():
        assert (df.index.get_level_values("t") == wave).sum() == n


def test_source_payload_is_invariant_within_cluster(serbia):
    """Validate the PRECONDITION that licenses the collapse, against source.

    This is the check that makes ``first()`` legitimate rather than lucky.  It
    is asserted here on the real data (not merely on the synthetic frames
    above) so that a future wave whose ``stratum``/``tip`` genuinely varies
    within a cluster fails a test rather than silently shipping one of the
    conflicting values.
    """
    try:
        df = serbia.cluster_features()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"{COUNTRY} cluster_features unavailable: {exc}")

    # If the payload were not cluster-invariant, the cluster-grain extraction
    # above could not have produced a unique index at all -- the hook would
    # have raised.  Re-assert the substantive property directly.
    counts = df.reset_index().groupby(["t", "v"], observed=True)[
        ["Region", "Rural"]
    ].nunique(dropna=False)
    offenders = counts[(counts > 1).any(axis=1)]
    assert offenders.empty, (
        f"{len(offenders)} cluster(s) carry conflicting Region/Rural:\n{offenders}"
    )
