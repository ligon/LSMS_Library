"""Regression tests for Liberia's cluster (``v``) identity.  GH #323.

Liberia 2018-19 wired ``v`` to ``ea_code``, which is NOT an enumeration-area
identifier: it is an EA *serial number*, unique only within
``(county, district, clan)``.  It takes 32 distinct values across the survey's
250 real enumeration areas, so 18 of those 32 buckets silently merged EAs from
opposite ends of the country.  ``ea_code`` 12 alone held 621 households drawn
from 54 real EAs spanning ALL 14 counties; the ``.first()`` collapse in
``Wave.cluster_features()`` then stamped every one of them with
``county='bong'``.

This is class-1 (silently WRONG), not class-2 (silently missing): the API
returned confidently-labelled rows whose labels were fiction.  And because
``_join_v_from_sample()`` propagates ``sample``'s ``v`` to every
household-level table, the conflation was library-wide for Liberia.

The invariants below fail on the pre-fix config and pass after ``v`` is rekeyed
to ``ea_unique`` (the survey's real EA key -- a string prefix of ``hhid`` for
all 2,986 households).

Tests skip if Liberia data isn't available (no DVC / no ``.dta`` on disk).
"""
from __future__ import annotations

import pytest

pd = pytest.importorskip("pandas")

# Ground truth, straight from the source file.
N_HOUSEHOLDS = 2986
N_REAL_EAS = 250          # distinct ea_unique
N_EA_CODE_BUCKETS = 32    # distinct ea_code -- the pre-fix (wrong) count


def _liberia_or_skip():
    try:
        import lsms_library as ll

        return ll.Country("Liberia")
    except Exception as exc:  # pragma: no cover - environment-dependent
        pytest.skip(f"Liberia not available: {exc!r}")


@pytest.fixture(scope="module")
def lbr():
    return _liberia_or_skip()


@pytest.fixture(scope="module")
def source(lbr):
    """The raw cover-page file: the authority on every household's true EA.

    NOTE: skip ONLY on genuine data-unavailability (missing file / no DVC).  A
    broad ``except Exception`` here previously turned a *coding* error in this
    fixture into a green "skip", which is precisely the unvalidated-instrument
    failure this module exists to prevent.
    """
    from lsms_library.local_tools import format_id, get_dataframe

    # `Country.file_path` is a PosixPath *property* (-> countries/Liberia),
    # not a callable.  Data lives under the wave directory.
    path = lbr.file_path / "2018-19" / "Data" / "Household" / "sect1_public.dta"
    try:
        df = get_dataframe(str(path))
    except (FileNotFoundError, OSError) as exc:  # pragma: no cover - env-dependent
        pytest.skip(f"Liberia source file unavailable: {exc!r}")

    df = df.copy()
    df["i"] = df["hhid"].map(format_id)
    df["v_true"] = df["ea_unique"].map(format_id)
    assert len(df) == N_HOUSEHOLDS, (
        f"source has {len(df)} rows, expected {N_HOUSEHOLDS} -- the fixture is "
        f"reading the wrong file; do not trust any result derived from it."
    )
    return df


def test_cluster_features_is_one_row_per_real_ea(lbr):
    """250 real EAs, not 32 serial-number buckets."""
    cf = lbr.cluster_features()

    assert list(cf.index.names) == ["t", "v"]
    assert cf.index.is_unique, "cluster_features must be unique on (t, v)"
    assert len(cf) == N_REAL_EAS, (
        f"cluster_features has {len(cf)} rows; expected {N_REAL_EAS} (one per "
        f"enumeration area). {N_EA_CODE_BUCKETS} means `v` is still wired to "
        f"`ea_code`, conflating unrelated EAs."
    )


def test_sample_v_and_cluster_features_v_agree(lbr):
    """The two `v` encodings must match character-for-character.

    `v` is an *idxvar* in cluster_features (auto `format_id`) but a *myvar* in
    sample (NOT auto-formatted).  If they disagree, `_join_v_from_sample()`
    matches nothing and every household silently gets NaN cluster attributes.
    """
    sample_v = set(lbr.sample()["v"].dropna().astype(str))
    cluster_v = set(lbr.cluster_features().index.get_level_values("v").astype(str))

    assert sample_v == cluster_v, (
        f"v encodings diverge: {len(sample_v - cluster_v)} in sample not in "
        f"cluster_features, {len(cluster_v - sample_v)} the other way. "
        f"Examples: {sorted(sample_v - cluster_v)[:3]} vs "
        f"{sorted(cluster_v - sample_v)[:3]}"
    )


def test_every_household_gets_its_true_county_and_region(lbr, source):
    """The load-bearing test: no household may be given another EA's county.

    Validates the rows that were genuinely AMBIGUOUS -- the 2,819 households in
    the 18 conflated `ea_code` buckets -- not merely the ones whose answer was
    already trivially determined.
    """
    cf = lbr.cluster_features().reset_index()
    smp = lbr.sample().reset_index()

    got = (
        smp[["i", "v"]]
        .merge(cf[["v", "County", "Region"]], on="v", how="left")
        .set_index("i")
    )
    truth = source.set_index("i")[["county_code", "region"]]
    joined = truth.join(got, how="inner")

    assert len(joined) == N_HOUSEHOLDS
    assert joined["v"].notna().all(), "some households have no cluster at all"

    wrong_county = (joined["County"].astype(str) != joined["county_code"].astype(str)).sum()
    wrong_region = (joined["Region"].astype(str) != joined["region"].astype(str)).sum()

    assert wrong_county == 0, (
        f"{wrong_county} households carry a county that is NOT their own "
        f"(GH #323 conflation)."
    )
    assert wrong_region == 0, f"{wrong_region} households carry the wrong region."


def test_cluster_attributes_are_constant_within_cluster(lbr, source):
    """The invariant `Wave.cluster_features()`'s silent `.first()` ASSUMES.

    country.py collapses household rows to cluster grain with `.first()`,
    justified by "invariant within a cluster by construction of the LSMS-ISA
    sampling design".  Liberia violated exactly that.  Assert it rather than
    assume it.
    """
    v_level = lbr.cluster_features().index.get_level_values("v")
    assert v_level.nunique() == N_REAL_EAS

    for attr in ("county_code", "region", "locality"):
        spread = source.groupby("v_true", observed=True)[attr].nunique(dropna=False)
        offenders = int((spread > 1).sum())
        assert offenders == 0, (
            f"{offenders} clusters span >1 distinct {attr!r}; collapsing them "
            f"with .first() would silently discard real variation."
        )
