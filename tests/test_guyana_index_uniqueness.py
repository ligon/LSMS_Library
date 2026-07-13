"""Guyana 1992: the declared index must be UNIQUE for every table (GH #323).

Guyana keys a household on the THREE-level (ED, SN, HH), where SN is the ED
sample-segment serial.  The config used to declare only (ED, HH), which is NOT a
household key: ED numbers are reused across segments, so 248 (ED,HH) buckets
fused 542 distinct real households.  ``_normalize_dataframe_index`` then
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

Prose in a CONTENTS.org is not enforcement; this file is.
"""
import os
import warnings
from pathlib import Path

import pytest

import lsms_library as ll


def _aws_creds_available() -> bool:
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return True
    creds_file = (
        Path(__file__).parent.parent
        / "lsms_library" / "countries" / ".dvc" / "s3_creds"
    )
    if creds_file.exists():
        try:
            return "aws_access_key_id" in creds_file.read_text()
        except OSError:
            return False
    return False


pytestmark = pytest.mark.skipif(
    not _aws_creds_available(),
    reason="Guyana index tests need DVC -> S3 access to the 1992 source .dta.",
)


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

    A duplicate here is silently collapsed by ``groupby().first()`` at
    normalize time -- the GH #323 data loss.  Guarding the *index* (rather than
    only the row count) keeps this test meaningful if the source is ever
    re-extracted.
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


def test_no_silent_collapse_warning(guyana):
    """The framework's own GH #323 warning must not fire for any Guyana table."""
    offenders = []
    for table in sorted(EXPECTED_ROWS):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            getattr(guyana, table)()
        for w in caught:
            if "duplicate tuple" in str(w.message):
                offenders.append(f"{table}: {w.message}")
    assert not offenders, "Silent index collapse still happening:\n" + "\n".join(offenders)


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
    Rural for 274.
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
