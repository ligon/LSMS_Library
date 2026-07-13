"""Guinea-Bissau cluster_features -- the GH #323 CONTROL case.

The 2018-19 wave parquet holds 5,410 rows on a declared (t, v) index that has
only 450 distinct clusters, so the framework collapses 4,960 duplicate rows.
Unlike Mali (32,026 real people vaporized) that collapse is provably LOSSLESS,
and this test pins exactly why -- so a future change that makes the collapse
lossy here, or that "fixes" #323 by loosening the guard, fails loudly.

The 5,410 decompose exactly, and both terms are byte-identical repeats:

  TERM 1 (4,901) the cluster_features YAML reads s00_me_gnb2018.dta, the
      HOUSEHOLD cover page (5,351 households over 450 grappes), but declares
      only `v: grappe`.  Each household therefore emits a row carrying ITS
      CLUSTER's attributes (Region s00q01, Rural s00q04).  Both are perfectly
      constant within grappe at source, so the repeats are redundant, not
      distinct observations.  5,351 - 450 = 4,901.

  TERM 2 (59) grappe_gps_gnb2018.dta has 450 rows but only 445 unique grappes:
      5 records are byte-identical duplicates (same grappe, vague, Lat, Lon,
      Accuracy, Altitude AND Timestamp -- an export artifact in the released
      file, not two readings).  merge_on: v fans those 5 grappes out x2, and
      they contain exactly 59 households.

  5,351 + 59 = 5,410.
"""
import pandas as pd
import pytest

from lsms_library import Country


@pytest.fixture(scope="module")
def cf():
    return Country("Guinea-Bissau").cluster_features()


def test_one_row_per_cluster(cf):
    """450 clusters -- the correct grain, reached without discarding anything."""
    assert len(cf) == 450
    assert cf.index.is_unique
    assert list(cf.index.names) == ["t", "v"]


def test_cluster_attributes_are_unambiguous(cf):
    """No cluster carries two different values for any payload column.

    This is the property that makes the collapse lossless.  If a future data
    re-release breaks it, the framework will now RAISE (GH #323) rather than
    silently pick one -- and this test says why that is correct.
    """
    flat = cf.reset_index()
    for col in ("Region", "Rural", "Latitude", "Longitude"):
        conflicting = (flat.groupby("v")[col].nunique(dropna=False) > 1).sum()
        assert conflicting == 0, f"{col} disagrees within {conflicting} cluster(s)"


def test_building_it_emits_no_data_loss_warning(recwarn):
    """The #323 warning must NOT fire for Guinea-Bissau: nothing is lost."""
    Country("Guinea-Bissau").cluster_features()
    assert [w for w in recwarn if "#323" in str(w.message)] == []


def test_rural_is_categorical_not_a_raw_code(cf):
    assert set(cf["Rural"].dropna().unique()) <= {"Rural", "Urban"}
