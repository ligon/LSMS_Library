"""Guinea-Bissau cluster_features -- the GH #323 CONTROL case.

SCOPE NOTE (2026-07-22, after adversarial review).  The `Urbano` -> `Urban`
mapping added by this PR is an **extraction-level** fix, NOT an API-level one.
By the time this branch was written, commit `c8c25f68` (GH #602/#605) had
already added `Urbano, urbano, URBANO` to `Columns.cluster_features.Rural.
spellings` in `lsms_library/data_info.yml`, and `_enforce_canonical_spellings`
applies that at API time.  So `Country(...).cluster_features()` returns the
canonical value on plain `development` WITHOUT this PR, and every API-level
assertion below passes with or without the config change -- they pin the
invariant, they do NOT discriminate the fix.

What the config change does measurably achieve is that the **stored L2-wave
parquet** carries canonical `Urban` (2,014 rows) instead of raw `Urbano`, so a
consumer reading the parquet directly -- rather than through the Country API --
sees the canonical domain.  `test_wave_parquet_stores_canonical_rural` below is
the one test that actually discriminates; keep it that way.


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

    This is the property that makes the collapse lossless -- every duplicate is
    a byte-identical repeat, so ``.first()`` cannot pick "the wrong one" because
    there is no wrong one to pick.  If a future data re-release breaks this, the
    collapse silently starts fabricating cluster attributes.  (Making core NOTICE
    that is core's business -- GH #323 Sites 1/2 -- but this test pins the
    premise those sites rely on for Guinea-Bissau.)
    """
    flat = cf.reset_index()
    for col in ("Region", "Rural", "Latitude", "Longitude"):
        conflicting = (flat.groupby("v")[col].nunique(dropna=False) > 1).sum()
        assert conflicting == 0, f"{col} disagrees within {conflicting} cluster(s)"


def test_rural_is_categorical_not_a_raw_code(cf):
    """THE FIX (GH #323).  Guinea-Bissau is LUSOPHONE: s00q04's raw labels are
    'Rural' / 'Urbano'.  The `Rural` mapping carried French 'Urbain' keys copied
    from a francophone EHCVM sibling, so 'Urbano' matched nothing and leaked
    through UNMAPPED -- 169 of the 450 clusters carried the off-schema raw value
    'Urbano' where lsms_library/data_info.yml declares the domain to be
    {Rural, Urban}.
    """
    assert set(cf["Rural"].dropna().unique()) <= {"Rural", "Urban"}


def test_the_169_urbano_clusters_are_now_urban(cf):
    """Sizes the fix: 169 Urban / 281 Rural, none unmapped."""
    counts = cf["Rural"].value_counts(dropna=False).to_dict()
    assert counts.get("Urban") == 169, counts
    assert counts.get("Rural") == 281, counts


def test_wave_parquet_stores_canonical_rural():
    """The STORED parquet must be canonical, not merely the API view.

    This is the only assertion in this file that fails without the config
    change: `_enforce_canonical_spellings` repairs the API at read time, so an
    API-level test cannot tell whether the extraction was fixed or whether the
    safety net caught it.  Reading the wave parquet bypasses that net.

    Defence-in-depth matters here because the parquet is a published artifact:
    `docs/guide/caching.md` documents L2-wave as a consumer-visible layer, and
    CLAUDE.md notes cached parquets store PRE-transformation data, so anything
    the net fixes is invisible in storage.
    """
    from lsms_library.local_tools import data_root

    path = data_root() / "Guinea-Bissau" / "2018-19" / "_" / "cluster_features.parquet"
    if not path.exists():
        pytest.skip(f"wave parquet not materialized at {path}")

    stored = pd.read_parquet(path)
    assert "Rural" in stored.columns, stored.columns.tolist()
    values = set(stored["Rural"].dropna().astype(str))

    leaked = values - {"Rural", "Urban"}
    assert not leaked, (
        f"the STORED parquet carries non-canonical Rural values {sorted(leaked)} "
        "-- the extraction mapping is missing, and the API-time spellings net "
        "is hiding it from every other test in this file"
    )
