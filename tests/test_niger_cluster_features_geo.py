"""Niger ``cluster_features`` GPS wiring (GH #323 Site 4 / GH #515).

Niger 2011-12's ``df_geo`` sub-frame pointed at
``NER_HouseholdGeovars_Y1.dta`` and asked for ``lat_dd_mod`` / ``lon_dd_mod``.
That file is the LSMS-ISA *geovariables* extract -- derived spatial covariates
at HOUSEHOLD grain -- and it carries **no coordinate column of any name**.  The
resulting ``KeyError`` was swallowed by the GH #515 optional-sub-df fallback, so
the wave was served with ``Latitude`` / ``Longitude`` 100% absent even though
they are declared REQUIRED in ``Niger/_/data_scheme.yml``.

The coordinates DO ship, in the companion ``NER_EA_Offsets.dta``: offset
(confidentiality-displaced) EA centroids at CLUSTER grain, under the UPPERCASE
names ``LAT_DD_MOD`` / ``LON_DD_MOD``.  So the honest fix is to point at the
right file, NOT to mark the columns ``optional: true``.

The wiring was corrected in ``3488b791`` (which unblocked PR #627).  These tests
exist so it cannot silently regress.  Two of them are the *negative control* in
permanent form: they pin the column inventory of BOTH files, so a future edit
that re-points ``df_geo`` back at the geovariables file -- which would
additionally manufacture a cartesian merge, being household-grain -- fails
loudly instead of quietly losing GPS again.  A third pins the harder-to-see
failure: that "fixing" the raise by marking the columns ``optional: true``
would delete a real column across all four waves and call it a cleanup.

WHICH TESTS DISCRIMINATE WHAT (measured on a cold, isolated data root, not
assumed).  Reverting ``2011-12/_/data_info.yml`` to ``3488b791^`` with the rest
of the tree at HEAD gives, on ``development``'s core, **4 failed / 5 passed / 1
skipped**: ``..._points_at_the_ea_offsets_file``,
``..._uses_the_uppercase_offset_column_names``, ``test_merge_how_is_left`` and
``test_2011_12_clusters_all_have_coordinates`` fail.  On PR #627's core the
same revert gives 3 failed + 4 errors, the errors being the required-column
``RuntimeError`` reaching the module fixture -- which is the point: it is an
error, not a skip.  Dropping ONLY ``merge_how: left`` (#627's core) fails
exactly two: ``test_merge_how_is_left`` and
``test_2011_12_emits_no_nan_key_grain_warning``.

The remaining tests pin *invariants* rather than the fix -- see their
docstrings, each of which names the test that does discriminate.

MODE.  The module deliberately runs NON-strict (see ``_non_strict``): the
strict-mode condition is asserted directly, by asserting the ABSENCE of the
2011-12 ``GrainCollapseWarning``, which is the same event
``LSMS_GRAIN_STRICT=1`` raises on.  Running the module itself under
``LSMS_GRAIN_STRICT=1`` would instead abort the country build on Niger
2014-15's pre-existing household-grain conflict (42 of 3,617 rows; GH #614
Site 1, owned by GH #637), which has nothing to do with the geo wiring.

Data-dependent tests do NOT swallow exceptions.  The data-free CI job is
handled by the missing-credentials net in ``tests/conftest.py`` -- which
converts a ``NoCredentialsError`` (in a test OR in fixture setup) into a skip,
and *only* that.  Anything else -- a ``GrainCollapseError``, a ``RuntimeError``
from a failed build -- must turn this file red, not green-by-skip.
"""
import inspect
import os
import warnings

import pytest
import yaml

import lsms_library as ll
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

WAVE = "2011-12"
SUBDIR = "NER_2011_ECVMA_v01_M_Stata8"
OFFSETS = f"{SUBDIR}/NER_EA_Offsets.dta"
GEOVARS = f"{SUBDIR}/NER_HouseholdGeovars_Y1.dta"


def _core_reads_merge_how() -> bool:
    """Whether the installed core actually honours ``merge_how:``.

    ``merge_how`` is introduced by PR #627; on ``development`` the ``dfs:``
    merge is hardcoded to ``how='outer'``, so the key sits in Niger's YAML
    inert.  Tests whose subject is the *effect* of ``merge_how`` are skipped
    on such a core rather than left to fail for the wrong reason.
    """
    import lsms_library.country as _country

    return "merge_how" in inspect.getsource(_country)


@pytest.fixture(scope="module", autouse=True)
def _non_strict():
    """Run this module in the default (warn, don't raise) grain mode.

    See the module docstring: the strict condition is asserted directly, as
    the absence of a warning, so the module does not need -- and must not
    depend on -- ``LSMS_GRAIN_STRICT`` being set by the caller.
    """
    prior = os.environ.pop("LSMS_GRAIN_STRICT", None)
    yield
    if prior is not None:
        os.environ["LSMS_GRAIN_STRICT"] = prior


@pytest.fixture(scope="module")
def niger_root():
    root = countries_root() / "Niger"
    if not root.is_dir():
        pytest.skip("Niger config tree unavailable")
    return root


@pytest.fixture(scope="module")
def cluster_spec(niger_root):
    with open(niger_root / WAVE / "_" / "data_info.yml") as f:
        info = yaml.safe_load(f)
    return info["cluster_features"]


@pytest.fixture(scope="module")
def geo_spec(cluster_spec):
    return cluster_spec["df_geo"]


def _read(path):
    from lsms_library.local_tools import get_dataframe

    return get_dataframe(str(path))


# --------------------------------------------------------------------------
# Config: the wiring itself
# --------------------------------------------------------------------------

def test_geo_subdf_points_at_the_ea_offsets_file(geo_spec):
    """Not the geovariables file -- it has no coordinates and is HH grain."""
    assert geo_spec["file"] == OFFSETS
    assert geo_spec["file"] != GEOVARS


def test_geo_subdf_uses_the_uppercase_offset_column_names(geo_spec):
    assert geo_spec["myvars"]["Latitude"] == "LAT_DD_MOD"
    assert geo_spec["myvars"]["Longitude"] == "LON_DD_MOD"


def test_merge_how_is_left(cluster_spec):
    """The offsets file has one trailing null-``grappe`` row; ``left`` drops it.

    What this buys is narrower than it looks, and the difference was measured:
    under the default ``outer`` that row does NOT survive into the built table
    either -- the collapse to ``(t, v)`` uses ``groupby``, which drops NaN-key
    rows -- so the returned frame is byte-for-byte the same.  What ``left``
    removes is the ``nan_key_rows: 1`` grain report, i.e. a
    ``GrainCollapseWarning`` that is FATAL under ``LSMS_GRAIN_STRICT=1``.
    ``test_2011_12_emits_no_nan_key_grain_warning`` is the test that pins that
    effect; this one pins the declaration.

    NOTE the key is inert on ``development``: ``merge_how`` is introduced by
    PR #627.  Until that lands, this test pins a key nothing reads.
    """
    assert cluster_spec.get("merge_how") == "left"


def test_latlon_stay_required_in_the_country_scheme(niger_root):
    """The fix must NOT be 'silence the guard'.

    Niger ships coordinates for three of four waves, so ``optional: true``
    would disarm the GH #323/#515 required-column check for every one of them.
    """
    # `load_yaml`, not `yaml.safe_load`: data_scheme.yml carries `!make` tags.
    with open(niger_root / "_" / "data_scheme.yml") as f:
        scheme = load_yaml(f)["Data Scheme"]["cluster_features"]
    for col in ("Latitude", "Longitude"):
        entry = scheme[col]
        assert not (isinstance(entry, dict) and entry.get("optional")), (
            f"{col} was marked optional; Niger's coordinates exist "
            "(NER_EA_Offsets.dta / grappe_gps_ner2018.dta / s00_me_ner2021.dta)"
        )


# --------------------------------------------------------------------------
# Data: the evidence the wiring rests on (the negative control, pinned)
# --------------------------------------------------------------------------

def test_offsets_file_carries_cluster_grain_coordinates(niger_root):
    df = _read(niger_root / WAVE / "Data" / OFFSETS)
    assert {"grappe", "LAT_DD_MOD", "LON_DD_MOD"} <= set(df.columns)
    keyed = df.dropna(subset=["grappe"])
    assert not keyed["grappe"].duplicated().any(), "offsets are one row per EA"
    # Why `merge_how: left` is load-bearing.  `<= 1` rather than `== 1`: the
    # stray row is an artifact of this WB release, and a re-release without it
    # would be nothing to fail a test over.
    assert len(df) - len(keyed) <= 1, "unexpected extra null-key row(s)"
    # Niger is entirely within (11.7N-23.5N, 0.2E-16.0E).
    assert keyed["LAT_DD_MOD"].between(11.0, 24.0).all()
    assert keyed["LON_DD_MOD"].between(0.0, 16.5).all()


def test_geovariables_file_has_no_coordinates_at_all(niger_root):
    """The negative control: why ``df_geo`` may not point here.

    Note this asserts a fact about the DATA, so it passes with or without the
    config fix -- deliberately.  ``test_geo_subdf_points_at_the_ea_offsets_file``
    is the one that discriminates the wiring; this one is the reason that
    wiring is right.
    """
    df = _read(niger_root / WAVE / "Data" / GEOVARS)
    cols = {c.lower() for c in df.columns}
    assert not any(c.startswith(("lat", "lon")) for c in cols), sorted(cols)
    assert "grappe" in cols and len(df) > df["grappe"].nunique(), (
        "geovariables are HOUSEHOLD grain -- merging them on v alone would "
        "manufacture a cartesian (GH #323 Site 4)"
    )


# --------------------------------------------------------------------------
# End to end
#
# No `except Exception: skip` here.  A build that raises must fail this file;
# tests/conftest.py's missing-credentials net is what handles the data-free CI
# job, and nothing else is tolerated.  (That helper is not imported directly:
# the repo has a root-level conftest.py, so `from conftest import ...` resolves
# to the wrong module -- the net is a hook, and applies without an import.)
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def niger_build(_non_strict):
    """``Country('Niger').cluster_features()`` plus every warning it emitted.

    The grain report is stamped into the cached parquet and re-emitted on
    read, so the warning list is the same warm or cold -- verified both ways.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = ll.Country("Niger").cluster_features()
    assert df is not None and not df.empty, "Niger cluster_features built empty"
    return df.reset_index(), [(w.category, str(w.message)) for w in caught]


@pytest.fixture(scope="module")
def clusters(niger_build):
    return niger_build[0]


def test_2011_12_clusters_all_have_coordinates(clusters):
    w = clusters[clusters["t"].astype(str) == WAVE]
    assert len(w) > 0
    assert w["Latitude"].notna().all()
    assert w["Longitude"].notna().all()
    assert w["Latitude"].between(11.0, 24.0).all()
    assert w["Longitude"].between(0.0, 16.5).all()


def test_2011_12_is_at_cluster_grain(clusters):
    """Invariant, not discrimination.

    Measured: this passes with and without ``merge_how: left`` -- the collapse
    to ``(t, v)`` guarantees a unique index by construction.  It is kept as a
    floor on the shape of the table, not as a test of the geo fix;
    ``test_2011_12_emits_no_nan_key_grain_warning`` is the one that sees the
    difference.
    """
    w = clusters[clusters["t"].astype(str) == WAVE]
    assert not w["v"].duplicated().any()


def test_no_phantom_null_key_cluster(clusters):
    """Invariant, not discrimination -- same caveat as the test above.

    The null-key row from the offsets file never reaches the built table under
    EITHER merge mode: ``groupby`` drops it.  Measured both ways.
    """
    w = clusters[clusters["t"].astype(str) == WAVE]
    assert w["v"].notna().all()
    assert "nan" not in set(w["v"].astype(str))


@pytest.mark.skipif(
    not _core_reads_merge_how(),
    reason="this core hardcodes how='outer' on the dfs: merge (merge_how "
           "arrives with PR #627), so the nan-key row is unavoidable here",
)
def test_2011_12_emits_no_nan_key_grain_warning(niger_build):
    """What ``merge_how: left`` actually buys, and the only test that sees it.

    Without it the offsets file's trailing null-``grappe`` row reaches the
    ``(t, v)`` collapse and is deleted there, which the grain reporter records
    as ``nan_key_rows: 1`` -- a ``GrainCollapseWarning``, and a
    ``GrainCollapseError`` under ``LSMS_GRAIN_STRICT=1``.  Measured: reverting
    ``merge_how`` reproduces exactly this warning, at HEAD there is none.

    Scoped to 2011-12 on purpose.  Niger 2014-15 emits its own, unrelated
    grain warning (a household-grain projection whose households disagree --
    GH #614 Site 1 / GH #637); asserting silence for the whole country would
    make this test about someone else's bug.
    """
    _, caught = niger_build
    offenders = [
        msg for cat, msg in caught
        if cat.__name__ == "GrainCollapseWarning"
        and f"cluster_features/{WAVE}" in msg
    ]
    assert not offenders, (
        "Niger 2011-12 cluster_features emitted a grain warning; if it "
        "mentions NaN in a declared index level, `merge_how: left` was lost "
        f"from the wave's cluster_features spec:\n{offenders}"
    )
