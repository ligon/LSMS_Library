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

Data-dependent tests skip cleanly when the Niger sources are unavailable.
"""
import pytest
import yaml

import lsms_library as ll
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

WAVE = "2011-12"
SUBDIR = "NER_2011_ECVMA_v01_M_Stata8"
OFFSETS = f"{SUBDIR}/NER_EA_Offsets.dta"
GEOVARS = f"{SUBDIR}/NER_HouseholdGeovars_Y1.dta"


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

    try:
        return get_dataframe(str(path))
    except Exception as exc:  # noqa: BLE001 - any access failure -> skip
        pytest.skip(f"Niger source unavailable: {exc}")


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
    """Load-bearing: the offsets file has one trailing null-``grappe`` row.

    Under the default ``outer`` that row arrives as a phantom cluster with a
    null ``v``; ``left`` drops it, ``df_main`` being authoritative about which
    clusters exist.
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
    # The reason `merge_how: left` is load-bearing.
    assert len(df) - len(keyed) == 1, "expected exactly one trailing null-key row"
    # Niger is entirely within (11.7N-23.5N, 0.2E-16.0E).
    assert keyed["LAT_DD_MOD"].between(11.0, 24.0).all()
    assert keyed["LON_DD_MOD"].between(0.0, 16.5).all()


def test_geovariables_file_has_no_coordinates_at_all(niger_root):
    """The negative control: why ``df_geo`` may not point here."""
    df = _read(niger_root / WAVE / "Data" / GEOVARS)
    cols = {c.lower() for c in df.columns}
    assert not any(c.startswith(("lat", "lon")) for c in cols), sorted(cols)
    assert "grappe" in cols and len(df) > df["grappe"].nunique(), (
        "geovariables are HOUSEHOLD grain -- merging them on v alone would "
        "manufacture a cartesian (GH #323 Site 4)"
    )


# --------------------------------------------------------------------------
# End to end
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def clusters():
    try:
        df = ll.Country("Niger").cluster_features()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Niger cluster_features unavailable: {exc}")
    if df is None or df.empty:
        pytest.skip("Niger cluster_features empty")
    return df.reset_index()


def test_2011_12_clusters_all_have_coordinates(clusters):
    w = clusters[clusters["t"].astype(str) == WAVE]
    assert len(w) > 0
    assert w["Latitude"].notna().all()
    assert w["Longitude"].notna().all()
    assert w["Latitude"].between(11.0, 24.0).all()
    assert w["Longitude"].between(0.0, 16.5).all()


def test_2011_12_is_at_cluster_grain(clusters):
    w = clusters[clusters["t"].astype(str) == WAVE]
    assert not w["v"].duplicated().any()


def test_no_phantom_null_key_cluster(clusters):
    """What ``merge_how: left`` buys, checked on the built table."""
    w = clusters[clusters["t"].astype(str) == WAVE]
    assert w["v"].notna().all()
    assert "nan" not in set(w["v"].astype(str))
