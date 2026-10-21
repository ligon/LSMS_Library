"""Burkina Faso ``cluster_features`` GPS wiring (GH #1050).

Both of Burkina Faso's ``df_geo`` sub-frames (2014, 2018-19) asked for
``lat_dd_mod`` / ``lon_dd_mod``, which neither file carries.  The ``KeyError``
was swallowed by the GH #515 optional-sub-df fallback and, because
``Latitude`` / ``Longitude`` are ``optional: true`` in
``Burkina_Faso/_/data_scheme.yml``, the GH #323/#515 required-column guard
never fired: every Burkina Faso cluster was served with no GPS.

What each wave actually ships (measured; evidence in ``_/CONTENTS.org``,
"cluster_features -- GPS by wave"):

- **2018-19** -- ``grappe_gps_bfa2018.dta``, one row per grappe (585), the
  coordinates under ``coordonnes_gps__Latitude`` / ``__Longitude`` (the
  CotedIvoire 2018-19 spelling).  555 grappes carry a fix, 30 do not.
- **2014** -- no coordinate anywhere in the distribution.
  ``emc2014_agri_gps.dta`` is the parcel GPS-*area* file: ``V20`` "La parcelle
  a-t-elle ete mesuree par le GPS?" and ``V22``, the measured area in
  hectares.  ``df_geo`` is removed rather than pointed elsewhere.
- **2021-22** -- the household GPS variables in ``s00_me_bfa2021.dta`` are
  present and 100% empty, and no grappe-level file ships.

So ``optional: true`` stays: two of three waves have no cluster GPS, and the
flag is country-grain.  ``test_optional_matches_the_waves_that_lack_gps`` pins
that it stays *only* while that is true.

WHICH TESTS DISCRIMINATE (measured, each revert alone against the fix).
Reverting ``2018-19/_/data_info.yml`` to the ``lat_dd_mod`` spelling: 4 failed
/ 6 passed -- ``test_2018_19_geo_subdf_maps_the_coordonnes_gps_columns``,
``test_no_wave_asks_for_lat_dd_mod``, ``test_2018_19_clusters_carry_gps`` and
``test_waves_without_gps_are_served_empty_not_invented`` (no wave supplies the
columns, so they are absent rather than empty).  Restoring the 2014
``df_geo``: 3 failed / 7 passed -- ``test_2014_declares_no_geo_subdf``,
``test_no_wave_asks_for_lat_dd_mod`` and
``test_optional_matches_the_waves_that_lack_gps``.
The ``*_file_*`` tests assert facts about the DATA and pass either way; they
are the reasons the wiring is right, and they turn red if a re-release
changes those facts (e.g. 2021-22 starts shipping household GPS).

Data-dependent tests do NOT swallow exceptions; ``requires_s3`` plus the
missing-credentials net in ``tests/conftest.py`` handle the data-free CI job.
"""
import importlib.util
import os
import pathlib
import warnings

import pytest
import yaml

import lsms_library as ll
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml


def _tests_conftest():
    """``tests/conftest.py``, loaded by path (the root conftest shadows it)."""
    path = pathlib.Path(__file__).with_name("conftest.py")
    spec = importlib.util.spec_from_file_location("lsms_tests_conftest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


requires_s3 = _tests_conftest().requires_s3

COUNTRY = "Burkina_Faso"
WAVES = ("2014", "2018-19", "2021-22")
GPS_2018 = "grappe_gps_bfa2018.dta"
AGRI_GPS_2014 = "emc2014_agri_gps.dta"
S00_2021 = "s00_me_bfa2021.dta"

# Burkina Faso lies within roughly 9.4N-15.1N, 5.5W-2.4E.
LAT = (9.3, 15.2)
LON = (-5.6, 2.5)


@pytest.fixture(scope="module", autouse=True)
def _non_strict():
    """Warn-mode grain checks.  2014 carries a pre-existing, unrelated
    ``GrainCollapseWarning`` (94 zd whose households disagree on
    Region/District/Rural) that ``LSMS_GRAIN_STRICT=1`` would turn into an
    abort of the whole country build."""
    prior = os.environ.pop("LSMS_GRAIN_STRICT", None)
    yield
    if prior is not None:
        os.environ["LSMS_GRAIN_STRICT"] = prior


@pytest.fixture(scope="module")
def bf_root():
    root = countries_root() / COUNTRY
    if not root.is_dir():
        pytest.skip("Burkina_Faso config tree unavailable")
    return root


def _cluster_spec(bf_root, wave):
    with open(bf_root / wave / "_" / "data_info.yml") as f:
        return yaml.safe_load(f)["cluster_features"]


def _mapped_columns(spec):
    """Every target column a ``cluster_features`` spec maps, over its sub-dfs."""
    subs = [spec[name] for name in spec["dfs"]] if spec.get("dfs") else [spec]
    return {col for sub in subs for col in (sub.get("myvars") or {})}


def _read(path):
    from lsms_library.local_tools import get_dataframe

    return get_dataframe(str(path))


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

def test_2018_19_geo_subdf_maps_the_coordonnes_gps_columns(bf_root):
    spec = _cluster_spec(bf_root, "2018-19")
    assert "df_geo" in spec["dfs"]
    geo = spec["df_geo"]
    assert geo["file"] == GPS_2018
    assert geo["idxvars"]["v"] == "grappe"
    assert geo["myvars"]["Latitude"] == "coordonnes_gps__Latitude"
    assert geo["myvars"]["Longitude"] == "coordonnes_gps__Longitude"


def test_2014_declares_no_geo_subdf(bf_root):
    """The EMC 2014 files carry no coordinate; a ``df_geo`` there can only
    be dropped on every build."""
    spec = _cluster_spec(bf_root, "2014")
    assert "df_geo" not in spec
    assert "df_geo" not in (spec.get("dfs") or [])
    assert not {"Latitude", "Longitude"} & _mapped_columns(spec)


def test_no_wave_asks_for_lat_dd_mod(bf_root):
    """The original bug: the LSMS-ISA offset-file spelling, which no Burkina
    Faso file carries."""
    for wave in WAVES:
        text = yaml.safe_dump(_cluster_spec(bf_root, wave)).lower()
        assert "lat_dd_mod" not in text and "lon_dd_mod" not in text, wave


def test_optional_matches_the_waves_that_lack_gps(bf_root):
    """``optional: true`` is country-grain, so it is honest only while some
    wave genuinely has no GPS -- and it is what hid #1050.  If every wave
    comes to map ``Latitude``/``Longitude``, drop the flag so the GH
    #323/#515 required-column guard is armed again."""
    lacking = [w for w in WAVES
               if not {"Latitude", "Longitude"} <= _mapped_columns(
                   _cluster_spec(bf_root, w))]
    # `load_yaml`, not `yaml.safe_load`: data_scheme.yml carries `!make` tags.
    with open(bf_root / "_" / "data_scheme.yml") as f:
        scheme = load_yaml(f)["Data Scheme"]["cluster_features"]
    for col in ("Latitude", "Longitude"):
        entry = scheme[col]
        optional = isinstance(entry, dict) and bool(entry.get("optional"))
        assert optional == bool(lacking), (
            f"{col}: optional={optional} but waves without GPS = {lacking}")
    assert lacking == ["2014", "2021-22"]


# --------------------------------------------------------------------------
# Data: the facts the wiring rests on
# --------------------------------------------------------------------------

@requires_s3
def test_2018_19_gps_file_is_one_fix_per_grappe(bf_root):
    df = _read(bf_root / "2018-19" / "Data" / GPS_2018)
    assert not df["grappe"].isna().any()
    assert not df["grappe"].duplicated().any(), "one row per grappe"
    assert len(df) == 585
    fixed = df.dropna(subset=["coordonnes_gps__Latitude"])
    assert len(fixed) == 555
    assert fixed["coordonnes_gps__Longitude"].notna().all()
    assert fixed["coordonnes_gps__Latitude"].between(*LAT).all()
    assert fixed["coordonnes_gps__Longitude"].between(*LON).all()


@requires_s3
def test_2014_agri_gps_file_has_no_coordinate(bf_root):
    """The negative control: why 2014 has no ``df_geo``.  The only file named
    for GPS records a parcel AREA measured by GPS, not a position."""
    df = _read(bf_root / "2014" / "Data" / AGRI_GPS_2014)
    cols = {c.lower() for c in df.columns}
    assert not any(c.startswith(("lat", "lon", "coord", "gps")) for c in cols), (
        sorted(cols))
    assert {"v20", "v22"} <= cols
    assert set(df["V20"].dropna().astype(str)) <= {"Oui", "Non"}
    assert df["V22"].dropna().between(0, 1000).all(), "hectares (999.99 = NA)"


@requires_s3
def test_2021_22_household_gps_file_is_blank(bf_root):
    """2021-22 ships the household GPS variables but not one value.  If a
    re-release populates them, map them (household grain: see GH #323 Site 2)
    and revisit ``optional: true``."""
    df = _read(bf_root / "2021-22" / "Data" / S00_2021)
    assert {"GPS__Latitude", "GPS__Longitude"} <= set(df.columns)
    assert df["GPS__Latitude"].isna().all()
    assert df["GPS__Longitude"].isna().all()


# --------------------------------------------------------------------------
# End to end
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def bf_build(_non_strict):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = ll.Country(COUNTRY).cluster_features()
    assert df is not None and not df.empty
    return df.reset_index(), [(w.category, str(w.message)) for w in caught]


@pytest.fixture(scope="module")
def clusters(bf_build):
    return bf_build[0]


def _wave(clusters, wave):
    return clusters[clusters["t"].astype(str) == wave]


@requires_s3
def test_2018_19_clusters_carry_gps(clusters):
    w = _wave(clusters, "2018-19")
    assert len(w) == 585 and not w["v"].duplicated().any()
    fixed = w.dropna(subset=["Latitude"])
    assert len(fixed) == 555, len(fixed)
    assert fixed["Longitude"].notna().all()
    assert fixed["Latitude"].between(*LAT).all()
    assert fixed["Longitude"].between(*LON).all()


@requires_s3
def test_waves_without_gps_are_served_empty_not_invented(clusters):
    for wave in ("2014", "2021-22"):
        w = _wave(clusters, wave)
        assert len(w) > 0, wave
        assert w["Latitude"].isna().all() and w["Longitude"].isna().all(), wave


@requires_s3
def test_2018_19_gps_merge_is_lossless(bf_build):
    """GPS is a grappe constant, so projecting the household-grain merge onto
    ``(t, v)`` destroys nothing.  Scoped to 2018-19: 2014's own grain
    warning is pre-existing and unrelated."""
    _, caught = bf_build
    offenders = [msg for cat, msg in caught
                 if cat.__name__ == "GrainCollapseWarning"
                 and "cluster_features/2018-19" in msg]
    assert not offenders, offenders
