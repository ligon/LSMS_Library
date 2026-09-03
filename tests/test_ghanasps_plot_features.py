"""GhanaSPS ``plot_features`` -- the GH #732 hazard, pinned (refs #729, #140).

``2017-18/Data/04h_agsection.dta`` carries two unit columns.  ``plotsizeunit``
is the natural-looking name and the one 2013-14 uses, but in 2017-18 it is a
skip-pattern remnant asked only of the 575 plots whose size was NOT given in
acres; the producer-resolved unit for every plot is ``plotunit``.  Wiring the
wrong one leaves 4,791 of 5,366 plots (89.3%) with no unit and therefore no
``Area`` -- the only column the canonical schema marks REQUIRED anywhere in
the agriculture set -- and NEITHER framework guard fires: the Site B null-read
guard fires at 100% null, and ``Country._assert_built_required_columns``
checks presence, not content.  That failure ships green.

So the load-bearing check here is a CONTENT one: per-wave ``Area`` non-null
>= 95% (measured on the cold build: 97.73% / 99.79% / 99.63%).  The config
tests pin the wiring itself -- W3 reads ``plotunit``, W2 reads
``plotsizeunit`` (where it IS the resolved unit), every wave merges ``left``
on the plot key -- so a regression is caught without data too.

Data-dependent tests are marked ``requires_s3`` (the conftest's documented
spelling; nothing is imported from it) and are skipped in the data-free CI
job.  They do NOT swallow exceptions: a ``GrainCollapseError`` or a failed
build must turn this file red.  The grain condition is asserted directly --
zero ``GrainCollapseWarning`` for ``plot_features`` -- rather than by running
under ``LSMS_GRAIN_STRICT``, so the module does not depend on the caller's
environment.  (``LSMS_READ_STRICT=1`` is deliberately NOT relied on either:
on this branch it is fatal inside ``GhanaSPS/sample`` -- ``Rural``,
``weight``, ``panel_weight`` are 100% null in 2013-14 / 2017-18, a documented
property of that table -- which ``_join_v_from_sample`` re-enters for every
household table.  The read-strict condition for THIS table is asserted
directly via ``null_read_reports``.)
"""
import os
import warnings

import pytest
import yaml
from importlib.resources import files

import lsms_library as ll
from lsms_library.local_tools import all_dfs_from_orgfile
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

WAVES = ("2009-10", "2013-14", "2017-18")


# --------------------------------------------------------------------------
# Config: the wiring itself (data-free)
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def gsps_root():
    root = countries_root() / "GhanaSPS"
    if not root.is_dir():
        pytest.skip("GhanaSPS config tree unavailable")
    return root


@pytest.fixture(scope="module")
def wave_specs(gsps_root):
    out = {}
    for w in WAVES:
        with open(gsps_root / w / "_" / "data_info.yml") as f:
            out[w] = yaml.safe_load(f)["plot_features"]
    return out


def _areaunit_source(spec):
    src = spec["df_size"]["myvars"]["AreaUnit"]
    return src[0] if isinstance(src, list) else src


def test_w3_size_subdf_reads_plotunit_not_plotsizeunit(wave_specs):
    """The #732 wiring, pinned: 2017-18 must read the resolved unit column."""
    assert _areaunit_source(wave_specs["2017-18"]) == "plotunit"
    assert _areaunit_source(wave_specs["2017-18"]) != "plotsizeunit"


def test_w2_size_subdf_reads_plotsizeunit(wave_specs):
    """In 2013-14 ``plotsizeunit`` IS the resolved unit (4,694/4,694) and
    ``plotunit`` does not exist -- the asymmetry is the whole trap."""
    assert _areaunit_source(wave_specs["2013-14"]) == "plotsizeunit"


@pytest.mark.parametrize("wave", WAVES)
def test_every_wave_merges_left_on_the_plot_key(wave_specs, wave):
    spec = wave_specs[wave]
    assert spec["dfs"][0] == "df_size", "size file must be the primary sub-df"
    assert spec.get("merge_how") == "left"
    assert set(spec["merge_on"]) == {"i", "plot_id"}
    assert list(spec["final_index"]) == ["t", "i", "plot_id"]
    assert "v" not in spec["final_index"]


def test_scheme_index_has_no_v_and_area_is_required(gsps_root):
    # `load_yaml`, not `yaml.safe_load`: data_scheme.yml carries `!make` tags.
    with open(gsps_root / "_" / "data_scheme.yml") as f:
        entry = load_yaml(f)["Data Scheme"]["plot_features"]
    assert entry["index"].replace(" ", "") == "(t,i,plot_id)"
    area = entry["Area"]
    assert not (isinstance(area, dict) and area.get("optional")), (
        "Area is the one required column; marking it optional would disarm "
        "the guard this file exists to keep armed")


def _declared_tenure_vocabulary():
    with open(files("lsms_library") / "data_info.yml", encoding="utf-8") as f:
        info = yaml.safe_load(f)
    return set(info["Columns"]["plot_features"]["Tenure"]["spellings"])


def test_tenure_table_targets_are_all_canonical(gsps_root):
    """Every Preferred Label in the org table is in the declared vocabulary.

    ``_apply_categorical_mappings`` / the extraction-time lookup pass an
    unmatched value through unchanged, so a typo here would surface as a raw
    survey label in the table; this pins the TARGET side, the data test below
    pins the delivered side.
    """
    tables = all_dfs_from_orgfile(gsps_root / "_" / "categorical_mapping.org")
    targets = set(tables["Tenure"]["Preferred Label"].dropna())
    assert targets <= _declared_tenure_vocabulary(), targets - _declared_tenure_vocabulary()


# --------------------------------------------------------------------------
# Data: the content check no framework guard makes
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def gsps_build():
    """``Country('GhanaSPS').plot_features()`` plus every warning it emitted."""
    from lsms_library.null_read_audit import null_read_reports

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = ll.Country("GhanaSPS").plot_features()
    assert df is not None and not df.empty, "GhanaSPS plot_features built empty"
    grain = [str(w.message) for w in caught
             if w.category.__name__ == "GrainCollapseWarning"
             and "plot_features" in str(w.message)]
    nullread = null_read_reports(country="GhanaSPS", table="plot_features")
    return df.reset_index(), grain, nullread


@pytest.mark.requires_s3
@pytest.mark.parametrize("wave", WAVES)
def test_area_non_null_at_least_95pct_per_wave(gsps_build, wave):
    """GH #732.  A wrong unit column lands at ~11% here and ships green."""
    df, _, _ = gsps_build
    w = df[df["t"].astype(str) == wave]
    assert len(w) > 0, f"no {wave} rows"
    share = w["Area"].notna().mean()
    assert share >= 0.95, f"{wave}: Area non-null {share:.1%} (< 95%)"


@pytest.mark.requires_s3
def test_w3_area_unit_is_acres_in_the_thousands(gsps_build):
    """The symptom of #732 in AreaUnit terms: W3 acres must number thousands."""
    df, _, _ = gsps_build
    w3 = df[df["t"].astype(str) == "2017-18"]
    assert (w3["AreaUnit"] == "Acres").sum() >= 4000
    assert "Ropes" in set(w3["AreaUnit"].dropna()), "'Robes' was not folded onto Ropes"


@pytest.mark.requires_s3
def test_index_unique_no_grain_warning_no_null_read(gsps_build):
    df, grain, nullread = gsps_build
    assert not df.duplicated(subset=["t", "i", "plot_id"]).any()
    assert df["plot_id"].notna().all(), "a null plot_id survived the hook"
    assert not grain, grain
    assert not nullread, nullread


@pytest.mark.requires_s3
def test_tenure_values_within_declared_vocabulary(gsps_build):
    """The subset assertion -- a plausible distribution proves nothing, because
    an unmapped label passes through unchanged."""
    df, _, _ = gsps_build
    vocab = _declared_tenure_vocabulary()
    for wave in WAVES:
        got = set(df.loc[df["t"].astype(str) == wave, "Tenure"].dropna())
        assert got <= vocab, f"{wave}: {got - vocab}"


@pytest.mark.requires_s3
def test_v_join_is_total(gsps_build):
    df, _, _ = gsps_build
    assert "v" in df.columns
    assert df["v"].notna().all()


@pytest.mark.requires_s3
def test_area_scale_is_hectares(gsps_build):
    """Factor sanity: a Ghanaian smallholder plot is ~1 ha.  A 99th percentile
    above ~40 ha in any wave would mean a factor is wrong (measured: 9.6 /
    14.4 / 12.0)."""
    df, _, _ = gsps_build
    for wave in WAVES:
        a = df.loc[df["t"].astype(str) == wave, "Area"].dropna()
        assert 0.3 < a.median() < 3.0, f"{wave}: median {a.median():.2f} ha"
        assert a.quantile(0.99) < 40, f"{wave}: p99 {a.quantile(0.99):.1f} ha"
