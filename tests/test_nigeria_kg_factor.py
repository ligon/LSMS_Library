"""Nigeria ``crop_production.KgFactor`` -- the per-row ``*_conv`` factor (GH #859).

The Nigeria GHS-Panel harvest modules ship a *Kg/L conversion factor* beside
the harvest quantity and its unit.  It is a RATE (kilograms per ONE unit of
the row's ``u``), it is REPORTED and never constructed, and
``transformations.harvest_kg`` already prefers such a column as its first
layer -- so carrying it moves ``Harvest_kg`` with no other code change.

Two things are pinned here, and they fail differently:

1. **The wiring** (data-free): ``nigeria.crop_production_for_wave`` must put
   the named ``*_conv`` column on the row VERBATIM, and must deliver a FLOAT
   all-NaN column for a frame that names none.  A fixture frame is enough --
   the property is positional identity, not a number.
2. **What is NOT wired** (data-free, config): each wave's frame dict may name
   only a ``*_conv`` that pairs with the quantity/unit that frame stores.
   ``sa3iq6d_conv`` / ``sa3iq15_conv`` sit in the *"how much MORE does HH
   expect to harvest"* block with their own unit columns, and
   ``sa3iiq1_conv`` is ``secta3ii`` at hh-crop grain -- wiring any of them
   would attach one container's factor to another container's quantity.
3. **The counts** (data-gated): per-``t`` non-null ``KgFactor`` on the built
   table.  Skips cleanly where no build is available.

Schema facts are READ from the country's ``data_scheme.yml`` and from
``lsms_library/data_info.yml``, never hardcoded (CLAUDE.md, "Canonical
Schema").
"""
from __future__ import annotations

import importlib.util
import sys

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

NIGERIA = countries_root() / "Nigeria"

# Non-null KgFactor delivered per t, measured on a cold isolated build
# 2026-09-09.  Built <= raw: crop_production drops rows with no resolved crop
# label and de-duplicates on (t, i, plot, crop).  Raw non-null in the wired
# columns is 10 845 (sa3iq6_conv) + 695 (sa3iiiq13_conv) for 2019Q1 and
# 8 092 (sa3iq9_conv) + 295 (sa3iiiq23_conv) for 2024Q1.
EXPECTED_KGFACTOR_NONNULL = {
    "2011Q1": 0,
    "2013Q1": 0,
    "2016Q1": 0,
    "2019Q1": 11393,
    "2024Q1": 8285,
}

# The columns each wave's frames may name, and the ones they may not.
WIRED = {"sa3iq6_conv", "sa3iiiq13_conv", "sa3iq9_conv", "sa3iiiq23_conv"}
NOT_WIRED = {"sa3iq6d_conv", "sa3iq15_conv", "sa3iiq1_conv"}


# --------------------------------------------------------------------------
# Config (data-free)
# --------------------------------------------------------------------------

def test_data_scheme_declares_kgfactor_optional_float():
    scheme = load_yaml(NIGERIA / "_" / "data_scheme.yml")["Data Scheme"]
    cp = scheme["crop_production"]
    assert "KgFactor" in cp, "Nigeria crop_production must declare KgFactor"
    assert cp["KgFactor"] == {"type": "float", "optional": True}


def test_kgfactor_is_optional_in_the_canonical_schema():
    """`optional` is not a Nigeria opinion -- the canonical schema says so."""
    info = load_yaml(countries_root().parent / "data_info.yml")
    canon = info["Columns"]["crop_production"]["KgFactor"]
    assert canon["type"] == "float"
    assert canon["optional"] is True


def test_only_the_paired_conv_columns_are_wired():
    """A `*_conv` asked beside a DIFFERENT quantity must not be named here."""
    src = (NIGERIA / "_" / "crop_production.py").read_text()
    named = {ln.split("kg_factor='")[1].split("'")[0]
             for ln in src.splitlines() if "kg_factor='" in ln}
    assert named == WIRED, named
    for bad in NOT_WIRED:
        assert f"kg_factor='{bad}'" not in src, bad


# --------------------------------------------------------------------------
# The wiring itself (data-free fixture frames)
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def nigeria_mod():
    """Import ``Nigeria/_/nigeria.py`` directly (it is not a package module)."""
    path = NIGERIA / "_" / "nigeria.py"
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location("nigeria_for_test", path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


def _frame(conv=None):
    """Three plot-crop rows in the shape ``crop_production_for_wave`` reads."""
    raw = pd.DataFrame({
        "hhid": [1, 2, 3],
        "plotid": [1, 1, 2],
        "cropcode": [1080, 1080, 2030],
        "qty": [10.0, 4.0, 7.0],
        "unit": [1, 2, 2],
        "conv": [1.0, 88.5, np.nan],
    })
    dec = raw.copy()
    dec["unit"] = ["1. KG", "2. SACK/BAG (100KG)", "2. SACK/BAG (100KG)"]
    fr = dict(df=raw, dec=dec, hhid="hhid", plot="plotid", crop="cropcode",
              qty="qty", unit="unit", perennial=False)
    if conv is not None:
        fr["kg_factor"] = conv
    return raw, fr


def test_kgfactor_equals_its_conv_source(nigeria_mod):
    raw, fr = _frame(conv="conv")
    out = nigeria_mod.crop_production_for_wave(
        "2019Q1", [fr], {1080: "Maize", 2030: "Plantain"})
    assert "KgFactor" in out.columns
    # One row per (t, i, plot, crop), and each row's factor is its OWN
    # source value -- looked up by key, so a re-ordering cannot pass.
    got = {(i, plot, crop): f
           for (_, i, plot, crop), f in out["KgFactor"].items()}
    want = {("1", "1", "Maize"): 1.0,
            ("2", "1", "Maize"): 88.5,
            ("3", "2", "Plantain"): None}
    assert set(got) == set(want)
    for k, v in want.items():
        if v is None:
            assert pd.isna(got[k]), (k, got[k])
        else:
            assert got[k] == pytest.approx(v), (k, got[k])
    # ... and it IS the source column, not a coincidence of these values.
    assert sorted(raw["conv"].dropna()) == sorted(
        float(x) for x in out["KgFactor"].dropna())


def test_no_kg_factor_key_gives_a_float_all_nan_column(nigeria_mod):
    """W1-W3 ship no ``*_conv`` at all; the column must still be FLOAT.

    Object dtype here would survive the country-level concat and land an
    object column in the parquet.
    """
    _, fr = _frame(conv=None)
    out = nigeria_mod.crop_production_for_wave("2011Q1", [fr], {1080: "Maize",
                                                                2030: "Plantain"})
    assert "KgFactor" in out.columns
    assert out["KgFactor"].dtype == np.dtype("float64")
    assert out["KgFactor"].isna().all()


def test_a_non_positive_reported_factor_is_read_as_not_recorded(nigeria_mod):
    """0 kg per unit is not a weight.  (Measured no-op on Nigeria's own
    data -- every wired column reports positive-or-null -- but the contract
    is the same one Uganda's wiring states.)"""
    raw, fr = _frame(conv="conv")
    raw.loc[0, "conv"] = 0.0
    out = nigeria_mod.crop_production_for_wave(
        "2019Q1", [fr], {1080: "Maize", 2030: "Plantain"})
    assert out["KgFactor"].isna().sum() == 2


def test_a_frame_with_a_factor_and_one_without_concatenate_as_float(nigeria_mod):
    """The mixed case: annual frame wired, perennial frame not."""
    _, fa = _frame(conv="conv")
    _, fp = _frame(conv=None)
    fp["df"] = fp["df"].assign(plotid=[9, 9, 9])
    fp["dec"] = fp["dec"].assign(plotid=[9, 9, 9])
    fp["perennial"] = True
    out = nigeria_mod.crop_production_for_wave(
        "2019Q1", [fa, fp], {1080: "Maize", 2030: "Plantain"})
    assert out["KgFactor"].dtype == np.dtype("float64")
    assert out["KgFactor"].notna().sum() == 2


# --------------------------------------------------------------------------
# The built table (data-gated)
# --------------------------------------------------------------------------

@pytest.fixture(scope="module")
def built():
    import lsms_library as ll
    try:
        df = ll.Country("Nigeria").crop_production()
    except Exception as exc:                       # pragma: no cover
        pytest.skip(f"Nigeria crop_production unavailable: {exc!r}")
    if df is None or not len(df):                  # pragma: no cover
        pytest.skip("Nigeria crop_production built empty")
    return df


@pytest.mark.slow
def test_built_kgfactor_nonnull_counts(built):
    assert "KgFactor" in built.columns
    got = (built["KgFactor"].notna()
           .groupby(level="t").sum().astype(int).to_dict())
    assert got == EXPECTED_KGFACTOR_NONNULL


@pytest.mark.slow
def test_built_kgfactor_is_a_positive_float_rate(built):
    kgf = pd.to_numeric(built["KgFactor"], errors="coerce")
    present = kgf.dropna()
    assert len(present)
    assert (present > 0).all(), "a reported factor is never <= 0"
    # A RATE, not a weight: nothing in Nigeria's own columns is a 999 / 9999
    # style sentinel, so nothing here should approach the screen's ceiling.
    from lsms_library.transformations import KG_FACTOR_MAX
    assert present.max() <= KG_FACTOR_MAX


@pytest.mark.slow
def test_waves_without_a_conv_column_report_nothing(built):
    """W1-W3 have no `*_conv` in ANY harvest file -- all-NaN is the truth,
    not a wiring gap, and it must stay that way."""
    for t in ("2011Q1", "2013Q1", "2016Q1"):
        assert built.xs(t, level="t")["KgFactor"].isna().all()
