"""Ethiopia's WB-shipped conversion tables (GH #852, #853).

Two loaders in ``lsms_library/countries/Ethiopia/_/ethiopia.py``:

* :func:`ethiopia.crop_conversion_factors` reads ``Crop_CF_Wave{2,3,4}.dta`` /
  ``crop_cf_wave5.dta`` (crop x unit x region kg factors) and hands the result
  to ``transformations.harvest_kg(cp, shipped_factors=...)``.  It is
  ANALYST-CALLABLE and never stored -- ``crop_production``'s parquet is
  byte-identical with and without it.
* :func:`ethiopia.local_area_unit_factors` reads
  ``ET_local_area_unit_conversion.dta`` (woreda x local unit -> square metres)
  and is consumed by ``plot_features_for_wave`` to fill ``Area`` for a field
  reported only as a farmer estimate in a local unit.

Four properties carry both loaders and are pinned below:

* the two AMBIGUITIES are resolved HERE, by stated rule, because
  ``_shipped_factor_lookup`` refuses an ambiguous table: the WB's own crop-74
  / unit-62 duplicate (keep 4.34) and the decode collision our own
  ``harmonize_crop`` creates (KALE 56 + SPINACH 69 -> 'Leafy Greens', dropped);
* a wave with NO shipped file is REFUSED, never given another wave's numbers
  -- 2011-12 for the crop table, 2021-22 for the area table;
* codes are decoded through the SAME tables the wave scripts use, so ``j`` and
  ``u`` speak the frame's vocabulary; the match rate is measured, not assumed
  (a mismatch matches nothing, silently);
* an implausible converted area is COUNTED and REFUSED, never clipped.
"""
from __future__ import annotations

import importlib.util
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root
from lsms_library.transformations import (
    SHIPPED_FACTOR_JOIN_LEVELS,
    harvest_kg,
    yield_kg,
)


@pytest.fixture(scope="module")
def ethiopia():
    """Import ``Ethiopia/_/ethiopia.py`` as a module, wherever the tree is."""
    path = countries_root() / "Ethiopia" / "_" / "ethiopia.py"
    if not path.exists():                       # pragma: no cover
        pytest.skip(f"Ethiopia country module not found at {path}")
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location("ethiopia_under_test",
                                                      path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    finally:
        sys.path.remove(str(path.parent))
    return mod


def _load(mod, **kw):
    """Call the crop loader, tolerating its documented decode-collision warning."""
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*DISAGREE on.*")
        return mod.crop_conversion_factors(**kw)


# ---------------------------------------------------------------------------
# Config-only: no source data needed
# ---------------------------------------------------------------------------

def test_wave_file_maps_name_the_waves_that_actually_ship_a_file(ethiopia):
    """2011-12 has no Crop_CF; 2021-22 has no area table.  Both are facts."""
    assert set(ethiopia.CROP_CF_FILES) == {"2013-14", "2015-16", "2018-19",
                                           "2021-22"}
    assert "2011-12" not in ethiopia.CROP_CF_FILES
    assert ethiopia.LOCAL_AREA_UNIT_WAVES == ("2011-12", "2013-14", "2015-16",
                                              "2018-19")
    assert "2021-22" not in ethiopia.LOCAL_AREA_UNIT_WAVES


def test_w4_and_w5_are_two_names_for_one_blob(ethiopia):
    """The WB shipped no new factor file for ESPS-5 -- documented, not hidden."""
    assert ethiopia.CROP_CF_FILES["2018-19"] == "Crop_CF_Wave4.dta"
    assert ethiopia.CROP_CF_FILES["2021-22"] == "crop_cf_wave5.dta"
    doc = ethiopia.crop_conversion_factors.__doc__
    assert "national" in doc.lower()
    root = countries_root() / "Ethiopia"
    md5s = {}
    for wave, fn in (("2018-19", "Crop_CF_Wave4.dta"),
                     ("2021-22", "crop_cf_wave5.dta")):
        sidecar = root / wave / "Data" / f"{fn}.dvc"
        assert sidecar.exists(), sidecar
        md5s[wave] = [ln for ln in sidecar.read_text().splitlines()
                      if "md5" in ln][0].split(":")[-1].strip()
    assert md5s["2018-19"] == md5s["2021-22"], md5s


def test_the_duplicate_rule_is_the_wave5_row_and_keeps_434(ethiopia):
    """crop 74 x unit 62 ships twice (4.34, 6.125).  We keep 4.34."""
    rule = ethiopia.CROP_CF_DUPLICATE_RULE
    assert rule["crop_code"] == 74 and rule["unit_cd"] == 62
    # The threshold must separate the two shipped values and only those.
    assert 4.34 < rule["drop_above"] < 6.125


def test_no_crop_file_for_2011_12_raises(ethiopia):
    with pytest.raises(ValueError, match="no WB crop-conversion file"):
        ethiopia.crop_conversion_factors("2011-12")


def test_no_area_file_for_2021_22_raises(ethiopia):
    """The default is NOT to borrow -- EPAR does; a borrow would be a decision."""
    with pytest.raises(ValueError, match="ships no ET_local_area_unit"):
        ethiopia.local_area_unit_factors("2021-22")


def test_region_columns_cover_the_files_wide_layout(ethiopia):
    assert ethiopia.CROP_CF_NATIONAL_COLUMN == "mean_cf_nat"
    assert set(ethiopia.CROP_CF_REGION_COLUMNS) == {1, 2, 3, 4, 6, 7, 12, 99}
    assert all(c.startswith("mean_cf")
               for c in ethiopia.CROP_CF_REGION_COLUMNS.values())


# ---------------------------------------------------------------------------
# Loader shape and keys (needs the .dta blobs)
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
def test_national_table_is_keyed_on_t_j_u_and_is_unique(ethiopia):
    """The transform REFUSES a duplicated key, so the loader must not ship one."""
    df = _load(ethiopia)
    assert list(df.index.names) == ["t", "j", "u"]
    assert df.index.is_unique
    assert list(df.columns) == ["KgFactor", "Source"]
    assert set(df.index.names) <= set(SHIPPED_FACTOR_JOIN_LEVELS)
    assert "u" in df.index.names          # enforced by _shipped_factor_lookup
    assert (df["KgFactor"] > 0).all() and np.isfinite(df["KgFactor"]).all()
    assert set(df.index.get_level_values("t")) == set(ethiopia.CROP_CF_FILES)


@pytest.mark.requires_s3
def test_region_table_is_keyed_on_t_j_u_region_and_is_unique(ethiopia):
    df = _load(ethiopia, region=True)
    assert list(df.index.names) == ["t", "j", "u", "region"]
    assert df.index.is_unique
    assert set(df.index.get_level_values("region")) <= set(
        ethiopia.CROP_CF_REGION_COLUMNS)


@pytest.mark.requires_s3
def test_the_wave5_duplicate_is_resolved_to_434_not_averaged(ethiopia):
    """4.34 (kept), 6.125 (dropped), and NOT their mean 5.2325."""
    df = _load(ethiopia)
    enset = df.xs("2021-22", level="t")
    hit = enset[np.isclose(enset["KgFactor"].astype(float), 4.34, atol=1e-6)]
    assert len(hit) >= 1, "the kept 4.34 factor is gone"
    vals = enset["KgFactor"].astype(float)
    assert not np.isclose(vals, 6.125, atol=1e-6).any(), "6.125 was not dropped"
    assert not np.isclose(vals, (4.34 + 6.125) / 2, atol=1e-6).any(), \
        "the duplicate was AVERAGED -- the one thing GH #852 forbids"


@pytest.mark.requires_s3
def test_the_decode_collision_is_dropped_loudly_not_resolved(ethiopia):
    """KALE 56 and SPINACH 69 both decode to 'Leafy Greens' on the Esir units."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = ethiopia.crop_conversion_factors()
    msgs = [str(w.message) for w in caught if "DISAGREE on" in str(w.message)]
    assert msgs, "the dropped keys were not announced"
    assert "Leafy Greens" in msgs[0]
    assert df.index.is_unique
    # measured 2026-09-09: 1 key in 2013-14 + 3 in each later wave
    assert "DROPPED 10 key(s)" in msgs[0]
    leafy = df[df.index.get_level_values("j") == "Leafy Greens"]
    assert not leafy.index.get_level_values("u").str.startswith("Esir").any()


@pytest.mark.requires_s3
def test_the_crop_code_match_rate_is_total(ethiopia):
    """Every WB crop code in every shipped table decodes through harmonize_crop.

    Pinned because the failure is SILENT: an undecoded code becomes a NaN
    ``j``, the row is dropped, and the layer just serves fewer rows.
    """
    crop_map = ethiopia._eth_crop_label_map()
    from lsms_library.local_tools import get_dataframe
    for t, fn in ethiopia.CROP_CF_FILES.items():
        raw = get_dataframe(f"Ethiopia/{t}/Data/{fn}",
                            convert_categoricals=False)
        codes = pd.to_numeric(raw["crop_code"], errors="coerce").astype("Int64")
        unmapped = sorted(set(codes[~codes.map(crop_map).notna()].dropna()))
        assert not unmapped, f"{t}: WB crop codes with no label: {unmapped}"


@pytest.mark.requires_s3
def test_the_unit_label_match_rate_is_pinned(ethiopia):
    """Share of each wave's SECTION 9 harvest rows whose ``u`` the table covers.

    This is the CEILING on ``shipped_matched``; a silent drop to zero is the
    dominant failure mode of the shipped layer, and only a measured number
    catches it.  Measured 2026-09-09 on the raw sect9 files.
    """
    from lsms_library.local_tools import get_dataframe
    expect = {"2013-14": (0.69, "sect9_ph_w2.dta", "ph_s9q04_b"),
              "2015-16": (0.82, "sect9_ph_w3.dta", "ph_s9q04_b"),
              "2018-19": (0.81, "sect9_ph_w4.dta", "s9q05b"),
              "2021-22": (0.77, "sect9_ph_w5.dta", "s9q05b")}
    for t, (floor, h, ucol) in expect.items():
        lab = get_dataframe(f"Ethiopia/{t}/Data/{ethiopia.CROP_CF_FILES[t]}",
                            convert_categoricals=True)
        table_units = set(ethiopia._clean_unit_label(lab["unit_cd"]).dropna())
        hv = get_dataframe(f"Ethiopia/{t}/Data/{h}", convert_categoricals=True)
        hu = ethiopia._clean_unit_label(hv[ucol])
        share = float(hu.isin(table_units).mean())
        assert floor - 0.02 <= share <= floor + 0.02, (t, share)


# ---------------------------------------------------------------------------
# The area-unit loader
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
def test_area_factor_table_is_keyed_on_the_raw_geography_triple(ethiopia):
    """EPAR's padded string keys disagree across their own two sides; ours don't."""
    df = ethiopia.local_area_unit_factors()
    assert list(df.index.names) == ["t", "region", "zone", "woreda", "AreaUnit"]
    assert df.index.is_unique
    assert list(df.columns) == ["SqmPerUnit", "Source"]
    assert (df["SqmPerUnit"] > 0).all()
    # The four sidecars carry the SAME 259 rows; only Stata storage differs.
    per_wave = df.groupby(level="t").size()
    assert per_wave.nunique() == 1, per_wave.to_dict()
    assert set(df.index.get_level_values("AreaUnit")) == {
        "Timad", "Boy", "Senga", "Kert"}


@pytest.mark.requires_s3
def test_area_unit_labels_join_the_plot_features_vocabulary(ethiopia):
    """The factor table's AreaUnit must be the label plot_features emits."""
    unit_map = ethiopia._harmonize_wave_keyed("harmonize_area_unit")
    labels = {v for k, v in unit_map.items()}
    df = ethiopia.local_area_unit_factors()
    assert set(df.index.get_level_values("AreaUnit")) <= labels


# ---------------------------------------------------------------------------
# yield_kg pass-through
# ---------------------------------------------------------------------------

def _tiny_pair():
    cp = pd.DataFrame(
        {"Quantity": [10.0, 10.0]},
        index=pd.MultiIndex.from_tuples(
            [("2019-20", "h1", "h1-1-1", "Maize", "sack"),
             ("2019-20", "h1", "h1-1-2", "Maize", "sack")],
            names=["t", "i", "plot", "j", "u"]))
    pf = pd.DataFrame(
        {"Area": [1.0, 1.0]},
        index=pd.MultiIndex.from_tuples(
            [("2019-20", "h1", "1_a"), ("2019-20", "h1", "1_b")],
            names=["t", "i", "plot_id"]))
    return cp, pf


def test_yield_kg_takes_shipped_factors_and_they_change_the_answer():
    """Without the kwarg an analyst asking for yields gets NO shipped layer."""
    cp, pf = _tiny_pair()
    sf = pd.DataFrame({"KgFactor": [50.0], "Source": ["test"]},
                      index=pd.MultiIndex.from_tuples([("Maize", "sack")],
                                                      names=["j", "u"]))
    bare = yield_kg(cp, pf)
    assert bare.empty, "the unit is unconvertible, so nothing should convert"
    served = yield_kg(cp, pf, shipped_factors=sf)
    assert len(served) == 1
    # 2 plots x 10 sacks x 50 kg, over 2 x 1 ha of parcel 1
    assert float(served["Yield_kg"].iloc[0]) == pytest.approx(500.0)


def test_yield_kg_takes_min_reports():
    """The second half of the pass-through the shipped-factors ledger deferred."""
    cp, pf = _tiny_pair()
    cp = cp.assign(KgFactor=[50.0, np.nan])
    assert yield_kg(cp, pf, min_reports=99).shape[0] == 1     # median withheld
    loose = yield_kg(cp, pf, min_reports=1)
    assert float(loose["Yield_kg"].iloc[0]) == pytest.approx(500.0)


# ---------------------------------------------------------------------------
# Data-gated: the built Ethiopia tables
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
def test_ethiopia_harvest_kg_before_and_after_the_shipped_table(ethiopia):
    """Pin the measured effect of GH #852 on the built table.

    Measured 2026-09-09 on a cold isolated build.  Update these numbers in the
    same PR as any deliberate change; do not loosen the assertions.
    """
    import lsms_library as ll

    cp = ll.Country("Ethiopia").crop_production()
    assert len(cp) == 85_519
    # Ethiopia asks NO per-row conversion factor, so `reported` is
    # structurally empty and `reported_vs_shipped` can never be populated.
    assert "KgFactor" not in cp.columns

    before = harvest_kg(cp)
    after = harvest_kg(cp, shipped_factors=_load(ethiopia))
    b, a = before.attrs["kg_factor_sources"], after.attrs["kg_factor_sources"]
    assert b["shipped"] == 0 and b["shipped_matched"] == 0
    assert b["inferred"] == 26_546 and b["none"] == 58_973
    assert a["shipped"] == 62_164 and a["shipped_matched"] == 62_164
    assert a["inferred"] == 7_766 and a["none"] == 15_589
    assert a["shipped_implausible"] == 0
    assert sum(a[k] for k in ("reported", "shipped", "survey_median",
                              "inferred", "none")) == len(cp)
    # 2011-12 ships no factor file and must be UNTOUCHED.
    bt = before.groupby(level="t")["Harvest_kg"].sum()
    at = after.groupby(level="t")["Harvest_kg"].sum()
    assert float(bt["2011-12"]) == pytest.approx(float(at["2011-12"]))
    assert float(at.sum()) > 6 * float(bt.sum())


@pytest.mark.requires_s3
def test_quintal_is_exactly_100kg_on_every_newly_served_row(ethiopia):
    """The one EXTERNAL agreement the shipped table can be checked against.

    An earlier version of this test compared the shipped factor to the
    library's INFERRED one on the 18,780 rows carrying both, found a median
    ratio of 1.0000, and called that evidence the join was keyed right.  It
    is not: the red-team measured that overlap and it is **100% metric** --
    ``Kg`` 18,677 and ``Gram`` 103.  Both sides are quoting the definition of
    a kilogram, so the ratio is 1.0000 by construction, and a ``j`` mis-key
    would leave it at 1.0000 too (``Kg`` is crop-independent).  That test
    pinned a tautology.

    ``Quintal`` is the real check.  It is 13,924 of the 43,384 NEWLY-SERVED
    rows (~64% of the newly-served kilograms), the library infers **no**
    factor for it, and the WB table says exactly 100.0 for every crop in
    every wave -- which is what a quintal is.  External, not definitional.

    WHAT STILL HAS NO NUMERICAL CHECK, stated so nobody thinks it does: the
    ~29,000 local-container rows (Madaberia / Joniya / Kunna / Kerchat...),
    whose factor varies with the crop by an order of magnitude.  Their
    warrant is STRUCTURAL -- the loader decodes ``j`` and ``u`` through the
    SAME ``_eth_crop_label_map`` / ``_clean_unit_label`` that
    ``crop_production_for_wave`` uses, so a ``j`` mis-key would have to be a
    fault in a shared map, which would equally corrupt ``crop_production.j``
    itself.  That argument, not a ratio, is the reason to believe the join.
    """
    import lsms_library as ll
    from lsms_library.transformations import harvest_kg_factors

    cp = ll.Country("Ethiopia").crop_production()
    fa = harvest_kg_factors(cp, shipped_factors=_load(ethiopia))
    newly = fa["kg_shipped"].notna() & fa["kg_inferred"].isna()
    assert int(newly.sum()) == 43_384
    u = fa.index.get_level_values("u")
    quintal = newly & (u == "Quintal")
    assert int(quintal.sum()) == 13_924
    assert set(fa.loc[quintal, "kg_shipped"].astype(float)) == {100.0}
    # And the overlap really is metric-only, so the old claim stays dead.
    overlap = fa["kg_shipped"].notna() & fa["kg_inferred"].notna()
    assert set(pd.Series(u[overlap]).unique()) == {"Kg", "Gram"}


@pytest.mark.requires_s3
def test_region_true_cannot_be_passed_to_harvest_kg_today(ethiopia):
    """``region=True`` has no reachable consumer -- pin the loud refusal.

    ``crop_production`` carries no ``region`` level, so
    ``_shipped_factor_lookup`` drops that key, finds the remaining
    ``(t, j, u)`` ambiguous, and REFUSES rather than averaging.  That is the
    correct behaviour and the docstring says so; this pins it, because
    "offered" would otherwise read as "usable".
    """
    import lsms_library as ll

    cp = ll.Country("Ethiopia").crop_production()
    assert "region" not in (cp.index.names or [])
    assert "region" not in cp.columns
    with pytest.raises(ValueError, match="ambiguous on the join keys"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            harvest_kg(cp, shipped_factors=_load(ethiopia, region=True))


@pytest.mark.requires_s3
def test_plot_features_area_is_filled_from_the_woreda_table(ethiopia):
    """GH #853: 2,273 fields gain an Area; GPS rows are untouched; W5 gains none.

    EVERY row the woreda table can convert is SERVED.  Implausibility is
    counted (26 / 3 / 2 / 2 = 33 rows above 5x their own region's GPS p99)
    and reported to the build log; no row is refused and no value is clipped.
    """
    import lsms_library as ll

    pf = ll.Country("Ethiopia").plot_features()
    assert len(pf) == 132_694
    t = pf.index.get_level_values("t")
    null_by_wave = {w: int(pf.loc[t == w, "Area"].isna().sum())
                    for w in sorted(set(t))}
    assert null_by_wave == {"2011-12": 4_966, "2013-14": 1_333,
                            "2015-16": 937, "2018-19": 80, "2021-22": 138}
    # A converted row keeps the NATIVE unit name -- 'hectares' still means
    # exactly "this came from the GPS measurement" (data_info.yml: AreaUnit is
    # "original survey unit before conversion to hectares").
    converted = pf[pf["Area"].notna() & (pf["AreaUnit"] != "hectares")]
    assert len(converted) == 2_273
    assert set(converted["AreaUnit"]) == {"Timad", "Boy", "Senga", "Kert"}
    # 2021-22 has no shipped table and we do not borrow one.
    w5 = pf.loc[t == "2021-22"]
    assert not (w5["Area"].notna() & (w5["AreaUnit"] != "hectares")).any()


@pytest.mark.requires_s3
def test_area_unit_carries_the_native_label_on_unconverted_rows(ethiopia):
    """Pre-2026-09-09 this column was <NA> on 100% of non-GPS fields.

    ``harmonize_area_unit`` is wave-keyed, so ``_harmonize_wave_keyed`` returns
    ``{(wave, code): label}``; ``plot_features_for_wave`` passed that straight
    to ``_map_int_codes``, which maps a BARE code, so every lookup missed.
    ``Ethiopia/_/CONTENTS.org`` claimed the opposite and had never been right.
    """
    import lsms_library as ll

    pf = ll.Country("Ethiopia").plot_features()
    unconverted = pf[pf["Area"].isna()]
    assert len(unconverted) == 7_454
    # 541 residual <NA>: fields with no GPS area whose farmer-estimate unit
    # CODE is itself missing or outside harmonize_area_unit (2011-12 268,
    # 2013-14 73, 2015-16 197, 2018-19 1, 2021-22 2).  4.1% of the affected
    # population -- the fix reaches the label, not a missing code.
    assert int(unconverted["AreaUnit"].isna().sum()) == 541
    assert {"Timad", "Other", "Square Meters", "Hectare"} <= set(
        unconverted["AreaUnit"].dropna())
