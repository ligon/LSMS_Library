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

#: The EXTERNAL answer key for Ethiopia's quintal, stated here as a literal and
#: read from nowhere in the library (GH #931, @ligon 2026-10-18).  The quintal
#: Ethiopia uses is the metric quintal (the centner): 100 kg.  Sources:
#:
#: * the unit's definition -- the metric quintal is 100 kg, and it is the unit
#:   in which Ethiopian grain production and grain prices are quoted
#:   (https://en.wikipedia.org/wiki/Quintal, metric quintal and its present
#:   use in Ethiopia; contrast the Spanish/Central American quintal of 100 lb,
#:   the reason GH #919 moved the quintal out of KNOWN_METRIC);
#: * the survey's own published report, independent of both the WB factor
#:   file and our ``u.org``: CSA & World Bank, *Ethiopia Socioeconomic Survey
#:   (ESS) 2015/2016, Survey Report* (``Ethiopia/2015-16/Documentation/
#:   9331_ethiopia_survey_report_web_final.pdf``, the self-reported-yield
#:   paragraph beside Table 4.6), which states each yield in both units:
#:   "wheat yields were 1,403 kilograms or 14 quintals per hectare, sorghum
#:   yields were 833 kilograms or 8.4 quintals ..., barley yields were 1,012
#:   kilograms or 10.1 quintals ..., and teff yields were 650 kilograms or 6.5
#:   quintals" -- 100.2 / 99.2 / 100.2 / 100.0 kg per quintal to the report's
#:   own rounding (maize, "1,760 kilograms or 17.8 quintals", is 98.9).
QUINTAL_KG = 100.0


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
    # 2026-09-18, GH #832: 2015-16 moves 0.820 -> 0.8435 (measured 0.843513).
    # That wave's sect9 spelled three units "Zorba/Akara <mojibake> {Large,
    # Medium, Small}" -- the double-encoded U+FFFD -- while its OWN conversion
    # table spells them cleanly under codes 191/192/193, so 550 harvest rows
    # could not match a factor that was sitting right there.
    # `ethiopia._clean_unit_label` now strips that sequence, which RAISES the
    # ceiling this test pins.  The other three waves are unchanged (they never
    # carried the damaged spelling): 0.692744 / 0.819809 / 0.776213.
    expect = {"2013-14": (0.69, "sect9_ph_w2.dta", "ph_s9q04_b"),
              "2015-16": (0.84, "sect9_ph_w3.dta", "ph_s9q04_b"),
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

    2026-10-19, GH #931 -- NUMBERS MOVED, deliberately (@ligon ruling
    2026-10-18: ``harvest_kg_factors`` reads the declared ``u_kg``).  The
    library now holds an opinion on ``Quintal`` -- 100 kg, from the global
    ``u_kg`` table, stamped on this frame as ``attrs['unit_kg']`` -- so the
    13,977 Quintal rows leave ``none`` for ``metric`` in the ``before`` split
    (``metric`` 26,548 -> 40,525, ``none`` 58,971 -> 44,994, ``declared``
    13,977).  In the ``after`` split ``shipped`` still OUTRANKS ``metric``, so
    13,924 of them stay ``shipped`` at the WB's 100.0; the other 53 -- Quintal
    rows whose (crop, unit) cell the WB table lacks, 31 in 2018-19 and 22 in
    2021-22 -- move ``none`` -> ``metric`` (``metric`` 7,768 -> 7,821,
    ``none`` 15,046 -> 14,993).  Totals: ``before`` 1,905,712.51 ->
    8,799,669.51 kg, ``after`` 12,685,707.32 -> 12,696,657.32 kg (+10,950,
    the 53 rows), so the after/before ratio falls 6.657 -> 1.443: most of what
    the shipped table used to ADD was the quintal, which the library now
    converts on its own.  The old ``> 6 *`` assertion is replaced by the
    pinned totals and ratio, not loosened.  2011-12 carries NO Quintal row, so
    W1 is untouched in both splits.  And with the declaration switched off
    (``unit_kg={}``) every pre-#931 number below is reproduced exactly --
    pinned, so the move is provably the declared layer and nothing else.

    2026-09-14, GH #919: the ``before`` split moves by ONE row AGAIN,
    26,547 -> 26,548 ``inferred`` and 58,972 -> 58,971 ``none`` -- the same
    mechanism as the #850 note below, for the same reason.  ``f7c1eb380``
    seeded ``centilitre`` into ``KNOWN_METRIC``, and Ethiopia's
    ``crop_production`` carries exactly one ``Centilitres`` row, which stopped
    falling through to ``none``.  That half of ``f7c1eb380`` is correct and
    kept: a centilitre is a centilitre everywhere.

    The OTHER half of that commit seeded ``quintal``, which is NOT universal --
    the metric centner (100 kg) in Ethiopia, 100 *pounds* (45.36 kg) in Central
    America -- and it moved 13,977 rows off the ``shipped`` rung onto
    ``metric``, taking with them the one non-tautological external check this
    file had (then ``test_quintal_is_exactly_100kg_on_every_newly_served_row``,
    re-expressed under GH #931 as
    ``test_shipped_quintal_matches_the_external_answer_key``; see
    ``Ethiopia/_/CONTENTS.org``).  ``quintal`` now lives in
    ``categorical_mapping/u.org``'s ``u_kg`` table, where a country can
    override it; Ethiopia inherits 100 kg from the global default, the shipped
    rung reclaims its rows, and the newly-served count returns to 43,384.

    2026-09-15, GH #929 split the crop ladder's ``metric`` layer out of
    ``inferred``, mirroring the food ladder since GH #850.  The counts below
    move from ``inferred`` to ``metric`` WITHOUT changing: measured across
    both splits, 0 of 85,519 ``kg_per_unit`` values differ, the NaN pattern is
    identical, and ``Harvest_kg`` sums to the same 12,680,617.283538.  The
    seeds were always inside the inference with precedence; only the label
    they are counted under is new.  ``inferred`` is 0 in BOTH splits -- the
    price-ratio inference contributes nothing to this country at all, which
    was invisible while the two layers shared a bucket.

    That this reads 26,548 rather than the 40,525 an earlier draft of #929
    measured is GH #919: that draft was written on a base without it, while
    ``quintal`` was still seeded into ``KNOWN_METRIC``.  The ratio assertion
    below is left at ``> 6 *`` for the same reason -- #919 returned the
    un-shipped total to 1,905,712 (ratio 6.654), so it holds as written.

    2026-09-10, GH #850 defect (c): the ``before`` split moved by ONE row,
    26,546 -> 26,547 ``inferred`` and 58,973 -> 58,972 ``none``.  That branch
    taught ``_parse_explicit_metric`` the plural / abbreviated metric
    spellings (``50 kgs``, ``20 lts``), which ``_kg_factor_series`` reads, so
    a single Ethiopian crop row carrying such a label stopped falling through
    to ``none``.  Uganda moves 58,831 rows the same way and Niger 5; every
    other country is 0.  The same one row moves in the ``after`` split (7,766
    -> 7,767 ``inferred``, 15,589 -> 15,588 ``none``): the shipped layer
    outranks ``inferred`` but does not cover this row, so nothing shields it.
    ``shipped`` and ``shipped_matched`` are unchanged at 62,164.
    """
    import lsms_library as ll

    cp = ll.Country("Ethiopia").crop_production()
    assert len(cp) == 85_519
    # Ethiopia asks NO per-row conversion factor, so `reported` is
    # structurally empty and `reported_vs_shipped` can never be populated.
    assert "KgFactor" not in cp.columns
    # GH #931: the declaration the crop ladder now reads by default.
    assert cp.attrs["unit_kg"] == {"Ethiopia": {"quintal": QUINTAL_KG}}

    shipped = _load(ethiopia)
    before = harvest_kg(cp)
    after = harvest_kg(cp, shipped_factors=shipped)
    b, a = before.attrs["kg_factor_sources"], after.attrs["kg_factor_sources"]
    assert before.attrs["kg_factor_declared_from"] == "attrs"
    assert b["shipped"] == 0 and b["shipped_matched"] == 0
    assert b["metric"] == 40_525 and b["none"] == 44_994
    assert b["declared"] == a["declared"] == 13_977
    # 2026-09-18, GH #832: `shipped` / `shipped_matched` 62,164 -> 62,705 and
    # `none` 15,587 -> 15,046, both by the SAME 541 rows.  Those are 2015-16
    # Zorba/Akara harvest rows whose unit label carried the double-encoded
    # U+FFFD mojibake; `_clean_unit_label` now strips it, so they join the
    # shipped table instead of falling through to `none`.  (550 rows carry the
    # damaged label; 9 of them have no (crop, unit) cell in the WB table and
    # still land on `none`.)  `metric` and the whole `before` split do not
    # move -- nothing about the metric seeds changed.  Harvest_kg total
    # 12,680,617.28 -> 12,685,707.32; the ratio asserted below rises
    # 6.654 -> 6.657, so `> 6 *` still holds as written.
    assert a["shipped"] == 62_705 and a["shipped_matched"] == 62_705
    assert a["metric"] == 7_821 and a["none"] == 14_993
    # Ethiopia's crop units are ALL read off a label, never guessed.  Pinned
    # as its own assertion rather than folded into the counts above: if the
    # price-ratio inference ever starts serving this country, that is a fact
    # about Ethiopia worth failing over, not a number to re-baseline.
    assert b["inferred"] == 0 and a["inferred"] == 0
    assert a["shipped_implausible"] == 0
    assert sum(a[k] for k in ("reported", "shipped", "survey_median",
                              "metric", "inferred", "none")) == len(cp)
    # 2011-12 ships no factor file and must be UNTOUCHED -- and, carrying no
    # Quintal row, it is untouched by GH #931 as well.
    bt = before.groupby(level="t")["Harvest_kg"].sum()
    at = after.groupby(level="t")["Harvest_kg"].sum()
    assert float(bt["2011-12"]) == pytest.approx(float(at["2011-12"]))
    assert float(bt["2011-12"]) == pytest.approx(653_661.0)
    assert float(bt.sum()) == pytest.approx(8_799_669.50962, rel=1e-9)
    assert float(at.sum()) == pytest.approx(12_696_657.323532, rel=1e-9)
    assert float(at.sum()) / float(bt.sum()) == pytest.approx(1.4429, abs=1e-4)

    # The declaration switched OFF reproduces every pre-#931 number, so the
    # move above is the declared layer and nothing else.
    blind_b = harvest_kg(cp, unit_kg={})
    blind_a = harvest_kg(cp, shipped_factors=shipped, unit_kg={})
    ob, oa = (blind_b.attrs["kg_factor_sources"],
              blind_a.attrs["kg_factor_sources"])
    assert ob["metric"] == 26_548 and ob["none"] == 58_971
    assert oa["metric"] == 7_768 and oa["none"] == 15_046
    assert ob["declared"] == oa["declared"] == 0
    assert float(blind_b["Harvest_kg"].sum()) == pytest.approx(
        1_905_712.50962, rel=1e-9)
    assert float(blind_a["Harvest_kg"].sum()) == pytest.approx(
        12_685_707.323532, rel=1e-9)
    assert float(blind_a["Harvest_kg"].sum()) > 6 * float(
        blind_b["Harvest_kg"].sum())


def test_the_declared_quintal_matches_the_external_answer_key():
    """Our OWN declaration, checked against the same cited key -- config only.

    Since GH #931 the crop ladder reads ``u_kg``, so the library's quintal is
    served on every Ethiopian Quintal row the WB table does not cover.  It is
    checked against :data:`QUINTAL_KG`, never against the WB table, so a wrong
    edit to ``u.org`` fails here and cannot hide behind agreement with it.
    """
    import lsms_library as ll

    assert ll.Country("Ethiopia")._unit_kg_factors()["quintal"] == QUINTAL_KG


@pytest.mark.requires_s3
def test_shipped_quintal_matches_the_external_answer_key(ethiopia):
    """The WB table's quintal, checked against an answer key NEITHER side supplies.

    History.  An earlier version of this test compared the shipped factor to
    the library's INFERRED one on the 18,780 rows carrying both, found a
    median ratio of 1.0000, and called that evidence the join was keyed
    right.  It is not: that overlap was **100% metric** -- ``Kg`` 18,677 and
    ``Gram`` 103 -- so both sides quoted the definition of a kilogram, and a
    ``j`` mis-key would leave it at 1.0000 too.  It was replaced by
    "``Quintal`` == 100.0 on the rows the library has NO opinion about" --
    external because the library then served the quintal ``none``.

    GH #931 (@ligon, 2026-10-18) ended that: the crop ladder reads the
    declared ``u_kg``, so the library now holds 100 kg for a quintal, the
    Quintal rows leave the "no opinion" set (43,925 -> 30,001), and the
    overlap becomes Kg / Gram / Quintal.  Comparing the WB table with the
    library there would compare two numbers that both say "a quintal is
    100 kg" and could not tell a wrong one from a right one.

    So the check is now made against :data:`QUINTAL_KG` -- a literal in this
    file, with its sources cited above it -- and the TWO parties are each
    checked against it, never against each other: the WB shipped factor here,
    and our own declaration in
    :func:`test_the_declared_quintal_matches_the_external_answer_key`.  The
    shipped table still SERVES those rows (``shipped`` outranks ``metric``),
    so a wrong WB factor would reach the user, and this fails on it.

    WHAT STILL HAS NO NUMERICAL CHECK, unchanged: the ~30,000
    local-container rows (Madaberia / Joniya / Kunna / Kerchat...), whose
    factor varies with the crop by an order of magnitude.  Their warrant is
    STRUCTURAL -- the loader decodes ``j`` and ``u`` through the SAME
    ``_eth_crop_label_map`` / ``_clean_unit_label`` that
    ``crop_production_for_wave`` uses.  Nor does the quintal check a ``j``
    key: it is 100 kg for every crop.
    """
    import lsms_library as ll
    from lsms_library.transformations import harvest_kg_factors

    cp = ll.Country("Ethiopia").crop_production()
    fa = harvest_kg_factors(cp, shipped_factors=_load(ethiopia))
    u = fa.index.get_level_values("u")
    t = fa.index.get_level_values("t")
    quintal = u == "Quintal"
    assert int(quintal.sum()) == 13_977

    # (a) the WB table, against the key: every Quintal row it serves.
    shipped_q = quintal & fa["kg_shipped"].notna()
    assert int(shipped_q.sum()) == 13_924
    assert set(fa.loc[shipped_q, "kg_shipped"].astype(float)) == {QUINTAL_KG}
    assert set(fa.loc[shipped_q, "KgFactorSource"]) == {"shipped"}
    assert set(fa.loc[shipped_q, "kg_per_unit"].astype(float)) == {QUINTAL_KG}

    # (b) the 53 Quintal rows the WB table does NOT cover are served by our
    # declaration -- which (c) is the key's value on every Quintal row.
    unshipped_q = quintal & fa["kg_shipped"].isna()
    assert pd.Series(t[unshipped_q]).value_counts().to_dict() == {
        "2018-19": 31, "2021-22": 22}
    assert set(fa.loc[unshipped_q, "KgFactorSource"]) == {"metric"}
    assert set(fa.loc[quintal, "kg_declared"].astype(float)) == {QUINTAL_KG}

    # The "no library opinion" set the old check used, re-measured: the
    # quintal has left it, which is exactly why it is no longer the check.
    newly = (fa["kg_shipped"].notna() & fa["kg_metric"].isna()
             & fa["kg_inferred"].isna())
    assert int(newly.sum()) == 30_001
    assert "Quintal" not in set(u[newly])
    overlap = fa["kg_shipped"].notna() & fa["kg_inferred"].notna()
    assert set(pd.Series(u[overlap]).unique()) == {"Kg", "Gram", "Quintal"}


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
