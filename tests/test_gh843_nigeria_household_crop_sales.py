"""Nigeria ``crop_production``: W3-W5 household-crop sales (GH #843).

From W3 the GHS records the crop sale in A3.ii (``secta3ii``), per household and
crop, with no plot, and the build used to leave ``Quantity_sold`` / ``Value_sold``
empty for three of five rounds.  Ruling (@ligon, 2026-11-03, option d):

* exactly one served harvest row for that household and crop -> the sale goes on
  that row, in its own ``Unit_sold``;
* otherwise (two or more rows, or none) -> ONE ``plot_id='Farm-level'`` row with
  no harvest ``Quantity``.

No invented split, no double count, nothing dropped.

Two kinds of test, and they fail differently:

1. **The rules, on synthetic frames** (data-free).  These load ``nigeria.py`` by
   path and exercise branches the real files reach rarely or never.
2. **The oracle** (data-gated).  ``TestOracle`` reads the source files directly
   and imports NOTHING from ``Nigeria/_/``, so it cannot inherit a bug from the
   code it checks.  It needs a warm Nigeria ``crop_production`` parquet and never
   cold-builds one.
"""
from __future__ import annotations

import importlib.util
import json
import sys
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root

NIGERIA = countries_root() / "Nigeria"
TABLE = "crop_production"
FARM_LEVEL = "Farm-level"
SINGLE_ROW = "Nigeria::crop_production::hh-crop-sale-single-row"
SUMMED = "Nigeria::crop_production::hh-crop-sale-records-summed"
TWO_UNITS = "Nigeria::crop_production::hh-crop-sale-two-units"

#: The sale block as the questionnaires print it, per PH round.  W3 asks the
#: sale its own unit.  W4/W5 ask only a quantity "Of the [QTY] [UNIT] [CONDITION]
#: of [CROP] harvested", so the unit, size and condition are the total harvest's.
SALE_BLOCK = {
    "2016Q1": dict(wave="2015-16", file="Data/secta3ii_harvestw3.dta",
                   qty="sa3iiq5a", value="sa3iiq6", unit=["sa3iiq5b"]),
    "2019Q1": dict(wave="2018-19", file="Data/secta3ii_harvestw4.dta",
                   qty="sa3iiq5a", value="sa3iiq6",
                   unit=["sa3iiq1c", "sa3iiq1d", "sa3iiq1b"]),
    "2024Q1": dict(wave="2023-24",
                   file="Data/Post Harvest Wave 5/Agriculture/secta3ii_harvestw5.dta",
                   qty="sa3iiq6", value="sa3iiq7",
                   unit=["sa3iiq3b", "sa3iiq3c", "sa3iiq3d"]),
}
HARVEST_FILES = {
    "2016Q1": ["Data/secta3i_harvestw3.dta"],
    "2019Q1": ["Data/secta3i_harvestw4.dta", "Data/secta3iii_harvestw4.dta"],
    "2024Q1": ["Data/Post Harvest Wave 5/Agriculture/secta3i_harvestw5.dta",
               "Data/Post Harvest Wave 5/Agriculture/secta3iii_harvestw5.dta"],
}
#: W1/W2 record the sale ON the plot-crop row, and must not move.
PLOT_GRAIN_SALES = {
    "2011Q1": dict(wave="2010-11",
                   file="Data/Post Harvest Wave 1/Agriculture/secta3_harvestw1.dta",
                   crop="sa3q2"),
    "2013Q1": dict(wave="2012-13",
                   file="Data/Post Harvest Wave 2/Agriculture/secta3_harvestw2.dta",
                   crop="cropcode"),
}


# --------------------------------------------------------------------------
# 1. the rules, on synthetic frames (data-free)
# --------------------------------------------------------------------------

def _load_nigeria_module():
    """Import ``Nigeria/_/nigeria.py`` by path (it is not a package)."""
    spec = importlib.util.spec_from_file_location("nigeria", NIGERIA / "_" / "nigeria.py")
    mod = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("nigeria", mod)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def ng():
    return _load_nigeria_module()


SPEC = dict(fn="synthetic", sold="sold", qty="q", unit="unit", size="size",
            condition="cond", value="v")
LABELS = {1010: "Maize", 1121: "Yam--roots", 1123: "Yam--roots"}


def _sale_file(rows):
    """``(raw, dec)`` for one synthetic A3.ii file.  ``rows`` are tuples
    ``(hhid, code, q, v, unit code, size, condition)``."""
    raw = pd.DataFrame(rows, columns=["hhid", "cropcode", "q", "v", "unit", "size", "cond"])
    raw["sold"] = 1
    dec = raw.copy()
    dec["cropcode"] = raw["cropcode"].map(
        lambda c: f"{c}. YELLOW PEPPER" if c not in LABELS else f"{c}. A CROP")
    dec["unit"] = raw["unit"].map(lambda c: {1: "1. KILOGRAMS", 130: "130. SACK/BAG",
                                             211: "211. TUBER"}.get(c))
    return raw, dec


def _harvest_frame(rows):
    """One synthetic plot-crop frame.  ``rows`` are ``(hhid, plot, code, qty)``."""
    df = pd.DataFrame(rows, columns=["hhid", "plotid", "cropcode", "qty"])
    df["unit"] = 1
    dec = df.copy()
    dec["unit"] = "1. KILOGRAMS"
    return dict(df=df, dec=dec, hhid="hhid", plot="plotid", crop="cropcode",
                qty="qty", unit="unit", perennial=False)


def _build(ng, harvest, sales):
    raw, dec = _sale_file(sales)
    records = ng._hh_crop_sale_frame(raw, dec, SPEC, LABELS)
    return ng.crop_production_for_wave(
        "2016Q1", [_harvest_frame(harvest)], LABELS, hh_sales=records)


class TestWhatIsASale:
    def test_only_a_positive_quantity_or_value_is_a_sale(self, ng):
        raw, dec = _sale_file([
            ("h1", 1010, 5.0, 100.0, 1, None, None),      # sale
            ("h2", 1010, np.nan, np.nan, np.nan, None, None),  # did not sell
            ("h3", 1010, 0.0, 0.0, 1, None, None),        # zero and zero
            ("h4", 1010, 5.0, 0.0, 1, None, None),        # recorded zero value
            ("h5", 1010, 5.0, np.nan, 1, None, None),     # quantity, no value
            ("h6", 1010, np.nan, 100.0, np.nan, None, None),   # value, no quantity
        ])
        got = ng._hh_crop_sale_frame(raw, dec, SPEC, LABELS).set_index("i")
        assert sorted(got.index) == ["h1", "h4", "h5", "h6"]
        assert got.loc["h4", "Value_sold"] == 0.0, "a recorded zero stays a zero"
        assert pd.isna(got.loc["h5", "Value_sold"]), "a missing value is not a zero"
        assert pd.isna(got.loc["h6", "Quantity_sold"])

    def test_a_non_sale_fabricates_no_zero_and_no_row(self, ng):
        out = _build(ng, [("h2", "1", 1010, 9.0), ("h1", "1", 1010, 9.0)],
                     [("h1", 1010, 5.0, 100.0, 1, None, None),
                      ("h2", 1010, np.nan, np.nan, np.nan, None, None)])
        assert len(out) == 2, "a did-not-sell record adds no row"
        row = out.loc[("2016Q1", "h2", "1", "Maize")]
        assert pd.isna(row["Quantity_sold"]) and pd.isna(row["Value_sold"])
        assert pd.isna(row["Unit_sold"])


class TestPlacement:
    def test_one_served_row_takes_the_sale_in_its_own_unit(self, ng):
        out = _build(ng, [("h1", "1", 1010, 9.0)], [("h1", 1010, 2.0, 100.0, 130, None, None)])
        assert len(out) == 1
        row = out.loc[("2016Q1", "h1", "1", "Maize")]
        assert row["Quantity_sold"] == 2.0 and row["Value_sold"] == 100.0
        assert row["Unit_sold"] == "Sack/Bag" and row["u"] == "Kg"
        assert row["Derivation"] == SINGLE_ROW

    def test_the_count_is_of_rows_not_of_rows_with_a_harvest(self, ng):
        """The Niger rule: a row with a blank Quantity still counts, both ways."""
        one_blank = _build(ng, [("h1", "1", 1010, np.nan)],
                           [("h1", 1010, 2.0, 100.0, 1, None, None)])
        assert one_blank.loc[("2016Q1", "h1", "1", "Maize"), "Value_sold"] == 100.0
        two_one_blank = _build(ng, [("h1", "1", 1010, 9.0), ("h1", "2", 1010, np.nan)],
                               [("h1", 1010, 2.0, 100.0, 1, None, None)])
        assert two_one_blank.loc[("2016Q1", "h1", FARM_LEVEL, "Maize"), "Value_sold"] == 100.0
        assert two_one_blank.loc[("2016Q1", "h1", "1", "Maize"), "Value_sold"] != 100.0

    @pytest.mark.parametrize("harvest", [
        [("h1", "1", 1010, 9.0), ("h1", "2", 1010, 4.0)],   # several rows
        [("h9", "1", 1010, 9.0)],                           # no row for h1
    ], ids=["several-rows", "no-harvest-row"])
    def test_otherwise_one_farm_level_row(self, ng, harvest):
        out = _build(ng, harvest, [("h1", 1010, 2.0, 100.0, 1, None, None)])
        farm = out.loc[("2016Q1", "h1", FARM_LEVEL, "Maize")]
        assert farm["Quantity_sold"] == 2.0 and farm["Value_sold"] == 100.0
        assert farm["Unit_sold"] == "Kg"
        # A survey answer at its own grain: the index discloses it, no key.
        assert pd.isna(farm["Derivation"])
        assert pd.isna(farm["Quantity"]) and farm["u"] == "Unknown"
        for col in ("KgFactor", "Value_harvested", "planting_month",
                    "harvest_month", "intercropped", "perennial"):
            assert pd.isna(farm[col]), col
        harvested = out.drop(index=("2016Q1", "h1", FARM_LEVEL, "Maize"))
        assert harvested["Value_sold"].isna().all(), "the sale is on one row only"

    def test_a_row_discarded_by_the_dedup_is_not_counted_and_carries_nothing(self, ng):
        """GH #1433 interaction.  Two raw lines of one (plot, crop) are one served
        row, so the sale attaches to it, once, and the discarded twin is gone."""
        out = _build(ng, [("h1", "1", 1121, 9.0), ("h1", "1", 1123, 4.0)],
                     [("h1", 1121, 2.0, 100.0, 1, None, None)])
        assert len(out) == 1, "no Farm-level row: one served row"
        row = out.loc[("2016Q1", "h1", "1", "Yam--roots")]
        assert row["Quantity"] == 9.0 and row["Value_sold"] == 100.0

    def test_no_sale_means_no_derivation_and_w1_w2_shape_is_unchanged(self, ng):
        out = ng.crop_production_for_wave(
            "2011Q1", [_harvest_frame([("h1", "1", 1010, 9.0)])], LABELS)
        assert out["Derivation"].isna().all()
        assert str(out["Derivation"].dtype).startswith("string")
        assert FARM_LEVEL not in out.index.get_level_values("plot_id")


class TestSeveralRecordsOfOneCrop:
    """Two codes one label serves, or a repeated roster line.  One index key."""

    def test_same_unit_sums_and_is_keyed(self, ng):
        out = _build(ng, [("h1", "1", 1121, 9.0)],
                     [("h1", 1121, 2.0, 100.0, 130, 12, 2),
                      ("h1", 1123, 3.0, 50.0, 130, 12, 2)])
        row = out.loc[("2016Q1", "h1", "1", "Yam--roots")]
        assert row["Quantity_sold"] == 5.0 and row["Value_sold"] == 150.0
        assert row["Unit_sold"] == "Sack/Bag"
        assert row["Derivation"] == "+".join(sorted([SUMMED, SINGLE_ROW]))

    @pytest.mark.parametrize("second", [
        (3.0, 50.0, 211, 12, 2),     # different unit
        (3.0, 50.0, 130, 11, 2),     # same unit, different size: 'Sack/Bag' both
        (3.0, 50.0, 130, 12, 1),     # same unit and size, different condition
    ], ids=["unit", "size", "condition"])
    def test_two_units_sum_value_only(self, ng, second):
        out = _build(ng, [("h1", "1", 1121, 9.0), ("h1", "2", 1121, 9.0)],
                     [("h1", 1121, 2.0, 100.0, 130, 12, 2), ("h1", 1123, *second)])
        row = out.loc[("2016Q1", "h1", FARM_LEVEL, "Yam--roots")]
        assert row["Value_sold"] == 150.0
        assert pd.isna(row["Quantity_sold"]), "no sum across units"
        assert pd.isna(row["Unit_sold"])
        assert row["Derivation"] == TWO_UNITS

    def test_farm_level_sum_carries_the_sum_key_alone(self, ng):
        out = _build(ng, [("h1", "1", 1121, 9.0), ("h1", "2", 1121, 9.0)],
                     [("h1", 1121, 2.0, 100.0, 130, 12, 2),
                      ("h1", 1123, 3.0, 50.0, 130, 12, 2)])
        assert out.loc[("2016Q1", "h1", FARM_LEVEL, "Yam--roots"), "Derivation"] == SUMMED


def test_an_unmapped_crop_code_keeps_its_own_label(ng):
    """W4 codes 9001-9012: base drops the harvest row, the sale is not dropped."""
    out = _build(ng, [("h9", "1", 1010, 9.0)], [("h1", 9006, 2.0, 100.0, 1, None, None)])
    assert out.loc[("2016Q1", "h1", FARM_LEVEL, "YELLOW PEPPER"), "Value_sold"] == 100.0


def test_country_script_passes_the_sale_block_for_three_waves():
    """Read, never imported: the script runs data reads at module level."""
    source = (NIGERIA / "_" / "crop_production.py").read_text()
    for wave in ("2015-16", "2018-19", "2023-24"):
        assert f"hh_crop_sale_records('{wave}', crop_labels)" in source, wave
    assert source.count("hh_crop_sale_records(") == 3


# --------------------------------------------------------------------------
# 2. the oracle (data-gated): reads the source, imports nothing from Nigeria/_/
# --------------------------------------------------------------------------

def _hh(x):
    from lsms_library.local_tools import format_id
    return format_id(x)


def _walk(t, ids):
    """Single-pass rename through the committed crosswalk, as the read path does."""
    mapping = json.loads((NIGERIA / "_" / "updated_ids.json").read_text()).get(t, {})
    return ids.map(lambda x: mapping.get(x, x))


def _crop_labels():
    from lsms_library.local_tools import df_from_orgfile
    table = df_from_orgfile(str(NIGERIA / "_" / "categorical_mapping.org"),
                            name="harmonize_food")
    out = {}
    for code, label in zip(table["Code"], table["Preferred Label"]):
        code = pd.to_numeric(code, errors="coerce")
        if pd.notna(code) and pd.notna(label) and str(label).strip() not in ("", "---"):
            out[int(code)] = str(label).strip()
    return out


def _source_sales(t):
    """One row per A3.ii record that records a sale, from the file alone."""
    from lsms_library.local_tools import get_dataframe
    cfg = SALE_BLOCK[t]
    fn = str(NIGERIA / cfg["wave"] / cfg["file"])
    raw = get_dataframe(fn, convert_categoricals=False)
    dec = get_dataframe(fn, convert_categoricals=True)
    q = pd.to_numeric(raw[cfg["qty"]], errors="coerce")
    v = pd.to_numeric(raw[cfg["value"]], errors="coerce")
    code = pd.to_numeric(raw["cropcode"], errors="coerce").astype("Int64")
    named = dec["cropcode"].astype("string").str.replace(r"^\s*\d+\.\s*", "", regex=True)
    crop = code.map(_crop_labels()).astype("string").fillna(named)
    unit = raw[cfg["unit"]].astype("string").fillna("").agg("/".join, axis=1)
    out = pd.DataFrame({
        "i": raw["hhid"].map(_hh).astype("string").values,
        "crop": crop.values, "q": q.values, "v": v.values, "unit": unit.values,
        "unit_label": dec[cfg["unit"][0]].astype("string").values,
    })
    return out[((q > 0) | (v > 0)).values].assign(i=lambda d: _walk(t, d["i"]).values)


def _source_harvest_rows(t):
    """Distinct ``(i, plot_id, crop)`` of the raw plot-crop files, labelled crops."""
    from lsms_library.local_tools import get_dataframe
    labels = _crop_labels()
    parts = []
    for rel in HARVEST_FILES[t]:
        d = get_dataframe(str(NIGERIA / SALE_BLOCK[t]["wave"] / rel), convert_categoricals=False)
        code = pd.to_numeric(d["cropcode"], errors="coerce").astype("Int64")
        parts.append(pd.DataFrame({
            "i": d["hhid"].map(_hh).astype("string").values,
            "plot_id": d["plotid"].map(_hh).astype("string").values,
            "crop": code.map(labels).astype("string").values,
        }))
    rows = pd.concat(parts, ignore_index=True).dropna(subset=["i", "crop"]).drop_duplicates()
    return rows.assign(i=lambda d: _walk(t, d["i"]).values)


@pytest.fixture(scope="module")
def served():
    """The API frame, flat, from a warm parquet.  Never cold-builds."""
    from lsms_library.local_tools import data_root, read_parquet_cache_waves
    path = data_root() / "Nigeria" / "var" / f"{TABLE}.parquet"
    try:
        cached = pd.read_parquet(path)
    except FileNotFoundError:
        pytest.skip("no warm Nigeria crop_production parquet")
    if "Derivation" not in cached.columns:
        pytest.skip("warm Nigeria crop_production parquet predates GH #843 (re-warm needed)")
    coverage = read_parquet_cache_waves(path)
    wanted = set(SALE_BLOCK) | set(PLOT_GRAIN_SALES)
    if coverage is not None and wanted - set(coverage):
        pytest.skip(f"partial warm cache: lacks {sorted(wanted - set(coverage))}")
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = ll.Country("Nigeria", assume_cache_fresh=True).crop_production()
    assert df.index.is_unique
    return df.reset_index()


def _has_sale(df):
    return df["Value_sold"].notna() | df["Quantity_sold"].notna()


@pytest.mark.requires_s3
@pytest.mark.parametrize("t", sorted(SALE_BLOCK))
class TestOracle:
    def test_value_is_conserved(self, served, t):
        got = served.loc[served["t"] == t, "Value_sold"].sum()
        assert got == pytest.approx(_source_sales(t)["v"].sum(), abs=1e-6)

    def test_every_household_crop_with_a_sale_is_on_exactly_one_row(self, served, t):
        src = _source_sales(t)
        rows = served[(served["t"] == t) & _has_sale(served)]
        assert not rows.duplicated(["i", "crop"]).any(), "a sale on two rows"
        want = set(map(tuple, src[["i", "crop"]].drop_duplicates().values))
        got = set(map(tuple, rows[["i", "crop"]].astype(str).values))
        assert got == want

    def test_attachment_only_to_a_single_served_row(self, served, t):
        src = _source_sales(t)[["i", "crop"]].drop_duplicates()
        harvest = _source_harvest_rows(t)
        n_rows = harvest.groupby(["i", "crop"]).size()
        sold = served[(served["t"] == t) & _has_sale(served)].set_index(["i", "crop"])["plot_id"]
        expected_one = 0
        for pair, plot in sold.items():
            if n_rows.get(pair, 0) == 1:
                expected_one += 1
                only = harvest[(harvest["i"] == pair[0]) & (harvest["crop"] == pair[1])]
                assert plot == only["plot_id"].iloc[0], pair
            else:
                assert plot == FARM_LEVEL, pair
        assert expected_one > 0 and len(sold) == len(src)

    def test_farm_level_rows_state_the_sale_and_nothing_else(self, served, t):
        farm = served[(served["t"] == t) & (served["plot_id"] == FARM_LEVEL)]
        assert len(farm) > 0
        assert _has_sale(farm).all(), "a Farm-level row exists only to carry a sale"
        assert farm.duplicated(["i", "crop"]).sum() == 0
        for col in ("Quantity", "KgFactor", "Value_harvested", "planting_month",
                    "harvest_month", "intercropped", "perennial"):
            assert farm[col].isna().all(), col
        assert (farm["u"] == "Unknown").all()

    def test_quantity_row_by_row_and_never_across_units(self, served, t):
        src = _source_sales(t)
        grouped = src.groupby(["i", "crop"])
        stated = src[src["q"] > 0].groupby(["i", "crop"])
        expect = pd.DataFrame({"q": grouped["q"].sum(min_count=1),
                               "units": stated["unit"].nunique()})
        expect["units"] = expect["units"].fillna(0)
        expect.loc[expect["units"] > 1, "q"] = np.nan
        rows = served[(served["t"] == t) & _has_sale(served)].copy()
        rows["i"] = rows["i"].astype(str)
        rows["crop"] = rows["crop"].astype(str)
        got = rows.set_index(["i", "crop"])
        expect.index = expect.index.set_levels(
            [lvl.astype(str) for lvl in expect.index.levels])
        joined = expect.join(pd.to_numeric(got["Quantity_sold"], errors="coerce").astype("float64"))
        assert (joined["q"].isna() == joined["Quantity_sold"].isna()).all()
        assert np.allclose(joined["q"].dropna(), joined["Quantity_sold"].dropna())
        two_units = got.loc[expect[expect["units"] > 1].index]
        assert two_units["Unit_sold"].isna().all()

    def test_served_unit_is_a_function_of_the_source_unit(self, served, t):
        """One-record pairs: each raw label decodes to one served ``Unit_sold``,
        which a reader would recognise.  A sold unit is never the harvest ``u``
        by assumption."""
        src = _source_sales(t)
        one = src[~src.duplicated(["i", "crop"], keep=False) & (src["q"] > 0)].copy()
        rows = served[served["t"] == t].copy()
        rows["i"] = rows["i"].astype(str)
        rows["crop"] = rows["crop"].astype(str)
        one["i"] = one["i"].astype(str)
        one["crop"] = one["crop"].astype(str)
        m = one.merge(rows[_has_sale(rows)][["i", "crop", "Unit_sold"]], on=["i", "crop"])
        assert m["Unit_sold"].notna().all()
        pairs = m.groupby("unit_label")["Unit_sold"].nunique()
        assert (pairs == 1).all(), pairs[pairs != 1]
        decoded = m.groupby("unit_label")["Unit_sold"].first()
        for raw, want in [("SACK", "Sack/Bag"), ("KILOGRAM", "Kg"), ("BASKET", "Basket")]:
            hits = decoded[decoded.index.str.contains(raw) & ~decoded.index.str.contains("BIN")]
            assert (hits == want).all(), hits

    def test_derivation_keys_mark_the_paths(self, served, t):
        src = _source_sales(t)
        n_records = src.groupby(["i", "crop"]).size()
        rows = served[(served["t"] == t) & _has_sale(served)].copy()
        rows["pair"] = list(zip(rows["i"].astype(str), rows["crop"].astype(str)))
        n_records.index = [(str(a), str(b)) for a, b in n_records.index]
        rows["n_records"] = rows["pair"].map(n_records)
        keys = rows["Derivation"].astype("string")
        attached = rows["plot_id"] != FARM_LEVEL
        assert keys[attached].str.contains(SINGLE_ROW, regex=False).all()
        lone_farm = ~attached & (rows["n_records"] == 1)
        assert keys[lone_farm].isna().all(), "a Farm-level answer at its own grain has no key"
        several = rows["n_records"] > 1
        assert keys[several].str.contains("records-summed|two-units").all()
        assert not keys[~several].fillna("").str.contains("records-summed|two-units").any()


@pytest.mark.requires_s3
@pytest.mark.parametrize("t", sorted(PLOT_GRAIN_SALES))
class TestW1W2Untouched:
    def test_sales_match_the_plot_crop_row_and_nothing_is_added(self, served, t):
        from lsms_library.local_tools import get_dataframe
        cfg = PLOT_GRAIN_SALES[t]
        d = get_dataframe(str(NIGERIA / cfg["wave"] / cfg["file"]), convert_categoricals=False)
        code = pd.to_numeric(d[cfg["crop"]], errors="coerce").astype("Int64")
        raw = pd.DataFrame({
            "i": d["hhid"].map(_hh).astype("string").values,
            "plot_id": d["plotid"].map(_hh).astype("string").values,
            "crop": code.map(_crop_labels()).astype("string").values,
            "q": pd.to_numeric(d["sa3q11a"], errors="coerce").values,
            "v": pd.to_numeric(d["sa3q12"], errors="coerce").values,
        }).dropna(subset=["i", "crop"]).sort_values(["i", "plot_id", "crop"], kind="stable")
        raw = raw.drop_duplicates(["i", "plot_id", "crop"], keep="first")
        raw["i"] = _walk(t, raw["i"]).values
        rows = served[served["t"] == t]
        assert FARM_LEVEL not in set(rows["plot_id"])
        assert rows["Derivation"].isna().all()
        assert len(rows) == len(raw)
        key = ["i", "plot_id", "crop"]
        got = rows.astype({k: str for k in key}).set_index(key).sort_index()
        want = raw.astype({k: str for k in key}).set_index(key).sort_index()
        assert got.index.equals(want.index)
        for ours, theirs in (("Value_sold", "v"), ("Quantity_sold", "q")):
            a = pd.to_numeric(got[ours], errors="coerce").astype("float64").to_numpy()
            b = want[theirs].to_numpy()
            assert (np.isnan(a) == np.isnan(b)).all(), ours
            assert np.allclose(a[~np.isnan(a)], b[~np.isnan(b)]), ours


@pytest.mark.requires_s3
@pytest.mark.parametrize("key", [SINGLE_ROW, SUMMED, TWO_UNITS])
def test_derivation_inputs_join_to_the_served_rows(served, key):
    """Each key's raw answers join the rows carrying it, by ``(t, i, plot_id, crop)``,
    and the quantities a two-unit row withholds come back from here."""
    import lsms_library as ll
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        inputs = ll.Country("Nigeria").derivation_inputs(key)
    assert list(inputs.index.names) == ["t", "i", "plot_id", "crop", "source_row"]
    assert inputs.index.is_unique
    carrying = served[served["Derivation"].astype("string").str.contains(key, regex=False, na=False)]
    got = set(map(tuple, inputs.index.droplevel("source_row").to_frame(index=False)
                  .astype(str).drop_duplicates().values))
    want = set(map(tuple, carrying[["t", "i", "plot_id", "crop"]].astype(str).values))
    assert got == want
