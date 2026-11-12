"""GH #824: Mali's EHCVM ``crop_production`` (rulings @ligon 2026-11-03).

2018-19 is one plot-by-crop file, ``s16c``, with harvest and sale on the line
(Niger's pattern).  2021-22 splits the module: ``s16c`` holds plot-by-crop rows
whose harvest ``s16cq16a`` is mostly skipped, and ``s16d`` holds household-by-crop
sales with no plot.  Under option (d) a sale goes on the household's single harvest
ROW for the crop where there is exactly one, keyed ``hh-crop-sale-single-row``, and
otherwise on ONE row with ``plot_id = 'Farm-level'``.  Nothing is dropped.  The harvest
total of GH #1437 has its own tests: ``tests/test_gh1437_mali_harvest_total.py``.

Four classes in three tiers (the first two need no microdata):

``TestRules``
    the rule on synthetic frames, through the registered function.
``TestDeclared``
    config only: the tables, the registry and the kg ladder's reading of a label.
``TestServed``, ``TestFoodLabelsDoNotMove``
    need S3 credentials (``requires_s3``): the first builds the real table and
    the second reads the raw food files.  The ORACLE reads the ``.dta`` files directly and
    imports nothing from ``Mali/_/``: it spells the household id and the plot id
    itself and decodes units from their CODES with its own table.

**Where the oracle is NOT independent: crops.**  It maps a crop through the
``harmonize_food`` table of ``categorical_mapping.org``, the table the build
reads, so a wrong row there moves both sides and every row-level check still
passes (``Souchet`` -> ``Mil`` and ``Radis`` -> ``Persil`` both did, in the red team
of GH #824).  What pins a crop label is the literal list ``NEW_CROPS``, in
``TestDeclared`` against the table and in ``TestServed`` against the source
counts.  A crop label that was already in the table before GH #824 is not pinned.

Measured on a cold build; see ``Mali/_/CONTENTS.org``, "2026-11-03 -- EHCVM
crop_production wired (GH #824)".
"""
from __future__ import annotations

import hashlib
import json
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library import transformations as T
from lsms_library.derivations import records_for, resolve_callable
from lsms_library.local_tools import df_from_orgfile, get_dataframe
from lsms_library.paths import countries_root

COUNTRY = "Mali"
TABLE = "crop_production"
KEY = "Mali::crop_production::hh-crop-sale-single-row"
#: GH #1437: a harvest key can share `Derivation` with KEY, joined with '+'.
WINDOW = "Mali::crop_production::harvest-to-date"
TOTAL = "Mali::crop_production::harvest-from-dispositions"
FARM = "Farm-level"
# Reviewed namespace transport at d47ff849; ledger 1687-mali-namespace-oracle §3.
UPDATED_IDS_SHA256 = "7c17086a611c44bdd1f99b79305636feb67c8273cd349b3e81a580cdbe6649dd"

#: The ONE unit code book of the EHCVM crop modules, written out here so the
#: oracle owes nothing to any label or to the `u` table.  s16cq12b, s16cq16b,
#: s16dq05b share these codes while spelling some of them differently.
UNIT_BY_CODE = {1: "Kg", 2: "Unité", 3: "Cart (donkey)", 4: "Cart (ox)",
                5: "Sac (25 Kg)", 6: "Sac (50 Kg)", 7: "Sac (100 Kg)", 8: "Grenier",
                9: "Botte", 10: "Gerbe", 11: "Baignoire", 12: "Panier", 13: "Autre",
                14: "Tonne"}

#: Rows of the EAC-I waves, which this change must not move.  Re-pinned for GH #1457:
#: 2017-18 gains 202 Farm-level rows, the s7g sales of crops on two or more plots.
EACI_ROWS = {"2014-15": 10341, "2017-18": 24719 + 202}
#: 2018-19: the source's rows.  2021-22: the source's rows plus the Farm-level ones:
#: 70 for a sale, and since GH #1437 181 more for a derived total with no sale there.
WAVE_ROWS = {"2018-19": 7992, "2021-22": 7247 + 70 + 181}
#: 2021-22 sale records by where they go.
ATTACHED, FARM_TWO_ROWS, FARM_THREE_PLUS, FARM_NO_ROW = 1070, 58, 12, 0
#: Raw totals, FCFA.
VALUE_2018, VALUE_2021 = 623_474_428.0, 418_697_975.0
#: The crops GH #824 added to harmonize_food: (source label, served label).
NEW_CROPS = [("Riz Paddy", "Riz"), ("Autre (à préciser)", "Autres cultures"), ("Chou", "Chou"),
             ("Calebassier", "Calebasse"), ("Oseille", "Oseille"), ("Souchet", "Souchet"),
             ("Persil", "Persil"), ("Céleri", "Céleri"), ("Radis", "Radis"),
             ("Pomme à pin", "Pomme à pin")]
#: Source rows of the labels that occur in s16c on a few rows, counted by source label.
NEW_CROP_ROWS = [("2018-19", "s16c_me_mli2018.dta", "Souchet", "Souchet", 7),
                 ("2018-19", "s16c_me_mli2018.dta", "Persil", "Persil", 1),
                 ("2018-19", "s16c_me_mli2018.dta", "Céleri", "Céleri", 1),
                 ("2018-19", "s16c_me_mli2018.dta", "Radis", "Radis", 1),
                 ("2021-22", "s16c_me_mli2021.dta", "Céleri", "Céleri", 1),
                 ("2021-22", "s16c_me_mli2021.dta", "Pomme à pin", "Pomme à pin", 1)]


def _root() -> Path:
    return Path(countries_root()) / COUNTRY


def _derive():
    return records_for(COUNTRY, TABLE)[KEY].resolve("function")


# ---------------------------------------------------------------------------
# the rule, on synthetic frames, through the registered function
# ---------------------------------------------------------------------------

def _harvest(rows):
    return pd.DataFrame(rows, columns=["i", "plot_id", "crop", "Quantity"]).astype(
        {"i": "string", "plot_id": "string", "crop": "string", "Quantity": "float64"})


def _sales(rows):
    return pd.DataFrame(
        rows, columns=["i", "crop", "Quantity_sold", "Unit_sold", "Value_sold"]).astype(
        {"i": "string", "crop": "string", "Quantity_sold": "float64",
         "Unit_sold": "string", "Value_sold": "float64"})


class TestRules:

    def test_a_single_row_takes_the_sale_and_the_key_and_keeps_its_own_units(self):
        got = _derive()(_harvest([("h", "1_1", "Mil", 5.0)]),
                        _sales([("h", "Mil", 2.0, "Kg", 300.0)]))
        assert len(got) == 1
        row = got.iloc[0]
        assert (row.plot_id, row.Quantity, row.Quantity_sold, row.Unit_sold, row.Value_sold) == (
            "1_1", 5.0, 2.0, "Kg", 300.0)
        assert row.Derivation == KEY

    def test_a_single_row_with_no_harvest_still_takes_it(self):
        """One served row is the test, not one row stating a Quantity."""
        got = _derive()(_harvest([("h", "1_1", "Mil", np.nan)]),
                        _sales([("h", "Mil", 2.0, "Kg", 300.0)]))
        assert got.plot_id.tolist() == ["1_1"] and got.Derivation.tolist() == [KEY]

    @pytest.mark.parametrize("plots", [("1_1", "1_2"), ("1_1", "2_1", "3_1")])
    def test_several_rows_mean_one_farm_level_row_and_no_split(self, plots):
        got = _derive()(_harvest([("h", p, "Mil", 5.0) for p in plots]),
                        _sales([("h", "Mil", 2.0, "Kg", 300.0)]))
        assert len(got) == len(plots) + 1
        farm = got[got.plot_id == FARM]
        assert len(farm) == 1
        assert farm.Quantity.isna().all() and farm.Derivation.isna().all()
        assert farm.Quantity_sold.tolist() == [2.0] and farm.Value_sold.tolist() == [300.0]
        on_plots = got[got.plot_id != FARM]
        assert on_plots.Quantity_sold.isna().all() and on_plots.Value_sold.isna().all()
        assert on_plots.Quantity.tolist() == [5.0] * len(plots)

    def test_a_sale_with_no_harvest_row_is_kept_at_farm_level(self):
        got = _derive()(_harvest([("h", "1_1", "Mil", 5.0)]),
                        _sales([("h", "Coton", 2.0, "Kg", 300.0)]))
        assert got[got.crop == "Coton"].plot_id.tolist() == [FARM]
        assert got[got.crop == "Mil"].Quantity_sold.isna().all()

    def test_another_households_row_does_not_count(self):
        got = _derive()(_harvest([("h", "1_1", "Mil", 5.0), ("g", "1_1", "Mil", 7.0)]),
                        _sales([("h", "Mil", 2.0, "Kg", 300.0)]))
        assert got.set_index("i").loc["h", "Derivation"] == KEY
        assert pd.isna(got.set_index("i").loc["g", "Quantity_sold"])

    def test_two_sales_of_one_crop_are_refused_rather_than_summed(self):
        with pytest.raises(ValueError, match="more than one sale"):
            _derive()(_harvest([("h", "1_1", "Mil", 5.0)]),
                      _sales([("h", "Mil", 2.0, "Kg", 300.0), ("h", "Mil", 1.0, "Tonne", 9.0)]))


# ---------------------------------------------------------------------------
# config only
# ---------------------------------------------------------------------------

def _table(name):
    t = df_from_orgfile(str(_root() / "_" / "categorical_mapping.org"), name=name)
    return dict(zip(t["Code"].astype(str).str.strip(), t["Preferred Label"]))


class TestDeclared:

    @pytest.mark.parametrize("wave", ["2018-19", "2021-22"])
    def test_every_wave_declares_the_table(self, wave):
        assert TABLE in ll.Country(COUNTRY)[wave].data_scheme

    def test_the_index_is_still_the_eaci_index(self):
        class Loader(yaml.SafeLoader):
            pass
        Loader.add_constructor("!make", lambda loader, node: {"__make__": True})
        with open(_root() / "_" / "data_scheme.yml", encoding="utf-8") as f:
            entry = yaml.load(f, Loader=Loader)["Data Scheme"][TABLE]
        # GH #1436 appended the condition level; `u` is still a column here.
        assert entry["index"] == "(t, i, plot_id, crop, condition)"

    @pytest.mark.parametrize("raw, served", [
        ("Sacs de 100 kg", "Sac (100 Kg)"), ("Sac-100 Kg", "Sac (100 Kg)"),
        ("Sacs de 50 kg", "Sac (50 Kg)"), ("Sac-50kG", "Sac (50 Kg)"),
        ("Sacs de 25 kg", "Sac (25 Kg)"), ("Sac-25kg", "Sac (25 Kg)"),
        ("Charretée asine", "Cart (donkey)"), ("Charettée asine", "Cart (donkey)"),
        ("Charretée bovine", "Cart (ox)"), ("Charettée bovine", "Cart (ox)"),
        ("Gerbes", "Gerbe"), ("Autres (à préciser)", "Autre"),
        ("Baignoire", "Baignoire"), ("Botte", "Botte")])
    def test_the_u_table_names_the_new_units(self, raw, served):
        assert _table("u")[raw] == served

    @pytest.mark.parametrize("label, kg", [("Sac (100 Kg)", 100.0), ("Sac (50 Kg)", 50.0),
                                           ("Sac (25 Kg)", 25.0)])
    def test_the_kg_ladder_reads_a_sack_rather_than_inferring_it(self, label, kg):
        assert T._parse_explicit_metric(label) == kg

    @pytest.mark.parametrize("raw, served", NEW_CROPS)
    def test_a_new_crop_has_its_literal_label(self, raw, served):
        """Literal, because the oracle reads this table too (module docstring)."""
        assert _table("harmonize_food")[raw] == served

    def test_the_derivation_is_registered_and_resolves(self):
        rec = records_for(COUNTRY, TABLE)[KEY]
        # Re-pinned for GH #1457: the same rule now serves the 2017-18 s7g sales.
        assert list(rec.waves) == ["2017-18", "2021-22"]
        assert set(rec.columns) == {"Quantity_sold", "Unit_sold", "KgFactor_sold", "Value_sold",
                                    "Condition_sold", "condition_sold_native"}
        assert callable(rec.resolve("function")) and callable(rec.resolve("inputs"))

    def test_make_reaches_every_wave(self):
        text = (_root() / "_" / "Makefile").read_text(encoding="utf-8")
        rule = next(line for line in text.splitlines()
                    if line.startswith("$(VAR_DIR)/crop_production.parquet:"))
        for wave in ("2014-15", "2017-18", "2018-19", "2021-22"):
            assert f"../{wave}/_/crop_production.parquet" in rule


# ---------------------------------------------------------------------------
# the oracle: the source files, read directly
# ---------------------------------------------------------------------------

def _household(frame):
    return (frame["grappe"].astype(int).astype(str) + "0"
            + frame["menage"].astype(int).astype(str).str.zfill(2))


def _plot(frame):
    return frame["s16cq02"].astype(int).astype(str) + "_" + frame["s16cq03"].astype(int).astype(str)


def _codes(series):
    return pd.to_numeric(series, errors="coerce").map(UNIT_BY_CODE).astype("string")


def _raw(wave, name):
    path = _root() / wave / "Data" / name
    return (get_dataframe(str(path), convert_categoricals=False),
            get_dataframe(str(path), convert_categoricals=True))


def _walk(wave, ids):
    payload = (_root() / "_" / "updated_ids.json").read_bytes()
    if wave == "2021-22":
        assert hashlib.sha256(payload).hexdigest() == UPDATED_IDS_SHA256, "unreviewed Mali panel map"
        if not ids.str.fullmatch(r"[0-9]+").fillna(False).all():
            raise ValueError("expected bare numeric raw household keys")
        # All raw keys, including unlinked/unknown/orphan; no production formatter.
        ids = "EHCVM21_" + ids
    mapping = json.loads(payload).get(wave, {})
    return ids.map(lambda x: mapping.get(x, x))


def _oracle_2018():
    codes, labels = _raw("2018-19", "s16c_me_mli2018.dta")
    crops = _table("harmonize_food")
    return pd.DataFrame({
        "i": _walk("2018-19", _household(codes)).values,
        "plot_id": _plot(codes).values,
        "crop": labels["s16cq04"].astype("string").str.strip().map(crops).values,
        "Quantity": pd.to_numeric(codes["s16cq12a"]).values,
        "u": _codes(codes["s16cq12b"]).values,
        "Quantity_sold": pd.to_numeric(codes["s16cq16a"]).values,
        "Unit_sold": _codes(codes["s16cq16b"]).values,
        "Value_sold": pd.to_numeric(codes["s16cq17"]).values,
    }).set_index(["i", "plot_id", "crop"]).sort_index()


def _oracle_2021():
    """(harvest rows, sale records), both on the served labels."""
    crops = _table("harmonize_food")
    c_codes, c_labels = _raw("2021-22", "s16c_me_mli2021.dta")
    harvest = pd.DataFrame({
        "i": _walk("2021-22", _household(c_codes)).values,
        "plot_id": _plot(c_codes).values,
        "crop": c_labels["s16cq04"].astype("string").str.strip().map(crops).values,
        "Quantity": pd.to_numeric(c_codes["s16cq16a"]).values,
        "u": _codes(c_codes["s16cq16b"]).values})
    d_codes, d_labels = _raw("2021-22", "s16d_me_mli2021.dta")
    records = pd.DataFrame({
        "i": _walk("2021-22", _household(d_codes)).values,
        "crop": d_labels["s16dq01"].astype("string").str.strip().map(crops).values,
        "answer": d_labels["s16dq04"].astype("string").values,
        "Quantity_sold": pd.to_numeric(d_codes["s16dq05a"]).values,
        "Unit_sold": _codes(d_codes["s16dq05b"]).values,
        "Value_sold": pd.to_numeric(d_codes["s16dq06"]).values})
    return harvest, records


def _same(a, b):
    """Exact on the NA pattern, numeric equality elsewhere."""
    a, b = pd.to_numeric(a).to_numpy("float64", na_value=np.nan), pd.to_numeric(b).to_numpy(
        "float64", na_value=np.nan)
    assert (np.isnan(a) == np.isnan(b)).all()
    assert (a[~np.isnan(a)] == b[~np.isnan(b)]).all()


@pytest.mark.requires_s3
class TestServed:

    @pytest.fixture(scope="class")
    def built(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return ll.Country(COUNTRY).crop_production()

    @pytest.fixture(scope="class")
    def flat(self, built):
        return built.reset_index()

    @pytest.fixture(scope="class")
    def w18(self, flat):
        return flat[flat.t == "2018-19"].astype({"i": str, "plot_id": str, "crop": str}).set_index(
            ["i", "plot_id", "crop"]).sort_index()

    @pytest.fixture(scope="class")
    def w21(self, flat):
        return flat[flat.t == "2021-22"].astype({"i": str, "plot_id": str, "crop": str})

    @pytest.mark.parametrize("wave, n", sorted({**EACI_ROWS, **WAVE_ROWS}.items()))
    def test_rows_per_wave(self, flat, wave, n):
        assert int((flat.t == wave).sum()) == n

    def test_the_index_is_unique(self, built):
        assert built.index.is_unique

    # -- 2018-19 -------------------------------------------------------------

    def test_2018_every_source_row_is_served_as_recorded(self, w18):
        oracle = _oracle_2018()
        assert oracle.index.is_unique
        assert w18.index.equals(oracle.index)
        for col in ("Quantity", "Quantity_sold", "Value_sold"):
            _same(w18[col], oracle[col])
        for col in ("u", "Unit_sold"):
            pd.testing.assert_series_equal(
                w18[col].astype("string"), oracle[col].astype("string"), check_names=False)

    def test_2018_a_99_is_a_quantity_not_a_code(self, w18):
        """Unlike the EAC-I columns, nothing labels 99 as missing on s16cq12a."""
        assert int((w18.Quantity == 99).sum()) == 3

    def test_2018_units_agree_once_their_spellings_are_folded(self, w18):
        """The raw labels agree on 705 of 2,101 priced rows; the codes on 1,856."""
        priced = w18[(w18.Quantity_sold > 0) & (w18.Value_sold > 0)]
        assert len(priced) == 2101
        assert int((priced.u == priced.Unit_sold).sum()) == 1856

    def test_2018_value_total(self, w18):
        assert float(w18.Value_sold.sum()) == VALUE_2018

    def test_2018_carries_no_sale_derivation(self, w18):
        # Re-pinned for GH #1437: the window key is on unfinished rows here.
        assert set(w18.Derivation.dropna()) <= {WINDOW}

    # -- 2021-22 -------------------------------------------------------------

    def test_2021_every_source_row_is_served_with_its_harvest_only(self, w21):
        harvest, _ = _oracle_2021()
        served = w21[w21.plot_id != FARM].set_index(["i", "plot_id", "crop"]).sort_index()
        oracle = harvest.set_index(["i", "plot_id", "crop"]).sort_index()
        assert oracle.index.is_unique and served.index.equals(oracle.index)
        # Re-pinned for GH #1437: what is not keyed as a derived total is as recorded.
        recorded = ~served.Derivation.astype("string").str.contains(TOTAL, regex=False).fillna(False)
        _same(served.Quantity[recorded], oracle.Quantity[recorded])
        pd.testing.assert_series_equal(served.u[recorded].astype("string"),
                                       oracle.u[recorded].astype("string"), check_names=False)
        assert int(served.Quantity[recorded].notna().sum()) == 309

    def test_2021_the_partition_by_cause(self, w21):
        harvest, records = _oracle_2021()
        lines = harvest.groupby(["i", "crop"]).size()
        sales = records[(records.Quantity_sold > 0) | (records.Value_sold > 0)]
        n = pd.MultiIndex.from_frame(sales[["i", "crop"]]).map(lines).to_series().fillna(0)
        assert int((n == 1).sum()) == ATTACHED
        assert int((n == 2).sum()) == FARM_TWO_ROWS
        assert int((n >= 3).sum()) == FARM_THREE_PLUS
        assert int((n == 0).sum()) == FARM_NO_ROW
        # Re-pinned for GH #1437: a Farm-level row can also be a derived total alone.
        farm = w21[w21.plot_id == FARM]
        assert int((farm.Quantity_sold.notna() | farm.Value_sold.notna()).sum()) == (
            FARM_TWO_ROWS + FARM_THREE_PLUS + FARM_NO_ROW)

    def test_2021_each_sale_is_on_one_row_as_recorded(self, w21):
        harvest, records = _oracle_2021()
        lines = harvest.groupby(["i", "crop"]).size().rename("lines").reset_index()
        sales = records[(records.Quantity_sold > 0) | (records.Value_sold > 0)].merge(
            lines, on=["i", "crop"], how="left")
        sold = w21[w21.Quantity_sold.notna() | w21.Value_sold.notna()]
        assert not sold.duplicated(["i", "crop"]).any(), "a sale on two rows"
        joined = sales.merge(sold, on=["i", "crop"], how="outer", suffixes=("_raw", ""),
                             indicator=True, validate="one_to_one")
        assert (joined["_merge"] == "both").all(), "a sale lost or invented"
        _same(joined.Quantity_sold, joined.Quantity_sold_raw)
        _same(joined.Value_sold, joined.Value_sold_raw)
        pd.testing.assert_series_equal(joined.Unit_sold.astype("string"),
                                       joined.Unit_sold_raw.astype("string"), check_names=False)
        assert ((joined.plot_id == FARM) == (joined.lines != 1)).all()

    def test_2021_conservation(self, w21):
        _, records = _oracle_2021()
        assert float(w21.Value_sold.sum()) == float(records.Value_sold.sum()) == VALUE_2021
        for unit, total in records.groupby("Unit_sold").Quantity_sold.sum().items():
            assert float(w21[w21.Unit_sold == unit].Quantity_sold.sum()) == float(total)

    def test_2021_the_key_is_on_attached_rows_and_only_there(self, w21):
        # Re-pinned for GH #1437: KEY can share the column with a harvest key.
        has_key = w21.Derivation.astype("string").str.contains(KEY, regex=False).fillna(False)
        keyed = w21[has_key]
        assert len(keyed) == ATTACHED
        assert (keyed.plot_id != FARM).all()
        assert (keyed.Quantity_sold.notna() | keyed.Value_sold.notna()).all()
        assert not (w21.Quantity_sold.notna() & (w21.plot_id != FARM) & ~has_key).any()

    def test_2021_a_farm_level_row_states_a_sale_or_a_derived_total(self, w21):
        # Re-pinned for GH #1437: the row that holds a sale can hold the total too.
        farm = w21[w21.plot_id == FARM]
        total = farm.Derivation.astype("string").eq(TOTAL).fillna(False)
        assert (farm.Quantity_sold.notna() | farm.Value_sold.notna() | total).all()
        assert (farm.Quantity.notna() == total).all() and (farm.u.notna() == total).all()
        assert farm.Derivation[~total].isna().all()
        for col in ("intercropped", "KgFactor_sold"):
            assert farm[col].isna().all(), col
        assert not farm.duplicated(["i", "crop"]).any()

    def test_2021_derivation_inputs_join_the_keyed_rows(self, w21):
        inputs = ll.Country(COUNTRY).derivation_inputs(KEY, wave="2021-22").reset_index()
        keyed = w21[w21.Derivation.astype("string").str.contains(KEY, regex=False).fillna(False)]
        joined = keyed.merge(inputs.astype({"i": str, "crop": str}), on=["t", "i", "crop"],
                             validate="one_to_one")
        assert len(joined) == len(keyed) == len(inputs)
        _same(joined.Value_sold, joined.s16dq06)
        assert (joined.plot_id == (joined.s16cq02.astype(int).astype(str) + "_"
                                   + joined.s16cq03.astype(int).astype(str))).all()

    # -- both waves ----------------------------------------------------------

    @pytest.mark.parametrize("wave", sorted(WAVE_ROWS))
    def test_no_sale_zero_is_fabricated(self, flat, wave):
        rows = flat[flat.t == wave]
        assert int((rows.Quantity_sold == 0).sum()) == 0
        assert int((rows.Value_sold == 0).sum()) == 0

    @pytest.mark.parametrize("wave", sorted(WAVE_ROWS))
    @pytest.mark.parametrize("col", ["KgFactor_sold", "months_harvested", "planting_month",
                                     "harvest_month", "perennial"])
    def test_what_the_instrument_does_not_ask_is_na(self, flat, wave, col):
        assert flat.loc[flat.t == wave, col].isna().all()

    @pytest.mark.parametrize("wave", sorted(WAVE_ROWS))
    def test_a_recorded_crop_is_never_lost_and_nothing_is_unknown(self, flat, wave):
        rows = flat[flat.t == wave]
        assert rows.crop.notna().all() and not (rows.u == "Unknown").any()

    @pytest.mark.parametrize("wave, name, raw, served, n", NEW_CROP_ROWS)
    def test_a_new_crop_keeps_exactly_its_source_rows(self, flat, wave, name, raw, served, n):
        _, labels = _raw(wave, name)
        assert int(labels["s16cq04"].astype("string").str.strip().eq(raw).sum()) == n
        rows = flat[(flat.t == wave) & (flat.plot_id != FARM)]
        assert int(rows.crop.eq(served).sum()) == n

    def test_every_harvest_row_finds_its_plot(self, flat):
        plots = ll.Country(COUNTRY).plot_features().reset_index()
        for wave in WAVE_ROWS:
            rows = flat[(flat.t == wave) & (flat.plot_id != FARM)]
            keys = set(map(tuple, plots.loc[plots.t == wave, ["i", "plot_id"]].astype(str).values))
            assert all(k in keys for k in map(tuple, rows[["i", "plot_id"]].astype(str).values))

    # -- consumers -----------------------------------------------------------

    def test_harvest_kg_reads_a_derived_total_and_yield_no_farm_level_row(self, built):
        # Re-pinned for GH #1437: a Farm-level row now carries a total, as the trees do.
        got = T.harvest_kg(built).reset_index()
        farm = built.reset_index()
        farm = farm[(farm.t == "2021-22") & (farm.plot_id == FARM)]
        assert (farm.Quantity.notna() == farm.Derivation.astype("string").eq(TOTAL).fillna(False)).all()
        assert set(got.loc[(got.t == "2021-22") & (got.plot_id == FARM), "i"]) <= set(
            farm.loc[farm.Quantity.notna(), "i"])
        plots = ll.Country(COUNTRY).plot_features()
        assert FARM not in T.yield_kg(built, plots, on="plot").index.get_level_values("plot_id")

    def test_revenue_keeps_every_sale(self, built):
        got = T.gross_crop_revenue(built).reset_index()
        assert float(got[got.t == "2021-22"].Gross_crop_revenue.sum()) == VALUE_2021
        assert float(got[got.t == "2018-19"].Gross_crop_revenue.sum()) == VALUE_2018

    def test_a_sack_converts_at_its_stated_weight(self, built):
        got = T.harvest_kg_factors(built)
        flat = built.reset_index().assign(kg=got["kg_metric"].to_numpy(),
                                          served=got["kg_per_unit"].to_numpy(),
                                          source=got["KgFactorSource"].to_numpy())
        rows = flat[flat.t.isin(WAVE_ROWS) & (flat.u == "Sac (100 Kg)")]
        assert (rows.kg == 100.0).all()
        # Since GH #1436 a row with the survey's own kilograms is served those instead.
        assert (rows[rows.source != "survey_kg"].served == 100.0).all()


@pytest.mark.requires_s3
class TestFoodLabelsDoNotMove:
    """Every new key is a label the food side never records, so the tables' new
    rows cannot reach `food_acquired`.  Identity rows would be harmless anyway."""

    NEW_CROPS = ("Riz Paddy", "Autre (à préciser)", "Calebassier")
    NEW_UNITS = ("Sacs de 100 kg", "Sac-100 Kg", "Sacs de 50 kg", "Sac-50kG", "Sacs de 25 kg",
                 "Sac-25kg", "Charretée asine", "Charettée asine", "Charretée bovine",
                 "Charettée bovine", "Gerbes", "Autres (à préciser)")

    @pytest.mark.parametrize("wave, name", [("2018-19", "s07b_me_mli2018.dta"),
                                            ("2021-22", "s07b_me_mli2021.dta")])
    def test_no_new_key_is_a_raw_food_label(self, wave, name):
        _, labels = _raw(wave, name)
        foods = set(labels["s07bq01"].astype("string").str.strip().dropna())
        units = set(labels["s07bq03b"].astype("string").str.strip().dropna())
        assert not foods & set(self.NEW_CROPS)
        assert not units & set(self.NEW_UNITS)
