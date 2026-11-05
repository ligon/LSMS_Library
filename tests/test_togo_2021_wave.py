"""GH #552: Togo 2021-22 (EHCVM-2, WB 6279) is wired, under ids of its own.

The first class needs no microdata.  ``TestServed`` builds real tables and carries
``requires_s3``; its ORACLE reads the ``.dta`` files through ``get_dataframe``,
imports nothing from ``Togo/_/`` or ``Togo/2021-22/_/``, spells the household id
itself and decodes values from the CODES in the file, with its own tables.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

WAVE = "2021-22"
ROOT = countries_root() / "Togo" / WAVE / "Data"
PREFIX = "E2_"


def _raw(name, labels=False):
    return get_dataframe(ROOT / name, convert_categoricals=labels)


def _i(grappe, menage):
    return f"{PREFIX}{int(grappe)}0{int(menage):02d}"


def _ids(frame):
    return [_i(g, m) for g, m in zip(frame["grappe"], frame["menage"])]


def _served(table):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return getattr(ll.Country("Togo"), table)(waves=[WAVE])


class TestSaleRules:
    """The shared EHCVM-2 functions on synthetic frames, through the registry."""

    KEY = "Togo::crop_production::hh-crop-sale-single-row"

    def _harvest(self):
        return pd.DataFrame({"i": ["a", "a", "b", "c"], "crop": ["x", "x", "x", "z"],
                             "plot_id": ["1_1", "1_2", "1_1", "1_1"],
                             "Quantity": [1.0, 2.0, 3.0, np.nan]})

    def _sales(self):
        return pd.DataFrame({"i": ["a", "b"], "crop": ["x", "x"], "Quantity_sold": [5.0, 6.0],
                             "Unit_sold": ["Kg", "Tas"], "Value_sold": [50.0, 60.0]})

    def test_one_harvest_row_takes_the_sale_and_the_key(self):
        from lsms_library.ehcvm import derive_hh_crop_sale_attachment

        out = derive_hh_crop_sale_attachment(self._harvest(), self._sales(), self.KEY)
        row = out[(out["i"] == "b") & (out["plot_id"] == "1_1")].iloc[0]
        assert row["Quantity_sold"] == 6.0 and row["Unit_sold"] == "Tas"
        assert row["Derivation"] == self.KEY
        assert out[out["i"] == "c"]["Derivation"].isna().all()

    def test_two_harvest_rows_send_the_sale_to_one_farm_level_row(self):
        from lsms_library.ehcvm import FARM_LEVEL, derive_hh_crop_sale_attachment

        out = derive_hh_crop_sale_attachment(self._harvest(), self._sales(), self.KEY)
        a = out[out["i"] == "a"]
        assert a["Value_sold"].sum() == 50.0 and a["Value_sold"].notna().sum() == 1
        farm = a[a["plot_id"] == FARM_LEVEL].iloc[0]
        assert pd.isna(farm["Quantity"]) and pd.isna(farm["Derivation"])

    def test_a_second_sale_of_a_pair_raises(self):
        from lsms_library.ehcvm import derive_hh_crop_sale_attachment

        sales = pd.concat([self._sales(), self._sales().iloc[:1]])
        with pytest.raises(ValueError):
            derive_hh_crop_sale_attachment(self._harvest(), sales, self.KEY)

    def test_records_drop_exact_repeats_and_refuse_a_conflict(self):
        from lsms_library.ehcvm import hh_crop_records

        frame = pd.DataFrame({"grappe": [1, 1], "menage": [2, 2], "s16dq01": [3, 3],
                              "s16dq06": [10.0, 10.0]})
        assert len(hh_crop_records(frame)) == 1
        frame.loc[1, "s16dq06"] = 11.0
        with pytest.raises(ValueError):
            hh_crop_records(frame)

    def test_the_window_key_joins_the_sale_key_and_needs_a_quantity(self):
        from lsms_library.derivations import MULTI_SEP
        from lsms_library.ehcvm import stamp_harvest_to_date

        frame = pd.DataFrame({"Quantity": [1.0, np.nan, 2.0], "finished": ["Non", "Non", "Oui"],
                              "Derivation": pd.array([self.KEY, pd.NA, pd.NA], dtype="string")})
        out = stamp_harvest_to_date(frame, "Togo::crop_production::harvest-to-date")
        assert out["Derivation"].iloc[0] == MULTI_SEP.join(
            sorted([self.KEY, "Togo::crop_production::harvest-to-date"]))
        assert out["Derivation"].iloc[1:].isna().all()


class TestDeclared:
    def test_a_split_good_does_not_take_2018s_bundle_label(self):
        """#833's rule.  2018's `Fromage local` is 20x this wave's `Fromage` per small
        unit and 1x its soy cheese; `Piment frais` is the one recorded exception."""
        from lsms_library.local_tools import df_from_orgfile

        table = df_from_orgfile(countries_root() / "Togo" / "_" / "categorical_mapping.org",
                                name="harmonize_food")
        label = dict(zip(table["Original Label"].astype(str).str.strip(),
                         table["Preferred Label"].astype(str).str.strip()))
        assert label["Fromage local"] == "Fromage"
        assert label["Fromage"] == "Fromage (hors soja)"
        assert label["Fromage à base de soja"] == "Fromage à base de soja"
        assert label["Arachides décortiquées"] == "Arachides décortiquées (hors pilées)"
        assert label["Arachides pilées"] == "Arachides pilées"
        assert label["Piment"] == label["Piment frais"] == "Piment frais"

    def test_every_script_of_the_wave_uses_the_waves_id(self):
        """A script on 2018's bare i would orphan its table from sample."""
        import ast

        for path in sorted((countries_root() / "Togo" / WAVE / "_").glob("*.py")):
            if path.name == "mapping.py":
                continue
            source = path.read_text(encoding="utf-8")
            names = {a.name for node in ast.walk(ast.parse(source))
                     if isinstance(node, ast.ImportFrom) and node.module == "mapping"
                     for a in node.names}
            assert names & {"household_ids", "ID_PREFIX"}, path.name

    def test_the_wave_reads_its_own_nonfood_table(self):
        """Its codes are not 2018's: 415 was Perfume there and is a mask here."""
        from lsms_library.local_tools import df_from_orgfile

        table = df_from_orgfile(countries_root() / "Togo" / WAVE / "_" / "categorical_mapping.org",
                                name="nonfood_items").set_index("Code")["Preferred Label"]
        table.index = table.index.astype(int)
        assert table[415] == "Washable Face Masks (COVID-19)"
        assert table[417] == "Perfume"
        assert table.is_unique

    def test_this_wave_prefixes_and_2018_does_not(self):
        fns = ll.Country("Togo")
        new = fns[WAVE].formatting_functions["i"]
        old = fns["2018"].formatting_functions["i"]
        assert new(pd.Series([100, 1])) == "E2_100001"
        assert old(pd.Series([100, 1])) == "100001"

    def test_the_person_id_is_one_number_in_this_wave(self):
        pid = ll.Country("Togo")[WAVE].formatting_functions["pid"]
        assert pid(3) == "3"


@pytest.mark.requires_s3
class TestServed:
    def test_the_ids_cannot_meet_2018s(self):
        s = _served("sample")
        assert s.index.get_level_values("i").str.startswith(PREFIX).all()
        c18 = get_dataframe(countries_root() / "Togo" / "2018" / "Data1" / "s00_me_tgo2018.dta",
                            convert_categoricals=False)
        bare = {f"{int(g)}0{int(m):02d}" for g, m in zip(c18["grappe"], c18["menage"])}
        assert not bare & set(s.index.get_level_values("i"))

    def test_sample_is_the_cover_and_the_weights(self):
        cover = _raw("s00_me_tgo2021.dta")
        weights = _raw("ehcvm_ponderations_tgo2021.dta")
        s = _served("sample").droplevel("t")
        assert set(s.index) == set(_ids(cover))
        assert len(s) == len(cover) == 6462
        region = {1: "Maritime", 2: "Plateaux", 3: "Centrale", 4: "Kara", 5: "Savanes",
                  6: "Grand Lomé"}
        by_id = dict(zip(_ids(cover), cover["s00q01"].map(region)))
        assert s["strata"].to_dict() == by_id
        # Weights come back at mean 1 in the wave, each household keeping its share.
        raw = pd.Series(weights["poids"].to_numpy() / weights["poids"].mean(), index=_ids(weights))
        pd.testing.assert_series_equal(s["weight"].astype(float).sort_index(),
                                       raw.sort_index(), check_names=False)
        pd.testing.assert_series_equal(s["panel_weight"].astype(float).sort_index(),
                                       raw.sort_index(), check_names=False)

    def test_the_cluster_point_is_constant_within_a_grappe(self):
        cover = _raw("s00_me_tgo2021.dta")
        points = cover.groupby("grappe")[["GPS__Latitude", "GPS__Longitude"]].nunique()
        assert (points == 1).all(axis=None)
        c = _served("cluster_features")
        assert len(c) == cover["grappe"].nunique() == 540
        expected = cover.groupby("grappe")["GPS__Latitude"].first()
        expected.index = expected.index.astype(str)
        got = c.droplevel("t")["Latitude"].astype(float)
        pd.testing.assert_series_equal(got.sort_index(), expected.sort_index(),
                                       check_names=False, check_index_type=False)

    def test_roster_people_sex_and_relationship_are_the_files(self):
        s01 = _raw("s01_me_tgo2021.dta")
        r = _served("household_roster").reset_index()
        assert len(r) == len(s01) == 28815
        key = pd.MultiIndex.from_arrays([_ids(s01), s01["membres__id"].astype(str)])
        sex = pd.Series(s01["s01q01"].map({1: "M", 2: "F"}).to_numpy(), index=key)
        got = pd.Series(r["Sex"].astype(str).to_numpy(),
                        index=pd.MultiIndex.from_arrays([r["i"], r["pid"]]))
        assert got.sort_index().equals(sex.sort_index())
        assert (r.groupby("i")["Generation"].apply(lambda g: (g == 0).any())).all()

    def test_age_agrees_with_the_surveys_own_derived_age(self):
        ind = _raw("ehcvm_individu_tgo2021.dta")
        own = pd.Series(ind["age"].to_numpy(dtype=float),
                        index=pd.MultiIndex.from_arrays([_ids(ind), ind["numind"].astype(str)]))
        r = _served("household_roster").reset_index()
        age = pd.Series(pd.to_numeric(r["Age"], errors="coerce").to_numpy(dtype=float),
                        index=pd.MultiIndex.from_arrays([r["i"], r["pid"]]))
        both = pd.concat({"own": own, "age": age}, axis=1).dropna()
        assert len(both) >= 28800
        assert ((both["own"] - both["age"]).abs() <= 1).mean() > 0.98

    def test_people_join_across_tables(self):
        r = _served("household_roster").reset_index()
        e = _served("individual_education").reset_index()
        assert set(zip(r["i"], r["pid"])) == set(zip(e["i"], e["pid"]))

    def test_step_follows_the_label_not_the_code(self):
        s20 = _raw("s20a_me_tgo2021.dta")
        # The file's codes run the other way from the served step.
        step = s20["s20aq05"].map({1: 4, 2: 3, 3: 2, 4: 1})
        expected = pd.Series(step.to_numpy(), index=_ids(s20)).dropna().astype(int)
        got = _served("subjective_well_being").reset_index().set_index("i")["Own Step"]
        pd.testing.assert_series_equal(got.astype(int).sort_index(), expected.sort_index(),
                                       check_names=False, check_dtype=False)

    def test_housing_reads_the_roof_and_floor_of_this_wave(self):
        s11 = _raw("s11_me_tgo2021.dta")
        h = _served("housing").reset_index().set_index("i")
        roof = {1: "Concrete", 2: "Clay Tiles", 3: "Iron Sheets", 4: "Thatch", 5: "Mud",
                6: "Thatch", 7: "Mats", 8: "Other"}
        expected = pd.Series(s11["s11q19"].map(roof).to_numpy(), index=_ids(s11))
        assert h["Roof"].astype(str).sort_index().equals(expected.sort_index())

    def test_assets_are_the_owned_lines_of_the_file(self):
        s12 = _raw("s12_me_tgo2021.dta")
        a = _served("assets")
        assert len(a) == len(s12) == 33619
        assert a["Quantity"].astype(float).sum() == pytest.approx(s12["s12q03"].sum())
        assert a["Value"].astype(float).sum() == pytest.approx(s12["s12q09"].sum())
        # Every label in the file has a harmonized name in Togo's table.
        labels = set(_raw("s12_me_tgo2021.dta", labels=True)["s12q01"].astype(str))
        assert not labels & set(a.index.get_level_values("j"))

    def test_a_shock_row_is_a_shock_the_household_reported(self):
        s14 = _raw("s14b_me_tgo2021.dta")
        s = _served("shocks")
        hit = s14[s14["s14bq02"] == 1]
        assert set(s.index.get_level_values("i")) <= set(_ids(hit))
        # The first coping strategy of a row is the column ranked 1 in the file.
        ranked = hit.filter(regex=r"s14bq05__\d+$")
        assert len(s) > 0
        assert s["HowCoped0"].notna().sum() <= (ranked == 1).any(axis=1).sum()

    def test_consumption_effect_reads_the_food_stock_not_the_herd(self):
        s14 = _raw("s14b_me_tgo2021.dta", labels=True)
        answered = s14[s14["s14bq04a"].notna()]
        effect = {"Diminué": True, "Augmenté": True, "Inchangé": False, "Non-concerné": False}
        food = answered["s14bq04e"].astype(str).map(effect).astype(bool)
        herd = answered["s14bq04d"].astype(str).map(effect).astype(bool)
        got = _served("shocks")["AffectedConsumption"].astype(bool)
        assert len(got) == len(answered)
        assert got.sum() == food.sum()
        assert food.sum() != herd.sum()

    def test_other_strategy_is_served_though_its_column_says_oui(self):
        s14 = _raw("s14b_me_tgo2021.dta", labels=True)
        answered = s14[s14["s14bq04a"].notna()]
        oui = (answered["s14bq05__25"].astype(str) == "Oui").sum()
        s = _served("shocks")
        assert oui == 26
        assert (s["HowCoped0"].astype("string") == "Autre stratégie").sum() == oui
        assert s["HowCoped0"].notna().all()

    def test_only_column_25_of_the_coping_grid_is_not_ranked(self):
        s14 = _raw("s14b_me_tgo2021.dta", labels=True)
        ranks = {"Non", "Première stratégie", "Deuxième strategie", "Troisième stratégie"}
        off = [n for n in range(1, 27)
               if not set(s14[f"s14bq05__{n}"].dropna().astype(str)) <= ranks]
        assert off == [25]

    def test_fies_score_counts_the_yes_answers(self):
        s08 = _raw("s08a_me_tgo2021.dta")
        items = s08[[f"s08aq0{n}" for n in range(1, 9)]]
        yes = pd.Series((items == 1).sum(axis=1).to_numpy(), index=_ids(s08))
        got = _served("food_security").reset_index().set_index("i")["FIES_score"].astype(int)
        pd.testing.assert_series_equal(got.sort_index(), yes.reindex(got.index).sort_index(),
                                       check_names=False, check_dtype=False)

    def test_interview_dates_are_the_covers(self):
        cover = _raw("s00_me_tgo2021.dta")
        d = _served("interview_date").xs(1, level="visit").reset_index().set_index("i")
        expected = pd.Series(pd.to_datetime(cover["s00q23a"]).to_numpy(), index=_ids(cover))
        got = pd.to_datetime(d["Interview start"])
        assert (got.sort_index() == expected.reindex(got.index).sort_index()).all()

    def test_food_acquired_sums_are_the_files(self):
        s07 = _raw("s07b_me_tgo2021.dta")
        qty = (s07["s07bq03a"].fillna(0) - s07["s07bq04"].fillna(0)
               - s07["s07bq05"].fillna(0)).clip(lower=0)
        same = ((s07["s07bq03b"] == s07["s07bq07b"]) & (s07["s07bq03c"] == s07["s07bq07c"])
                & (s07["s07bq07a"] > 0))
        value = (qty * s07["s07bq08"] / s07["s07bq07a"]).where(same)
        f = _served("food_acquired")
        side = f.groupby(level="s")[["Quantity", "Expenditure"]].sum()
        assert side.loc["purchased", "Quantity"] == pytest.approx(qty.sum())
        assert side.loc["purchased", "Expenditure"] == pytest.approx(value.sum())
        assert side.loc["produced", "Quantity"] == pytest.approx(s07["s07bq04"].fillna(0).sum())
        assert side.loc["inkind", "Quantity"] == pytest.approx(s07["s07bq05"].sum())
        assert f.loc[f.index.get_level_values("s") != "purchased", "Expenditure"].isna().all()
        assert f.index.is_unique

    def test_the_purchase_unit_reads_as_the_code_says(self):
        """Without the wave's map, 15,412 rows read as another unit (all sachets)."""
        from lsms_library.ehcvm import unit_match

        labels = _raw("s07b_me_tgo2021.dta", labels=True)
        codes = _raw("s07b_me_tgo2021.dta")
        by_code = ((codes["s07bq03b"] == codes["s07bq07b"])
                   & (codes["s07bq03c"] == codes["s07bq07c"])).to_numpy()
        fn = ll.Country("Togo")[WAVE].formatting_functions["LastPurchaseUnitCode"]
        last = labels["s07bq07b"].map(fn)
        got = unit_match(labels["s07bq03b"], labels["s07bq03c"],
                         last, labels["s07bq07c"]).to_numpy()
        assert (got == by_code).all()
        raw = unit_match(labels["s07bq03b"], labels["s07bq03c"],
                         labels["s07bq07b"], labels["s07bq07c"]).to_numpy()
        assert (raw != by_code).sum() > 15000

    def test_unit_labels_are_the_codes_ehcvm_units_name(self):
        """u agrees with 2018's across the file's lower-case labels; the size rides."""
        f = _served("food_acquired")
        bases = {u.split(" (")[0] for u in f.index.get_level_values("u").unique()}
        assert {"Sachet", "Tas", "Unité", "Boîte", "Morceau", "Panier"} <= bases
        assert not bases & {"sachet", "tas", "unité", "boîte", "morceau", "panier"}
        assert not {u for u in f.index.get_level_values("u").unique() if u.startswith("Kg (")}
        # The file labels three codes 2018's has no name for.
        assert {"Main", "Tête"} <= bases

    def test_every_item_label_of_the_file_is_mapped(self):
        from lsms_library.local_tools import df_from_orgfile

        table = df_from_orgfile(countries_root() / "Togo" / "_" / "categorical_mapping.org",
                                name="harmonize_food")
        keys = set(table["Original Label"].astype(str).str.strip())
        labels = set(_raw("s07b_me_tgo2021.dta", labels=True)["s07bq01"].astype(str).str.strip())
        assert not labels - keys

    def test_crop_production_is_the_harvest_rows_and_each_sale_once(self):
        s16c = _raw("s16c_me_tgo2021.dta")
        s16c = s16c[s16c["s16cq04"].notna()]
        s16d = _raw("s16d_me_tgo2021.dta").drop_duplicates()
        sales = s16d[(s16d["s16dq05a"] > 0) | (s16d["s16dq06"] > 0)]
        keys = pd.Series(1, index=pd.MultiIndex.from_arrays([_ids(sales), sales["s16dq01"]]))
        assert keys.index.is_unique and len(sales) == 5976
        cp = _served("crop_production")
        assert cp.index.is_unique
        plots = cp[cp.index.get_level_values("plot_id") != "Farm-level"]
        assert len(plots) == len(s16c) == 10877
        assert plots["Quantity"].astype(float).sum() == pytest.approx(s16c["s16cq16a"].sum())
        assert cp["Value_sold"].astype(float).sum() == pytest.approx(sales["s16dq06"].sum())
        assert cp["Quantity_sold"].astype(float).sum() == pytest.approx(sales["s16dq05a"].sum())
        assert cp["Value_sold"].notna().sum() == len(sales)

    def test_a_sale_is_on_its_single_row_or_on_one_farm_level_row(self):
        s16c = _raw("s16c_me_tgo2021.dta")
        s16c = s16c[s16c["s16cq04"].notna()]
        s16d = _raw("s16d_me_tgo2021.dta").drop_duplicates()
        sales = s16d[(s16d["s16dq05a"] > 0) | (s16d["s16dq06"] > 0)]
        lines = s16c.groupby(["grappe", "menage", "s16cq04"]).size()
        count = lines.reindex(pd.MultiIndex.from_frame(
            sales[["grappe", "menage", "s16dq01"]])).fillna(0).to_numpy()
        cp = _served("crop_production").reset_index()
        farm = cp[cp["plot_id"] == "Farm-level"]
        assert len(farm) == (count > 1).sum()
        assert farm["Quantity"].isna().all() and farm["Value_sold"].notna().all()
        assert (farm["u"] == "Unknown").all() and farm["Derivation"].isna().all()
        keyed = cp["Derivation"].astype("string").str.contains("hh-crop-sale-single-row",
                                                               regex=False).fillna(False)
        assert keyed.sum() == (count == 1).sum()
        assert (count == 0).sum() == 0

    def test_the_window_key_is_on_the_unfinished_rows_that_record_a_quantity(self):
        s16c = _raw("s16c_me_tgo2021.dta")
        expected = ((s16c["s16cq11"] == 2) & s16c["s16cq16a"].notna()).sum()
        cp = _served("crop_production")
        keyed = cp["Derivation"].astype("string").str.contains("harvest-to-date",
                                                               regex=False).fillna(False)
        assert keyed.sum() == expected == 111

    def test_the_inputs_join_to_the_keyed_rows(self):
        c = ll.Country("Togo")
        cp = _served("crop_production").reset_index()
        for name, count in (("hh-crop-sale-single-row", None), ("harvest-to-date", 111)):
            key = f"Togo::crop_production::{name}"
            raw = c.derivation_inputs(key, wave=WAVE)
            keyed = cp[cp["Derivation"].astype("string").str.contains(name, regex=False).fillna(False)]
            assert set(zip(keyed["i"], keyed["crop"])) <= set(raw.index.droplevel("t"))
            if count:
                assert len(raw) == count

    def test_plot_inputs_read_the_purchase_not_the_gift(self):
        s16b = _raw("s16b_me_tgo2021.dta")
        s16b = s16b[s16b["s16bq02"] == 1]
        p = _served("plot_inputs")
        assert len(p) == len(s16b)
        assert p["Purchased"].astype("boolean").sum() == (s16b["s16bq08"] == 1).sum()
        assert p["Received_as_gift"].astype("boolean").sum() == (s16b["s16bq05"] == 1).sum()
        assert p["Quantity"].astype(float).sum() == pytest.approx(s16b["s16bq03a"].sum())
        assert p["Quantity_purchased"].astype(float).sum() == pytest.approx(s16b["s16bq09a"].sum())
        assert p["Quantity_received_as_gift"].astype(float).sum() == pytest.approx(s16b["s16bq07a"].sum())
        # The two seed slots 2018's table did not name are not lost in a catch-all.
        crops = p.index.get_level_values("crop").to_series()
        assert crops.eq("Arachide").sum() == (s16b["s16bq01"] == 19).sum()
        assert crops.eq("Hévéa, teck").sum() == (s16b["s16bq01"] == 21).sum()

    def test_plot_features_area_prefers_the_measure(self):
        s16a = _raw("s16a_me_tgo2021.dta")
        f = _served("plot_features").reset_index()
        assert len(f) == len(s16a) == 9182
        estimate = s16a["s16aq09a"].where(s16a["s16aq09b"] == 1, s16a["s16aq09a"] / 10000)
        area = s16a["s16aq47"].astype(float).where(s16a["s16aq45"] == 1, estimate)
        area = area.where(area > 0)
        assert f["Area"].astype(float).sum() == pytest.approx(area.sum())

    def test_plot_labor_totals_are_the_grids(self):
        s16a = _raw("s16a_me_tgo2021.dta")
        family = s16a.filter(regex=r"^s16aq(33|35|37)b_\d+$").fillna(0).to_numpy().sum()
        hired = 0.0
        wage = 0.0
        for base in ("s16aq39", "s16aq41", "s16aq43"):
            for n in range(1, 5):
                hired += (s16a[f"{base}a_{n}"] * s16a[f"{base}b_{n}"]).sum()
                wage += s16a[f"{base}c_{n}"].fillna(0).sum()
        got = _served("plot_labor").groupby(level="source")[["PersonDays", "Wage"]].sum()
        assert got.loc["family", "PersonDays"] == pytest.approx(family)
        assert got.loc["hired", "PersonDays"] == pytest.approx(hired)
        assert got.loc["hired", "Wage"] == pytest.approx(wage)

    def test_livestock_is_the_roster(self):
        s17 = _raw("s17_me_tgo2021.dta")
        got = _served("livestock")
        assert len(got) == len(s17)
        assert got["HeadCount"].astype(float).sum() == pytest.approx(s17["s17q06"].sum())
        assert got["HeadSold"].astype(float).sum() == pytest.approx(s17["s17q10"].sum())
        cattle = got.xs("Cattle", level="animal")["HeadCount"].astype(float).sum()
        assert cattle == pytest.approx(s17.loc[s17["s17q01"] == 1, "s17q06"].sum())

    def test_people_last7days_carries_every_member(self):
        s04 = _raw("s04a_me_tgo2021.dta")
        got = _served("people_last7days")
        assert len(got) == len(s04) == 28815
        assert got["farm_work"].astype("boolean").sum() == (s04["s04q06"] == 1).sum()
        assert got["wage_work"].astype("boolean").sum() == (s04["s04q08"] == 1).sum()

    def test_every_nonfood_label_is_in_the_waves_own_table(self):
        from lsms_library.local_tools import df_from_orgfile

        table = df_from_orgfile(countries_root() / "Togo" / WAVE / "_" / "categorical_mapping.org",
                                name="nonfood_items")
        got = set(_served("nonfood_expenditures").index.get_level_values("j"))
        assert got <= set(table["Preferred Label"].astype(str).str.strip())

    def test_nonfood_items_follow_the_label_not_the_code(self):
        got = _served("nonfood_expenditures")
        assert got.index.is_unique
        by_window = got.groupby("RecallWindow")["Expenditure"].sum()
        for letter, window in dict(b="7 days", c="30 days", d="3 months", e="6 months",
                                   f="12 months").items():
            raw = _raw(f"s09{letter}_me_tgo2021.dta")
            assert by_window[window] == pytest.approx(raw[f"s09{letter}q03"].sum())
        s09d = _raw("s09d_me_tgo2021.dta")
        mask = got.xs("Washable Face Masks (COVID-19)", level="j")["Expenditure"].astype(float).sum()
        assert mask == pytest.approx(s09d.loc[s09d["s09dq01"] == 415, "s09dq03"].sum())
