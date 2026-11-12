"""GH #1697: CotedIvoire 2021-22 (EHCVM-2, WB 6273) is wired, under ids of its own.

The first class needs no microdata.  ``TestServed`` builds real tables and carries
``requires_s3``; its ORACLE reads the ``.dta`` files through ``get_dataframe``, imports
nothing from ``CotedIvoire/_/`` or ``CotedIvoire/2021-22/_/``, spells the household id
itself and decodes values from the CODES in the file, with its own tables.
"""
from __future__ import annotations

import ast
import csv
import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.local_tools import df_from_orgfile, get_dataframe
from lsms_library.paths import countries_root

COUNTRY = "CotedIvoire"
WAVE = "2021-22"
ROOT = countries_root() / COUNTRY / WAVE / "Data"
PREFIX = "E2_"
KEY_SALE = f"{COUNTRY}::crop_production::hh-crop-sale-single-row"
KEY_WINDOW = f"{COUNTRY}::crop_production::harvest-to-date"
KEY_TOTAL = f"{COUNTRY}::crop_production::harvest-from-dispositions"


def _raw(name, labels=False):
    return get_dataframe(ROOT / name, convert_categoricals=labels)


def _i(grappe, menage):
    return f"{PREFIX}{int(grappe)}{int(menage):03d}"


def _ids(frame):
    return [_i(g, m) for g, m in zip(frame["grappe"], frame["menage"])]


def _served(table):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return getattr(ll.Country(COUNTRY), table)(waves=[WAVE])


def _keyed(frame, name):
    return frame["Derivation"].astype("string").str.contains(name, regex=False).fillna(False).astype(bool)


#: The five answers of s16cq16d, by code, written out here so a wrong table cannot agree
#: with itself.
CONDITION = {1: "unshelled", 2: "shelled", 3: "shelled", 4: "unshelled", 5: "shell_not_applicable"}
CONDITION_LABEL = {1: "Epis", 2: "Grain", 3: "Décortiqué", 4: "Non-décortiqué", 5: "Non concerné"}


class TestHarvestTotalRule:
    """The shared function on synthetic frames, through the registry key."""

    def _lines(self):
        return pd.DataFrame({
            "i": ["a", "b", "c", "c", "d", "e", "f"],
            "crop": ["x"] * 7,
            "plot_id": ["1_1", "1_1", "1_1", "1_2", "1_1", "1_1", "1_1"],
            "u": pd.array([pd.NA] * 7, dtype="string"),
            "Quantity": [np.nan, np.nan, np.nan, np.nan, np.nan, 9.0, np.nan],
            "finished": ["Oui", "Oui", "Oui", "Oui", "Non", "Oui", "Oui"],
            "Derivation": pd.array([pd.NA] * 7, dtype="string"),
        })

    def _dispositions(self):
        one = {"s16dq02a": 1.0, "s16dq02b": "Kg", "s16dq03a": 2.0, "s16dq03b": "Kg",
               "s16dq05a": 3.0, "s16dq05b": "Kg", "s16dq13a": 0.0, "s16dq13b": pd.NA}
        rows = [dict(i=i, crop="x", **one) for i in "abcdef"]
        rows[1]["s16dq03b"] = "Tas"
        rows.append(dict(one, i="f2", crop="x"))
        frame = pd.DataFrame(rows)
        for var in ("s16dq02b", "s16dq03b", "s16dq05b", "s16dq13b"):
            frame[var] = frame[var].astype("string")
        return frame

    def _run(self):
        from lsms_library.ehcvm import derive_hh_crop_harvest_total

        return derive_hh_crop_harvest_total(self._lines(), self._dispositions(), KEY_TOTAL)

    def test_a_finished_crop_in_one_unit_takes_the_sum_on_its_single_row(self):
        out = self._run()
        row = out[out["i"] == "a"].iloc[0]
        assert row["Quantity"] == 6.0 and row["u"] == "Kg" and row["Derivation"] == KEY_TOTAL

    def test_two_units_stay_na(self):
        out = self._run()
        assert out[out["i"] == "b"]["Quantity"].isna().all()

    def test_two_plots_put_the_sum_on_one_new_farm_level_row(self):
        from lsms_library.ehcvm import FARM_LEVEL

        out = self._run()
        c = out[out["i"] == "c"]
        assert c.loc[c["plot_id"] == FARM_LEVEL, "Quantity"].tolist() == [6.0]
        assert c.loc[c["plot_id"] != FARM_LEVEL, "Quantity"].isna().all()

    def test_an_unfinished_row_and_a_recorded_quantity_block_the_sum(self):
        out = self._run()
        assert out[out["i"] == "d"]["Quantity"].isna().all()
        assert out[out["i"] == "e"]["Quantity"].tolist() == [9.0]

    def test_a_sale_on_the_row_and_the_total_share_the_key_slot(self):
        from lsms_library.derivations import MULTI_SEP
        from lsms_library.ehcvm import derive_hh_crop_harvest_total

        lines = self._lines()
        lines.loc[lines["i"] == "a", "Derivation"] = KEY_SALE
        out = derive_hh_crop_harvest_total(lines, self._dispositions(), KEY_TOTAL)
        assert out[out["i"] == "a"]["Derivation"].iloc[0] == MULTI_SEP.join(sorted([KEY_SALE, KEY_TOTAL]))

    def test_a_second_record_of_a_pair_raises(self):
        from lsms_library.ehcvm import harvest_totals

        frame = self._dispositions()
        frame = pd.concat([frame, frame.iloc[:1]])
        with pytest.raises(ValueError):
            harvest_totals(self._lines(), frame)


class TestDeclared:
    def _label(self):
        table = df_from_orgfile(countries_root() / COUNTRY / "_" / "categorical_mapping.org",
                                name="harmonize_food")
        return dict(zip(table["Original Label"].astype(str).str.strip(),
                        table["Preferred Label"].astype(str).str.strip()))

    def test_a_split_good_does_not_take_2018s_bundle_label(self):
        """#833's rule."""
        label = self._label()
        assert label["31. Autres abats (foie, rognon, tête, patte, cou, gorge, os, testicule, queue, peau, etc.)"] == "Abats (hors tripes)"
        assert label["115. Arachides décortiquées"] == "Arachides décortiquées (hors pilées)"
        assert label["78. Pastèque"] == "Pastèque" and label["79. Melon"] == "Melon"
        assert label["155. Café en poudre (moulu)"] == "Café en poudre"
        bundles = {"Abats et tripes (foie, rognon, etc.)", "Arachides décortiquées ou pilées",
                   "Pastèque, Melon", "Crabes, crevettes et autres fruits de mer",
                   "Aubergine, Courge/Courgette", "Vinaigre /moutarde"}
        prefixed = {k: v for k, v in label.items() if k[:1].isdigit() and ". " in k}
        assert len(prefixed) == 170
        assert not bundles & set(prefixed.values())

    def test_a_respelling_takes_2018s_label(self):
        label = self._label()
        assert label["1. Riz local gros grain"] == "Riz local gros grain court"
        assert label["2. Riz local long grain"] == "Riz local Danané petit grain long"
        assert label["125. Banane plantain"] == "Plantain"
        assert label["134. Sucre poudre"] == label["135. Sucre en morceaux"] == "Sucre (poudre ou morceaux)"

    def test_the_two_crop_spellings_are_2018s_crops(self):
        label = self._label()
        assert label["Cacaoo"] == label["Cacao"]
        assert label["Autre (à préciser)"] == label["Autre"]

    def test_every_script_of_the_wave_uses_the_waves_id(self):
        """A script on 2018-19's bare i would orphan its table from sample."""
        for path in sorted((countries_root() / COUNTRY / WAVE / "_").glob("*.py")):
            if path.name == "mapping.py":
                continue
            source = path.read_text(encoding="utf-8")
            names = {a.name for node in ast.walk(ast.parse(source))
                     if isinstance(node, ast.ImportFrom) and node.module == "mapping"
                     for a in node.names}
            assert "household_ids" in names, path.name

    def test_this_wave_prefixes_and_2018_19_does_not(self):
        fns = ll.Country(COUNTRY)
        new = fns[WAVE].formatting_functions["i"]
        old = fns["2018-19"].formatting_functions["i"]
        assert new(pd.Series([100, 1])) == "E2_100001"
        assert old(pd.Series([100, 1])) == "100001"
        assert new(pd.Series([100, np.nan])) is None

    def test_the_person_id_is_one_number_in_this_wave(self):
        pid = ll.Country(COUNTRY)[WAVE].formatting_functions["pid"]
        assert pid(3) == "3"

    def test_the_region_is_spelled_as_in_2018_19(self):
        fns = ll.Country(COUNTRY)[WAVE].formatting_functions
        stratum = fns["strata_label_upper_region"]
        assert stratum(pd.Series(["gÔh", "Urbain"])) == "GÔH Urbain"
        assert fns["Region"]("poro") == "PORO"
        assert stratum(pd.Series(["AUTONOME D'ABIDJAN", "Rural"])) == "AUTONOME D'ABIDJAN Rural"

    def test_the_unit_loses_its_code_and_keeps_its_words(self):
        fn = ll.Country(COUNTRY)[WAVE].formatting_functions["_unit_label"]
        assert fn("139. Sachet") == "Sachet"
        assert fn("120. Cueillère") == "Cuillère"
        assert fn("109. Bol/Cope") == "Bol/Cope"
        assert fn("136. Sac (25 Kg)") == "Sac (25 Kg)"

    def test_the_condition_table_is_this_files_five_codes(self):
        table = df_from_orgfile(countries_root() / COUNTRY / WAVE / "_" / "categorical_mapping.org",
                                name="harmonize_crop_condition_ehcvm2")
        table["Code"] = table["Code"].astype(int)
        assert dict(zip(table["Code"], table["Preferred Label"])) == CONDITION
        assert dict(zip(table["Code"], table["Source Label"])) == CONDITION_LABEL

    def test_plot_labor_is_not_wired_and_says_why(self):
        assert not (countries_root() / COUNTRY / WAVE / "_" / "plot_labor.py").exists()
        from pathlib import Path

        verdicts = Path(__file__).resolve().parent.parent / ".coder" / "coverage" / "absent_verdicts.csv"
        with open(verdicts, newline="", encoding="utf-8") as f:
            rows = [r for r in csv.DictReader(f)
                    if (r["country"], r["feature"], r["wave"]) == (COUNTRY, "plot_labor", WAVE)]
        assert [r["verdict"] for r in rows] == ["asked-not-distributed"]
        assert "C4" in rows[0]["checks_run"]


@pytest.mark.requires_s3
class TestServed:
    def test_the_ids_cannot_meet_2018_19s(self):
        s = _served("sample")
        assert s.index.get_level_values("i").str.startswith(PREFIX).all()
        c18 = get_dataframe(countries_root() / COUNTRY / "2018-19" / "Data" / "Menage" / "s00_me_CIV2018.dta",
                            convert_categoricals=False)
        bare = {f"{int(g)}{int(m):03d}" for g, m in zip(c18["grappe"], c18["menage"])}
        assert not bare & set(s.index.get_level_values("i"))

    def test_sample_is_the_interviewed_households_and_the_weights(self):
        cover = _raw("s00_me_civ2021.dta")
        weights = _raw("ehcvm_ponderations_civ2021.dta")
        s = _served("sample").droplevel("t")
        interviewed = cover[cover["s00q08"].isin([1, 2])]
        assert len(cover) == 13693 and len(s) == len(interviewed) == len(weights) == 12965
        assert set(s.index) == set(_ids(interviewed)) == set(_ids(weights))
        poids = weights["poids"].astype(float)
        raw = pd.Series((poids / poids.mean()).to_numpy(), index=_ids(weights))
        pd.testing.assert_series_equal(s["weight"].astype(float).sort_index(),
                                       raw.sort_index(), check_names=False)
        pd.testing.assert_series_equal(s["panel_weight"].astype(float).sort_index(),
                                       raw.sort_index(), check_names=False)

    def test_strata_are_2018_19s_labels_by_code(self):
        # GH #1575's region x milieu, extended to this wave on 2026-11-12.  The region is
        # this file's label upper-cased, which is 2018-19's label for the same code.
        new = _raw("s00_me_civ2021.dta", labels=True)
        raw = _raw("s00_me_civ2021.dta")
        old = get_dataframe(countries_root() / COUNTRY / "2018-19" / "Data" / "Menage" / "s00_me_CIV2018.dta",
                            convert_categoricals=True)
        old_cells = set(old["s00q01"].astype(str).str.strip() + " " + old["s00q04"].astype(str).str.strip())
        s = _served("sample").droplevel("t")
        interviewed = raw["s00q08"].isin([1, 2]).to_numpy()
        labels = (new["s00q01"].astype(str).str.strip().str.upper() + " "
                  + new["s00q04"].astype(str).str.strip())
        by_id = dict(zip(_ids(raw[interviewed]), labels[interviewed]))
        assert s["strata"].astype(str).to_dict() == by_id
        # Every served label is a 2018-19 label (no stratum-count golden, GH #1575).
        assert set(s["strata"].astype(str)) <= old_cells
        codes = raw["s00q01"]
        by_code = new["s00q01"].astype(str).groupby(codes.to_numpy()).first()
        old_by_code = old["s00q01"].astype(str).groupby(
            get_dataframe(countries_root() / COUNTRY / "2018-19" / "Data" / "Menage" / "s00_me_CIV2018.dta",
                          convert_categoricals=False)["s00q01"].to_numpy()).first()
        assert {k: str(v).upper() for k, v in by_code.items()} == old_by_code.str.strip().to_dict()
        # This wave's own key (F1 of the GH #1697 red team), not 2018-19's.
        assert (s["Derivation"] == "CotedIvoire::sample::region-milieu-stratum-upper-region").all()

    def test_the_registered_inputs_rederive_the_served_strata(self):
        # The inputs are the file's labels; the registered function upper-cases the region,
        # so it re-derives the served value with nothing added (F1 of the GH #1697 red team).
        key = "CotedIvoire::sample::region-milieu-stratum-upper-region"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            inputs = ll.Country(COUNTRY).derivation_inputs(key, wave=WAVE)
        rec = ll.Country(COUNTRY).derivations()[key]
        assert rec.waves == [WAVE]
        rebuilt = pd.Series([rec.resolve()(r, m) for r, m in zip(inputs["s00q01"], inputs["s00q04"])],
                            index=inputs.index.droplevel("t"))
        served = _served("sample").droplevel("t")["strata"]
        # The cover holds the 728 households not interviewed; the inputs keep them.
        assert len(rebuilt) == 13693 and len(served) == 12965
        assert (rebuilt.reindex(served.index) == served).all()

    def test_the_cluster_point_is_constant_within_a_grappe(self):
        cover = _raw("s00_me_civ2021.dta")
        points = cover.groupby("grappe")[["GPS__Latitude", "GPS__Longitude"]].nunique()
        assert (points == 1).all(axis=None)
        c = _served("cluster_features")
        assert len(c) == cover["grappe"].nunique() == 1081
        expected = cover.groupby("grappe")["GPS__Latitude"].first()
        expected.index = expected.index.astype(int).astype(str)
        got = c.droplevel("t")["Latitude"].astype(float)
        pd.testing.assert_series_equal(got.sort_index(), expected.sort_index(),
                                       check_names=False, check_index_type=False)

    def test_roster_people_sex_and_relationship_are_the_files(self):
        s01 = _raw("s01_me_civ2021.dta")
        r = _served("household_roster").reset_index()
        assert len(r) == len(s01) == 64491
        key = pd.MultiIndex.from_arrays([_ids(s01), s01["membres__id"].astype(int).astype(str)])
        sex = pd.Series(s01["s01q01"].map({1: "M", 2: "F"}).to_numpy(), index=key)
        got = pd.Series(r["Sex"].astype(str).to_numpy(),
                        index=pd.MultiIndex.from_arrays([r["i"], r["pid"]]))
        assert got.sort_index().equals(sex.sort_index())
        assert (r.groupby("i")["Generation"].apply(lambda g: (g == 0).any())).all()

    def test_age_agrees_with_the_surveys_own_derived_age(self):
        ind = _raw("ehcvm_individu_civ2021.dta")
        own = pd.Series(ind["age"].to_numpy(dtype=float),
                        index=pd.MultiIndex.from_arrays([_ids(ind), ind["numind"].astype(int).astype(str)]))
        r = _served("household_roster").reset_index()
        age = pd.Series(pd.to_numeric(r["Age"], errors="coerce").to_numpy(dtype=float),
                        index=pd.MultiIndex.from_arrays([r["i"], r["pid"]]))
        both = pd.concat({"own": own, "age": age}, axis=1).dropna()
        assert len(both) >= 64300
        assert ((both["own"] - both["age"]).abs() <= 1).mean() > 0.98

    def test_people_join_across_tables_but_for_the_files_own_86(self):
        """numind is 1 where membres__id is not, on 86 members; disclosed, not repaired."""
        r = _served("household_roster").reset_index()
        e = _served("individual_education").reset_index()
        roster = set(zip(r["i"], r["pid"]))
        education = set(zip(e["i"], e["pid"]))
        assert len(education - roster) <= 86
        assert {p for _, p in education - roster} == {"1"}

    def test_step_follows_the_label_not_the_code(self):
        s20 = _raw("s20a_me_civ2021.dta")
        step = s20["s20aq05"].map({1: 4, 2: 3, 3: 2, 4: 1})
        expected = pd.Series(step.to_numpy(), index=_ids(s20)).dropna().astype(int)
        got = _served("subjective_well_being").reset_index().set_index("i")["Own Step"]
        pd.testing.assert_series_equal(got.astype(int).sort_index(), expected.sort_index(),
                                       check_names=False, check_dtype=False)

    def test_housing_reads_the_roof_and_floor_of_this_wave(self):
        s11 = _raw("s11_me_civ2021.dta")
        h = _served("housing").reset_index().set_index("i")
        roof = {1: "Concrete", 2: "Clay Tiles", 3: "Iron Sheets", 4: "Thatch", 5: "Mud",
                6: "Thatch", 7: "Mats", 8: "Other", 9: "Plastic"}
        floor = {1: "Tiles", 2: "Cement", 3: "Earth", 4: "Earth with Dung", 5: "Other"}
        for column, source, table in (("Roof", "s11q19", roof), ("Floor", "s11q20", floor)):
            expected = pd.Series(s11[source].map(table).to_numpy(), index=_ids(s11))
            assert h[column].astype(str).sort_index().equals(expected.sort_index())

    def test_assets_are_the_owned_lines_of_the_grid(self):
        s12 = _raw("s12_me_civ2021.dta")
        owned = s12[s12["s12q02"] == 1]
        a = _served("assets")
        answered = owned[owned[["s12q03", "s12q07", "s12q08", "s12q09"]].notna().any(axis=1)]
        assert len(a) == len(answered)
        assert a["Quantity"].astype(float).sum() == pytest.approx(s12["s12q03"].sum())
        assert a["Value"].astype(float).sum() == pytest.approx(s12["s12q09"].sum())

    def _followups(self, labels=False):
        s14 = _raw("s14b_me_civ2021.dta", labels=labels)
        return s14[s14[[f"s14bq04{x}" for x in "abcde"]].notna().any(axis=1)]

    def test_a_shock_row_is_a_followup_of_the_file(self):
        """7,588 follow-ups; 38 have no menage; the rest land on one key per (i, shock)."""
        fu = self._followups()
        assert len(fu) == 7588
        keyed = fu[fu["menage"].notna()]
        assert fu["menage"].isna().sum() == 38
        s = _served("shocks")
        assert s.index.is_unique
        labels = _raw("s14b_me_civ2021.dta", labels=True).loc[keyed.index, "s14bq01"].astype(str)
        expected = set(zip(_ids(keyed), labels))
        got = set(zip(s.index.get_level_values("i"), s.index.get_level_values("Shock")))
        assert got == expected

    def test_a_junk_vague_does_not_cost_a_row(self):
        """`vague` is not read: the 21 rows with 3, 4, 7, 8, 107, 343 keep their household."""
        fu = self._followups()
        junk = fu[fu["vague"].notna() & ~fu["vague"].isin([1, 2]) & fu["menage"].notna()]
        assert len(junk) == 21
        s = _served("shocks")
        served = set(s.index.get_level_values("i"))
        assert set(_ids(junk)) <= served

    @pytest.mark.parametrize("column,letter", [("AffectedConsumption", "f"),
                                               ("AffectedFoodStock", "e")])
    def test_food_effects_read_purchases_and_stock_not_the_herd(self, column, letter):
        """Ruling GH #1556 / #1561: purchases (s14bq04f) and stock (s14bq04e)."""
        fu = self._followups(labels=True)
        fu = fu[fu["menage"].notna()].copy()
        effect = {"Diminué": True, "Augmenté": True, "Inchangé": False, "Non-concerné": False}
        source = fu[f"s14bq04{letter}"].astype(str).map(effect)
        herd = fu["s14bq04d"].astype(str).map(effect)
        assert source.sum() != herd.sum()
        s = _served("shocks")
        unique = ~pd.Series(list(zip(fu["grappe"], fu["menage"], fu["s14bq01"]))).duplicated(keep=False).to_numpy()
        got = pd.Series(s[column].astype(bool).to_numpy(),
                        index=pd.MultiIndex.from_arrays([s.index.get_level_values("i"),
                                                         s.index.get_level_values("Shock")]))
        want = pd.Series(source[unique].astype(bool).to_numpy(),
                         index=pd.MultiIndex.from_arrays([_ids(fu[unique]),
                                                          fu.loc[unique, "s14bq01"].astype(str)]))
        assert got.reindex(want.index).astype(bool).equals(want)

    def test_non_concerne_is_a_code_here_not_a_stata_missing(self):
        """2018-19 stores it as .d, read as NaN; this file has it on code 4 in all five."""
        fu = self._followups()
        for x in "abcde":
            assert fu[f"s14bq04{x}"].notna().all()
            assert set(fu[f"s14bq04{x}"].astype(int)) == {1, 2, 3, 4}

    def test_other_strategy_is_served_though_its_column_says_oui(self):
        fu = self._followups(labels=True)
        fu = fu[fu["menage"].notna()]
        oui = fu[fu["s14bq05__25"].astype(str) == "Oui"]
        assert len(oui) <= 15
        s = _served("shocks")
        assert (s["HowCoped0"].astype("string") == "Autre stratégie").sum() >= len(oui) - 2

    def test_only_column_25_of_the_coping_grid_is_not_ranked(self):
        s14 = _raw("s14b_me_civ2021.dta", labels=True)
        ranks = {"Non", "Première stratégie", "Deuxième strategie", "Troisième stratégie"}
        off = [n for n in range(1, 27)
               if not set(s14[f"s14bq05__{n}"].dropna().astype(str)) <= ranks]
        assert off == [25]

    def test_fies_score_counts_the_yes_answers(self):
        s08 = _raw("s08a_me_civ2021.dta")
        s08 = s08[s08["menage"].notna()]
        items = s08[[f"s08aq0{n}" for n in range(1, 9)]]
        yes = pd.Series((items == 1).sum(axis=1).to_numpy(), index=_ids(s08))
        got = _served("food_security").reset_index().set_index("i")["FIES_score"].astype(int)
        pd.testing.assert_series_equal(got.sort_index(), yes.reindex(got.index).sort_index(),
                                       check_names=False, check_dtype=False)

    def test_interview_dates_are_the_covers_and_no_non_rempli_is_served(self):
        cover = _raw("s00_me_civ2021.dta")
        cover = cover[cover["s00q08"].isin([1, 2])]
        d = _served("interview_date").xs(1, level="visit").reset_index().set_index("i")
        assert set(d.index) == set(_ids(cover))
        expected = pd.Series(pd.to_datetime(cover["s00q23a"]).to_numpy(), index=_ids(cover))
        got = pd.to_datetime(d["Interview start"])
        assert (got.sort_index() == expected.reindex(got.index).sort_index()).all()

    def test_food_acquired_sums_are_the_files(self):
        s07 = _raw("s07b_me_civ2021.dta")
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

    def test_no_purchased_row_is_a_floating_point_residue(self):
        """A row with s07bq04 + s07bq05 = s07bq03a purchased nothing; 12 reach the reshape."""
        f = _served("food_acquired").xs("purchased", level="s")
        assert (f["Quantity"].astype(float) >= 1e-9).all()

    def test_the_consumption_columns_move_together_in_every_region(self):
        """Not Togo's defect: q3/q4 is 1 at the mode on never-purchased own-production rows,
        region by region, and a Jamais row rarely keeps a residual.  What the file does
        instead is in _/CONTENTS.org; this pins that no regional factor sits on s07bq03a."""
        s07 = _raw("s07b_me_civ2021.dta")
        cover = _raw("s00_me_civ2021.dta")[["grappe", "menage", "s00q01"]]
        s07 = s07[s07["s07bq03a"] > 0].merge(cover, on=["grappe", "menage"])
        never = s07[(s07["s07bq06"] == 5) & (s07["s07bq04"] > 0)].copy()
        never["ratio"] = (never["s07bq03a"] / never["s07bq04"]).round(4)
        modes = never.groupby("s00q01")["ratio"].agg(lambda r: r.value_counts().index[0])
        assert len(modes) == 33 and (modes == 1.0).all()
        jamais = s07[s07["s07bq06"] == 5]
        residual = jamais["s07bq03a"] - jamais["s07bq04"].fillna(0) - jamais["s07bq05"].fillna(0)
        assert (residual > 1e-9).mean() < 0.02

    def test_u_loses_the_code_and_keeps_the_size(self):
        f = _served("food_acquired")
        units = set(f.index.get_level_values("u").unique())
        assert not {u for u in units if u[:1].isdigit() and ". " in u}
        assert {"Tas (Petit)", "Sachet (Moyen)", "Unité (Grand)", "Kg", "Litre"} <= units
        assert not {u for u in units if u.startswith(("Kg (", "Litre ("))}

    def test_the_purchase_unit_reads_as_the_code_says(self):
        """s07bq03b and s07bq07b carry one label set here, so labels and codes agree."""
        from lsms_library.ehcvm import unit_match

        labels = _raw("s07b_me_civ2021.dta", labels=True)
        codes = _raw("s07b_me_civ2021.dta")
        keep = codes["s07bq03a"].notna() & codes["s07bq07a"].notna()
        labels, codes = labels[keep], codes[keep]
        by_code = ((codes["s07bq03b"] == codes["s07bq07b"])
                   & (codes["s07bq03c"] == codes["s07bq07c"])).to_numpy()
        got = unit_match(labels["s07bq03b"], labels["s07bq03c"],
                         labels["s07bq07b"], labels["s07bq07c"]).to_numpy()
        assert (got == by_code).all()

    def test_every_item_label_of_the_file_is_mapped(self):
        table = df_from_orgfile(countries_root() / COUNTRY / "_" / "categorical_mapping.org",
                                name="harmonize_food")
        keys = set(table["Original Label"].astype(str).str.strip())
        labels = _raw("s07b_me_civ2021.dta", labels=True)["s07bq01"]
        labels = {str(v).strip() for v in labels.dropna().unique() if isinstance(v, str)}
        assert not labels - keys

    def _harvest_rows(self):
        """Crop rows with an answer.  One row names a crop and answers nothing, not even
        s16cq07, so the framework drops it as all-NA."""
        s16c = _raw("s16c_me_civ2021.dta")
        s16c = s16c[s16c["s16cq04"].notna()]
        assert len(s16c) == 21737 and s16c["s16cq07"].isna().sum() == 1
        return s16c[s16c["s16cq07"].notna()]

    def _records(self):
        s16d = _raw("s16d_me_civ2021.dta")
        return s16d[s16d["s16dq01"].notna()]

    def test_crop_production_is_the_harvest_rows_and_each_sale_once(self):
        s16c = self._harvest_rows()
        s16d = self._records()
        sales = s16d[(s16d["s16dq05a"] > 0) | (s16d["s16dq06"] > 0)]
        assert len(sales) == 8467 and not s16d.duplicated(["grappe", "menage", "s16dq01"]).any()
        cp = _served("crop_production")
        assert cp.index.is_unique
        plots = cp[cp.index.get_level_values("plot_id") != "Farm-level"]
        assert len(plots) == len(s16c) == 21736
        assert cp["Value_sold"].astype(float).sum() == pytest.approx(sales["s16dq06"].sum())
        assert cp["Quantity_sold"].astype(float).sum() == pytest.approx(sales["s16dq05a"].sum())
        assert cp["Value_sold"].notna().sum() == len(sales)

    def test_a_sale_is_on_its_single_row_or_on_one_farm_level_row(self):
        s16c = self._harvest_rows()
        s16d = self._records()
        sales = s16d[(s16d["s16dq05a"] > 0) | (s16d["s16dq06"] > 0)]
        lines = s16c.groupby(["grappe", "menage", "s16cq04"]).size()
        count = lines.reindex(pd.MultiIndex.from_frame(
            sales[["grappe", "menage", "s16dq01"]])).fillna(0).to_numpy()
        cp = _served("crop_production").reset_index()
        farm = cp[(cp["plot_id"] == "Farm-level") & cp["Value_sold"].notna()]
        assert len(farm) == (count > 1).sum() == 792
        assert _keyed(cp, "hh-crop-sale-single-row").sum() == (count == 1).sum() == 7675
        assert (count == 0).sum() == 0

    def test_the_recorded_harvest_is_served_as_recorded_under_the_window_key(self):
        s16c = self._harvest_rows()
        recorded = s16c[s16c["s16cq16a"].notna()]
        assert len(recorded) == 3595 and (recorded["s16cq11"] == 2).all()
        cp = _served("crop_production")
        window = cp[_keyed(cp, "harvest-to-date")]
        assert len(window) == 3595
        assert window["Quantity"].astype(float).sum() == pytest.approx(recorded["s16cq16a"].sum())

    def test_a_finished_crop_takes_its_dispositions_by_the_rule(self):
        """The oracle: the rule stated over raw codes, with its own unit equality on code."""
        s16c = self._harvest_rows()
        s16d = self._records()
        key = ["grappe", "menage", "crop"]
        facts = s16c.assign(crop=s16c["s16cq04"], open=s16c["s16cq11"].ne(1),
                            rec=s16c["s16cq16a"].notna()).groupby(key).agg(
            lines=("open", "size"), open=("open", "any"), rec=("rec", "any"))
        long = pd.concat([pd.DataFrame({"grappe": s16d["grappe"], "menage": s16d["menage"],
                                        "crop": s16d["s16dq01"], "q": s16d[f"{v}a"], "u": s16d[f"{v}b"]})
                          for v in ("s16dq02", "s16dq03", "s16dq05", "s16dq13")])
        long = long[long["q"] > 0]
        stated = long.groupby(key).agg(total=("q", "sum"), units=("u", "nunique"),
                                       unitless=("u", lambda u: u.isna().any()))
        pairs = stated.join(facts, how="inner")
        qualify = pairs[(pairs["units"] == 1) & ~pairs["unitless"] & ~pairs["open"] & ~pairs["rec"]]
        cp = _served("crop_production").reset_index()
        derived = cp[_keyed(cp, "harvest-from-dispositions")]
        assert len(derived) == len(qualify) == 8932
        assert derived["Quantity"].astype(float).sum() == pytest.approx(qualify["total"].sum())
        farm = derived[derived["plot_id"] == "Farm-level"]
        assert len(farm) == (qualify["lines"] > 1).sum() == 556

    def test_the_sold_unit_reads_the_sections_code_list(self):
        s16d = self._records()
        sold = s16d[(s16d["s16dq05a"] > 0) | (s16d["s16dq06"] > 0)]
        cp = _served("crop_production")
        got = cp["Unit_sold"].astype("string").value_counts()
        assert got["Tonne"] == (sold["s16dq05b"] == 339).sum()
        assert got["Tricycle"] == (sold["s16dq05b"] == 340).sum()
        assert got["Cuvette"] == (sold["s16dq05b"] == 316).sum()
        assert not [u for u in got.index if u.replace(".", "").isdigit()]

    def test_a_crop_code_with_no_label_stays_its_number(self):
        s16c = self._harvest_rows()
        cp = _served("crop_production")
        plots = cp[cp.index.get_level_values("plot_id") != "Farm-level"]
        crops = plots.index.get_level_values("crop").to_series()
        for code in (56, 57, 58, 59, 60):
            assert (crops == str(code)).sum() == (s16c["s16cq04"] == code).sum()
        assert (crops == "Cocoa").sum() == (s16c["s16cq04"] == 48).sum()

    def test_the_inputs_join_to_the_keyed_rows(self):
        c = ll.Country(COUNTRY)
        cp = _served("crop_production").reset_index()
        for key, name in ((KEY_SALE, "hh-crop-sale-single-row"), (KEY_WINDOW, "harvest-to-date"),
                          (KEY_TOTAL, "harvest-from-dispositions")):
            raw = c.derivation_inputs(key, wave=WAVE)
            keyed = cp[_keyed(cp, name)]
            assert set(zip(keyed["i"], keyed["crop"])) <= set(raw.index.droplevel("t"))

    def test_condition_and_the_answer_are_the_files_by_code(self):
        s16c = self._harvest_rows()
        cp = _served("crop_production")
        plots = cp[cp.index.get_level_values("plot_id") != "Farm-level"]
        got = plots.index.get_level_values("condition").to_series().value_counts()
        want = s16c["s16cq16d"].map(CONDITION).fillna("unknown_condition").value_counts()
        assert got.sort_index().to_dict() == want.sort_index().to_dict()
        native = plots["condition_native"].astype("string").value_counts(dropna=False)
        answers = s16c["s16cq16d"].map(CONDITION_LABEL).value_counts()
        assert {k: native[k] for k in answers.index} == answers.to_dict()
        assert plots["condition_native"].isna().sum() == s16c["s16cq16d"].isna().sum()

    def test_a_farm_level_row_and_a_derived_total_have_no_state(self):
        cp = _served("crop_production").reset_index()
        for rows in (cp[cp["plot_id"] == "Farm-level"], cp[_keyed(cp, "harvest-from-dispositions")]):
            assert (rows["condition"] == "unknown_condition").all()
            assert rows["condition_native"].isna().all()

    def _inputs(self):
        s16b = _raw("s16b_me_civ2021.dta")
        whole = s16b[s16b["s16bq01"].notna()]
        distinct = whole.groupby(["grappe", "menage"])["s16bq01"].nunique()
        damaged = distinct[distinct < 24].index
        assert len(damaged) == 38
        applied = s16b[s16b["s16bq02"] == 1]
        pair = pd.MultiIndex.from_frame(applied[["grappe", "menage"]])
        return applied, applied[~pair.isin(damaged)], applied[pair.isin(damaged)]

    def test_plot_inputs_read_the_purchase_not_the_gift(self):
        _, clean, damaged = self._inputs()
        p = _served("plot_inputs")
        ids = set(_ids(damaged))
        kept = p[~p.index.get_level_values("i").isin(ids)]
        assert len(kept) == len(clean)
        assert kept["Purchased"].astype("boolean").sum() == (clean["s16bq08"] == 1).sum()
        assert kept["Received_as_gift"].astype("boolean").sum() == (clean["s16bq05"] == 1).sum()
        assert kept["Quantity"].astype(float).sum() == pytest.approx(clean["s16bq03a"].sum())
        assert kept["Quantity_purchased"].astype(float).sum() == pytest.approx(clean["s16bq09a"].sum())
        assert kept["Quantity_received_as_gift"].astype(float).sum() == pytest.approx(clean["s16bq07a"].sum())

    def test_only_the_files_damaged_households_repeat_a_key(self):
        applied, _, damaged = self._inputs()
        rows = len(_served("plot_inputs"))
        assert len(applied) == 14704
        assert len(applied) - rows <= len(damaged)

    def test_the_new_seed_slots_are_not_lost_in_a_catch_all(self):
        _, clean, _ = self._inputs()
        crops = _served("plot_inputs").index.get_level_values("crop").to_series()
        assert (crops == "Cocoa").sum() >= (clean["s16bq01"] == 23).sum()
        assert (crops == "Arachide").sum() >= (clean["s16bq01"] == 19).sum()
        assert (crops == "Autre tubercule").sum() >= (clean["s16bq01"] == 20).sum()

    def test_plot_features_area_prefers_the_measure_and_keeps_every_parcel(self):
        s16a = _raw("s16a_me_civ2021.dta")
        parcels = s16a[(s16a["s16aq02"] > 0) & s16a["s16aq03"].notna()]
        f = _served("plot_features").reset_index()
        assert len(f) == len(parcels) == 15220
        estimate = parcels["s16aq09a"].where(parcels["s16aq09b"] == 1, parcels["s16aq09a"] / 10000)
        measured = (parcels["s16aq45"] == 1) & parcels["s16aq47"].notna()
        area = parcels["s16aq47"].astype(float).where(measured, estimate)
        area = area.where((area > 0) & (area <= 1000))
        assert f["Area"].astype(float).sum() == pytest.approx(area.sum())

    def test_livestock_is_the_answered_grid(self):
        s17 = _raw("s17_me_civ2021.dta")
        owned = s17[s17["s17q03"] == 1]
        got = _served("livestock")
        assert len(got) == len(owned) == 4219
        assert got["HeadCount"].astype(float).sum() == pytest.approx(owned["s17q06"].sum())
        assert got["HeadSold"].astype(float).sum() == pytest.approx(owned["s17q10"].sum())
        cattle = got.xs("Cattle", level="animal")["HeadCount"].astype(float).sum()
        assert cattle == pytest.approx(owned.loc[owned["s17q01"] == 1, "s17q06"].sum())

    def test_people_last7days_carries_every_member(self):
        s04 = _raw("s04a_me_civ2021.dta")
        got = _served("people_last7days")
        assert len(got) == len(s04) == 64491
        assert got["farm_work"].astype("boolean").sum() == (s04["s04q06"] == 1).sum()
        assert got["wage_work"].astype("boolean").sum() == (s04["s04q08"] == 1).sum()

    def test_plot_labor_serves_no_row_of_this_wave(self):
        got = _served("plot_labor")
        assert len(got) == 0
