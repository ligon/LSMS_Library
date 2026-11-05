"""GH #552: Benin 2021-22 (EHCVM-2, WB 6272) is wired, under ids of its own.

``TestDeclared``, ``TestHarvestRule`` and ``TestCopingHook`` need no microdata.
``TestServed`` builds real tables and carries ``requires_s3``; its ORACLE reads the
``.dta`` files through ``get_dataframe``, imports nothing from ``Benin/_/`` or
``Benin/2021-22/_/``, spells the household id itself and decodes values from the CODES
in the file, with its own tables.
"""
from __future__ import annotations

import ast
import importlib.util
import warnings

import numpy as np
import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library.local_tools import df_from_orgfile, get_dataframe
from lsms_library.paths import countries_root

WAVE = "2021-22"
OLD = "2018-19"
ROOT = countries_root() / "Benin" / WAVE / "Data"
OLD_ROOT = countries_root() / "Benin" / OLD / "Data"
PREFIX = "E2_"
KEY = "Benin::crop_production::{}"


def _raw(name, labels=False, root=ROOT):
    return get_dataframe(root / name, convert_categoricals=labels)


def _i(grappe, menage):
    return f"{PREFIX}{int(grappe)}{int(menage):03d}"


def _ids(frame):
    return [_i(g, m) for g, m in zip(frame["grappe"], frame["menage"])]


def _served(table):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return getattr(ll.Country("Benin"), table)(waves=[WAVE])


def _members(s01):
    """The people still in the household: s01q00aa is 2 (non) for those who have left."""
    return s01[s01["s01q00aa"] != 2]


def _mapping_module():
    path = countries_root() / "Benin" / WAVE / "_" / "mapping.py"
    spec = importlib.util.spec_from_file_location("benin_2021_mapping_for_test", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


#: s16cq16d, by code, written out here so a wrong table cannot agree with itself.
CONDITION = {1: "unshelled", 2: "shelled", 3: "shell_not_applicable"}
CONDITION_LABEL = {1: "Epis/coques", 2: "Grains", 3: "na"}
#: What the file's s07bq01 labels are served as, where a bundle of 2018-19's splits.
SPLIT_GOODS = {"Fromage": "Fromage (hors soja)",
               "Arachides décortiquées": "Arachides décortiquées (hors pilées)",
               "Piment frais": "Piment frais (hors séché)"}
FIRST, SECOND, THIRD = "Première stratégie", "Deuxième strategie", "Troisième stratégie"


class TestDeclared:
    def test_a_split_good_does_not_take_2018s_bundle_label(self):
        """#833's rule.  2018-19's Piment covers fresh and dried; unlike Togo's it earns no
        exception, because no price separates them and the row counts add up."""
        table = df_from_orgfile(countries_root() / "Benin" / "_" / "categorical_mapping.org",
                                name="harmonize_food")
        label = dict(zip(table["Original Label"].astype(str).str.strip(),
                         table["Preferred Label"].astype(str).str.strip()))
        for raw, served in SPLIT_GOODS.items():
            assert label[raw] == served
        assert label["Fromage local (Amon)"] == "Fromage"
        assert label["Piment"] == "Piment frais"
        assert label["Fromage à base de soja"] == "Fromage à base de soja"
        assert label["Arachides pilées"] == "Arachides pilées"
        assert label["Piment séché"] == "Piment séché"
        assert label["Sucre poudre"] == label["Sucre morceaux"] == "Sucre (poudre ou morceaux)"

    def test_every_script_of_the_wave_uses_the_waves_id(self):
        """A script on 2018-19's bare i would orphan its table from sample."""
        for path in sorted((countries_root() / "Benin" / WAVE / "_").glob("*.py")):
            if path.name == "mapping.py":
                continue
            source = path.read_text(encoding="utf-8")
            names = {a.name for node in ast.walk(ast.parse(source))
                     if isinstance(node, ast.ImportFrom) and node.module == "mapping"
                     for a in node.names}
            assert names & {"household_ids", "ID_PREFIX", "sale_records"}, path.name

    def test_this_wave_prefixes_and_2018_does_not(self):
        c = ll.Country("Benin")
        new = c[WAVE].formatting_functions["i"]
        old = c[OLD].formatting_functions["i"]
        assert new(pd.Series([100, 1])) == "E2_100001"
        assert old(pd.Series([100, 1])) == "100001"

    def test_each_unit_variable_names_the_code_only_the_other_labels(self):
        m = _mapping_module()
        assert m.ConsumptionUnitCode(102.0) == "Alvéole"
        assert m.LastPurchaseUnitCode(207.0) == "Chaîne en bois"
        assert m.ConsumptionUnitCode("Tas") == "Tas"
        assert pd.isna(m.LastPurchaseUnitCode(np.nan))

    def test_the_consumption_effect_reads_the_food_stock(self):
        info = yaml.safe_load((countries_root() / "Benin" / WAVE / "_" / "data_info.yml")
                              .read_text(encoding="utf-8"))
        assert info["shocks"]["myvars"]["AffectedConsumption"][0] == "s14bq04e"

    def test_the_waves_condition_table_is_its_own_three_codes(self):
        table = df_from_orgfile(countries_root() / "Benin" / WAVE / "_" / "categorical_mapping.org",
                                name="harmonize_crop_condition_ehcvm2")
        got = {int(c): str(t).strip() for c, t in zip(table["Code"], table["Preferred Label"])}
        assert got == CONDITION


class TestCopingHook:
    """Niger's hook after GH #1556, on synthetic rows through this wave's own function."""

    def _served(self, **cope):
        m = _mapping_module()
        row = {f"Cope{n}": "Non" for n in (1, 2, 3, 25, 26)}
        row.update({k.replace("c", "Cope"): v for k, v in cope.items()})
        out = m.shocks(pd.DataFrame([row]))
        assert not [c for c in out.columns if c.startswith("Cope")]
        return [out[f"HowCoped{k}"].iloc[0] for k in range(3)], m.COPING_LABELS

    def test_ranks_order_the_slots_not_the_codes(self):
        got, lab = self._served(c1=SECOND, c2=FIRST)
        assert got[:2] == [lab[2], lab[1]] and pd.isna(got[2])

    def test_a_third_strategy_has_a_slot(self):
        got, lab = self._served(c1=FIRST, c2=SECOND, c3=THIRD)
        assert got == [lab[1], lab[2], lab[3]]

    def test_autre_alone_is_served(self):
        got, lab = self._served(c25="Oui")
        assert got[0] == lab[25] and pd.isna(got[1])

    def test_autre_fills_the_rank_no_other_strategy_holds(self):
        got, lab = self._served(c1=FIRST, c25="Oui")
        assert got[:2] == [lab[1], lab[25]] and pd.isna(got[2])
        got, lab = self._served(c1=SECOND, c25="Oui")
        assert got[0] == lab[25] and got[1] == lab[1]

    def test_non_and_blank_are_not_strategies(self):
        got, _ = self._served(c1=np.nan, c25="Non")
        assert all(pd.isna(x) for x in got)


class TestHarvestRule:
    """The shared EHCVM-2 functions on synthetic frames."""

    TOTAL = KEY.format("harvest-from-dispositions")

    def _served_frame(self):
        return pd.DataFrame({
            "i": ["a", "b", "b", "c", "d", "e", "f"],
            "crop": ["x"] * 7,
            "plot_id": ["1_1", "1_1", "1_2", "1_1", "1_1", "1_1", "1_1"],
            "u": pd.array([pd.NA, pd.NA, pd.NA, "Kg", pd.NA, pd.NA, pd.NA], dtype="string"),
            "Quantity": [np.nan, np.nan, np.nan, 7.0, np.nan, np.nan, np.nan],
            "finished": ["Oui", "Oui", "Oui", "Oui", "Oui", "Non", "Oui"],
            "Derivation": pd.array([pd.NA] * 7, dtype="string"),
        })

    def _dispositions(self):
        rows = {"a": (1.0, "Kg", 2.0, "Kg"), "b": (3.0, "Sac", 4.0, "Sac"),
                "c": (5.0, "Kg", np.nan, None), "d": (1.0, "Kg", 1.0, "Tas"),
                "e": (6.0, "Kg", np.nan, None)}
        out = pd.DataFrame({"i": list(rows), "crop": "x"})
        for pos, var in enumerate(("s16dq02", "s16dq05")):
            out[f"{var}a"] = [v[2 * pos] for v in rows.values()]
            out[f"{var}b"] = pd.array([v[2 * pos + 1] for v in rows.values()], dtype="string")
        for var in ("s16dq03", "s16dq13"):
            out[f"{var}a"] = np.nan
            out[f"{var}b"] = pd.array([pd.NA] * len(rows), dtype="string")
        return out

    def _derive(self):
        from lsms_library.ehcvm import derive_hh_crop_harvest_total
        return derive_hh_crop_harvest_total(self._served_frame(), self._dispositions(), self.TOTAL)

    def test_a_single_row_takes_the_sum_and_the_unit(self):
        row = self._derive().query("i == 'a'").iloc[0]
        assert row["Quantity"] == 3.0 and row["u"] == "Kg" and row["Derivation"] == self.TOTAL

    def test_several_rows_send_it_to_one_farm_level_row(self):
        from lsms_library.ehcvm import FARM_LEVEL
        b = self._derive().query("i == 'b'")
        farm = b[b["plot_id"] == FARM_LEVEL]
        assert len(farm) == 1 and farm["Quantity"].iloc[0] == 7.0 and farm["u"].iloc[0] == "Sac"
        assert b[b["plot_id"] != FARM_LEVEL]["Quantity"].isna().all()

    def test_a_recorded_harvest_is_not_overridden(self):
        c = self._derive().query("i == 'c'")
        assert len(c) == 1 and c["Quantity"].iloc[0] == 7.0 and pd.isna(c["Derivation"].iloc[0])

    def test_two_units_stay_na(self):
        d = self._derive().query("i == 'd'")
        assert d["Quantity"].isna().all() and d["Derivation"].isna().all()

    def test_an_unfinished_row_holds_the_crop_out(self):
        e = self._derive().query("i == 'e'")
        assert e["Quantity"].isna().all()

    def test_no_disposition_stays_na(self):
        f = self._derive().query("i == 'f'")
        assert f["Quantity"].isna().all() and f["Derivation"].isna().all()


def _dedup(s16d):
    return s16d[~s16d.duplicated()]


@pytest.mark.requires_s3
class TestServed:
    def test_the_ids_cannot_meet_2018s(self):
        s = _served("sample")
        assert s.index.get_level_values("i").str.startswith(PREFIX).all()
        old = _raw("s00_me_ben2018.dta", root=OLD_ROOT)
        bare = {f"{int(g)}{int(m):03d}" for g, m in zip(old["grappe"], old["menage"])}
        assert not bare & set(s.index.get_level_values("i"))

    def test_sample_is_the_cover_and_the_weights(self):
        cover = _raw("s00_me_ben2021.dta")
        weights = _raw("ehcvm_ponderations_ben2021.dta")
        s = _served("sample").droplevel("t")
        assert set(s.index) == set(_ids(cover))
        assert len(s) == len(cover)
        region = dict(enumerate(["alibori", "atacora", "atlantique", "borgou", "collines", "couffo",
                                 "donga", "littoral", "mono", "oueme", "plateau", "zou"], start=1))
        by_id = dict(zip(_ids(cover), cover["s00q01"].map(region)))
        assert s["strata"].astype(str).to_dict() == by_id
        rural = dict(zip(_ids(cover), cover["s00q04"].map({1: "Urban", 2: "Rural"})))
        assert s["Rural"].astype(str).to_dict() == rural
        # Weights come back at mean 1 in the wave, each household keeping its share.
        raw = pd.Series(weights["poids"].to_numpy() / weights["poids"].mean(), index=_ids(weights))
        pd.testing.assert_series_equal(s["weight"].astype(float).sort_index(),
                                       raw.sort_index(), check_names=False)

    def test_the_cluster_point_is_a_cluster_attribute(self):
        cover = _raw("s00_me_ben2021.dta")
        points = cover.groupby("grappe")[["GPS__Latitude", "GPS__Longitude"]].nunique()
        assert (points == 1).all(axis=None)
        c = _served("cluster_features")
        assert len(c) == cover["grappe"].nunique()
        expected = cover.groupby("grappe")["GPS__Latitude"].first().astype(float)
        got = c.droplevel("t")["Latitude"].astype(float)
        got.index = got.index.astype(int)
        expected.index = expected.index.astype(int)
        pd.testing.assert_series_equal(got.sort_index(), expected.sort_index(),
                                       check_names=False, check_index_type=False)

    def test_those_who_have_left_say_why_and_nothing_else_and_are_not_served(self):
        s01 = _raw("s01_me_ben2021.dta")
        gone = s01[s01["s01q00aa"] == 2]
        assert len(gone) > 0
        assert gone[["s01q01", "s01q02", "s01q03c", "s01q04a", "s01q12"]].isna().all(axis=None)
        assert gone[["s01q00b", "s01q00c"]].notna().mean().min() > 0.99
        r = _served("household_roster").reset_index()
        kept = _members(s01)
        assert len(r) == len(kept) == len(_raw("ehcvm_individu_ben2021.dta"))
        served = set(zip(r["i"], r["pid"].astype(str)))
        assert served == set(zip(_ids(kept), kept["membres__id"].astype(int).astype(str)))

    def test_roster_sex_and_relationship_are_the_files(self):
        s01 = _members(_raw("s01_me_ben2021.dta"))
        r = _served("household_roster").reset_index()
        key = pd.MultiIndex.from_arrays([_ids(s01), s01["membres__id"].astype(int).astype(str)])
        sex = pd.Series(s01["s01q01"].map({1: "M", 2: "F"}).to_numpy(), index=key)
        got = pd.Series(r["Sex"].astype(str).to_numpy(),
                        index=pd.MultiIndex.from_arrays([r["i"], r["pid"].astype(str)]))
        assert got.sort_index().equals(sex.sort_index())
        assert (r["Relationship"].astype(str) == "Chef de ménage").sum() == (s01["s01q02"] == 1).sum()

    def test_age_follows_the_answers_and_not_the_surveys_own_age(self):
        """The served Age is the file's birth year; ehcvm_individu.age is not, on 3,662 panel
        members, where it is 2018-19's age + 3.  Togo's and Niger's have no such rows."""
        s01 = _members(_raw("s01_me_ben2021.dta"))
        key = pd.MultiIndex.from_arrays([_ids(s01), s01["membres__id"].astype(int).astype(str)])
        year = pd.Series(pd.to_numeric(s01["s01q03c"], errors="coerce").astype(float).to_numpy(), index=key)
        r = _served("household_roster").reset_index()
        age = pd.Series(pd.to_numeric(r["Age"], errors="coerce").astype(float).to_numpy(),
                        index=pd.MultiIndex.from_arrays([r["i"], r["pid"].astype(str)]))
        both = pd.concat({"year": year, "age": age}, axis=1).dropna()
        assert len(both) >= len(s01) - 50
        # The interview runs into 2022 and the birth month may be past: within 2 of 2021 - y.
        assert ((2021 - both["year"]) - both["age"]).abs().max() <= 2

        ind = _raw("ehcvm_individu_ben2021.dta")
        own = pd.Series(pd.to_numeric(ind["age"], errors="coerce").astype(float).to_numpy(),
                        index=pd.MultiIndex.from_arrays([_ids(ind), ind["numind"].astype(int).astype(str)]))
        joined = pd.concat({"own": own, "age": age}, axis=1).dropna()
        assert ((joined["own"] - joined["age"]).abs() <= 1).mean() > 0.9

    def test_people_join_across_tables(self):
        r = _served("household_roster").reset_index()
        e = _served("individual_education").reset_index()
        p = _served("people_last7days").reset_index()
        people = set(zip(r["i"], r["pid"]))
        assert people == set(zip(e["i"], e["pid"])) == set(zip(p["i"], p["pid"]))

    def test_step_follows_the_label_not_the_code(self):
        s20 = _raw("s20a_me_ben2021.dta")
        # The file's codes run the other way from the served step; 5 is Ne sait pas.
        step = s20["s20aq05"].map({1: 4, 2: 3, 3: 2, 4: 1})
        expected = pd.Series(step.to_numpy(), index=_ids(s20)).dropna().astype(int)
        got = _served("subjective_well_being").reset_index().set_index("i")["Own Step"]
        pd.testing.assert_series_equal(got.astype(int).sort_index(), expected.sort_index(),
                                       check_names=False, check_dtype=False)

    def test_housing_reads_the_roof_and_floor_of_this_wave(self):
        s11 = _raw("s11_me_ben2021.dta")
        h = _served("housing").reset_index().set_index("i")
        roof = {1: "Concrete", 2: "Clay Tiles", 3: "Iron Sheets", 4: "Thatch", 5: "Mud",
                6: "Thatch", 7: "Mats", 8: "Other"}
        expected = pd.Series(s11["s11q19"].map(roof).to_numpy(), index=_ids(s11))
        assert h["Roof"].astype(str).sort_index().equals(expected.sort_index())
        floor = {1: "Tiles", 2: "Cement", 3: "Earth", 4: "Earth with Dung", 5: "Other"}
        expected = pd.Series(s11["s11q20"].map(floor).to_numpy(), index=_ids(s11))
        assert h["Floor"].astype(str).sort_index().equals(expected.sort_index())

    def test_assets_are_the_owned_lines_of_the_grid(self):
        s12 = _raw("s12_me_ben2021.dta")
        owned = s12[s12["s12q02"] == 1]
        a = _served("assets")
        assert len(a) == len(owned) < len(s12)
        assert a["Quantity"].astype(float).sum() == pytest.approx(owned["s12q03"].sum())
        assert a["Value"].astype(float).sum() == pytest.approx(owned["s12q09"].sum())
        bikes = owned[owned["s12q01"] == 30]
        got = a.xs("Bicycle", level="j")["Quantity"].astype(float).sum()
        assert got == pytest.approx(bikes["s12q03"].sum())

    def test_a_shock_row_is_a_shock_with_follow_ups(self):
        s14 = _raw("s14b_me_ben2021.dta")
        followed = s14[s14["s14bq04a"].notna()]
        assert ((s14["s14bq02"] == 1) | s14["s14bq04a"].isna()).all()
        got = _served("shocks")
        assert len(got) == len(followed) < (s14["s14bq02"] == 1).sum()
        assert set(got.index.get_level_values("i")) == set(_ids(followed))

    def test_the_consumption_effect_is_the_food_stock_not_the_herd(self):
        s14 = _raw("s14b_me_ben2021.dta")
        answered = s14[s14["s14bq04a"].notna()]
        effect = {1: True, 2: True, 3: False, 4: False}
        food = answered["s14bq04e"].map(effect).astype(bool)
        herd = answered["s14bq04d"].map(effect).astype(bool)
        got = _served("shocks")["AffectedConsumption"].astype(bool)
        assert got.sum() == food.sum()
        assert food.sum() != herd.sum()

    def test_the_labels_are_what_the_wiring_assumes(self):
        from lsms_library.data_access import get_data_file
        path = get_data_file(f"Benin/{WAVE}/Data/s14b_me_ben2021.dta")
        with pd.io.stata.StataReader(str(path)) as reader:
            labels = reader.variable_labels()
        assert "cheptel" in labels["s14bq04d"].lower()
        assert "stock" in labels["s14bq04e"].lower()

    def test_only_column_25_of_the_coping_grid_is_not_ranked(self):
        s14 = _raw("s14b_me_ben2021.dta", labels=True)
        ranks = {"Non", FIRST, SECOND, THIRD}
        off = [n for n in range(1, 27)
               if not set(s14[f"s14bq05__{n}"].dropna().astype(str)) <= ranks]
        assert off == [25]

    def test_the_slots_are_the_ranks_and_keep_code_25(self):
        s14 = _raw("s14b_me_ben2021.dta")
        answered = s14[s14["s14bq04a"].notna()]
        copes = [f"s14bq05__{n}" for n in range(1, 27)]
        # In the file's codes 1, 2, 3 are the ranks and 4 is Non; column 25 has its own set.
        labels = get_dataframe(ROOT / "s14b_me_ben2021.dta", categories_only=True)
        rank_code = {code: label for code, label in dict(labels["s14bq05__1"]).items()}
        rank_of = {code: {FIRST: 1, SECOND: 2, THIRD: 3}[label]
                   for code, label in rank_code.items() if label in (FIRST, SECOND, THIRD)}
        oui = next(code for code, label in dict(labels["s14bq05__25"]).items() if label == "Oui")
        slots = [0, 0, 0]
        autre = [0, 0, 0]
        for row in answered[copes].to_numpy():
            chosen = [(rank_of[v], n) for n, v in enumerate(row, start=1)
                      if n != 25 and v in rank_of]
            if row[24] == oui:
                held = {r for r, _ in chosen}
                chosen.append((min(set(range(1, len(held) + 2)) - held), 25))
            for slot, (_, n) in enumerate(sorted(chosen)):
                slots[slot] += 1
                autre[slot] += n == 25
        got = _served("shocks")
        assert [int(got[f"HowCoped{k}"].notna().sum()) for k in range(3)] == slots
        assert [int((got[f"HowCoped{k}"] == "Autre stratégie").sum()) for k in range(3)] == autre
        assert sum(autre) == (answered["s14bq05__25"] == oui).sum()
        assert slots[2] > 0

    def test_fies_score_counts_the_yes_answers(self):
        s08 = _raw("s08a_me_ben2021.dta")
        items = s08[[f"s08aq0{n}" for n in range(1, 9)]]
        yes = pd.Series((items == 1).sum(axis=1).to_numpy(), index=_ids(s08))
        got = _served("food_security").reset_index().set_index("i")["FIES_score"].astype(int)
        pd.testing.assert_series_equal(got.sort_index(), yes.reindex(got.index).sort_index(),
                                       check_names=False, check_dtype=False)

    def test_interview_dates_are_the_covers(self):
        cover = _raw("s00_me_ben2021.dta")
        d = _served("interview_date").xs(1, level="visit").reset_index().set_index("i")
        expected = pd.Series(pd.to_datetime(cover["s00q23a"]).to_numpy(), index=_ids(cover))
        got = pd.to_datetime(d["Interview start"])
        assert (got.sort_index() == expected.reindex(got.index).sort_index()).all()

    # ---- food

    def _s07(self):
        s07 = _raw("s07b_me_ben2021.dta")
        return s07[s07["s07bq03a"].notna()]

    def test_the_consumption_quantity_is_not_rescaled(self):
        """Togo 2021-22 carries a regional factor in s07bq03a that nothing else shows.  The
        tests that catch one: the quarter grid, q3/q4 on own production, and Jamais rows."""
        s07 = self._s07()
        q3 = s07["s07bq03a"].astype(float)
        assert ((q3 * 4) % 1 == 0).mean() > 0.95
        own = s07[(s07["s07bq04"] > 0) & (s07["s07bq06"] == 5)]
        cover = _raw("s00_me_ben2021.dta")[["grappe", "menage", "s00q01"]]
        ratio = own.merge(cover, on=["grappe", "menage"]).assign(
            r=lambda d: (d["s07bq03a"].astype(float) / d["s07bq04"].astype(float)).round(4))
        modes = ratio.groupby("s00q01")["r"].agg(lambda s: s.mode().iloc[0])
        assert (modes == 1.0).all()
        never = s07[s07["s07bq06"] == 5]
        residual = never["s07bq03a"].astype(float) - never["s07bq04"].astype(float).fillna(0) \
            - never["s07bq05"].astype(float).fillna(0)
        assert (residual > 1e-9).mean() < 0.01

    def test_food_acquired_sums_are_the_files(self):
        s07 = self._s07()
        for col in ("s07bq03a", "s07bq04", "s07bq05", "s07bq07a", "s07bq08"):
            s07 = s07.assign(**{col: s07[col].astype(float)})
        qty = (s07["s07bq03a"] - s07["s07bq04"].fillna(0) - s07["s07bq05"].fillna(0)).clip(lower=0)
        same = ((s07["s07bq03b"] == s07["s07bq07b"]) & (s07["s07bq03c"] == s07["s07bq07c"])
                & (s07["s07bq07a"] > 0))
        value = (qty * s07["s07bq08"] / s07["s07bq07a"]).where(same)
        f = _served("food_acquired")
        side = f.groupby(level="s")[["Quantity", "Expenditure"]].sum()
        assert side.loc["purchased", "Quantity"] == pytest.approx(qty.sum())
        assert side.loc["purchased", "Expenditure"] == pytest.approx(value.sum())
        assert side.loc["produced", "Quantity"] == pytest.approx(s07["s07bq04"].fillna(0).sum())
        assert side.loc["inkind", "Quantity"] == pytest.approx(s07["s07bq05"].fillna(0).sum())
        assert f.loc[f.index.get_level_values("s") != "purchased", "Expenditure"].isna().all()
        assert f.index.is_unique

    def test_the_purchase_unit_reads_as_the_code_says(self):
        """Without the formatters, a code with no label reads as another unit."""
        from lsms_library.ehcvm import unit_match

        labels = _raw("s07b_me_ben2021.dta", labels=True)
        codes = _raw("s07b_me_ben2021.dta")
        keep = codes["s07bq03a"].notna().to_numpy()
        labels, codes = labels[keep], codes[keep]
        by_code = ((codes["s07bq03b"] == codes["s07bq07b"])
                   & (codes["s07bq03c"] == codes["s07bq07c"])).to_numpy()
        m = _mapping_module()
        first = labels["s07bq03b"].astype(object).map(m.ConsumptionUnitCode)
        last = labels["s07bq07b"].astype(object).map(m.LastPurchaseUnitCode)
        got = unit_match(first, labels["s07bq03c"], last, labels["s07bq07c"]).to_numpy()
        assert (got == by_code).all()
        raw = unit_match(labels["s07bq03b"].astype(object), labels["s07bq03c"],
                         labels["s07bq07b"].astype(object), labels["s07bq07c"]).to_numpy()
        assert (raw != by_code).sum() > 0

    def test_unit_labels_agree_with_2018s_and_no_code_leaks(self):
        f = _served("food_acquired")
        units = set(f.index.get_level_values("u").unique())
        bases = {u.split(" (")[0] for u in units}
        assert {"Sachet", "Tas", "Unité", "Boîte", "Morceau", "Panier", "Tohoungolo",
                "Alvéole/Plateau", "Chaîne en bois"} <= bases
        assert not bases & {"sachet", "unité", "panier", "Boite", "Tongolo", "litre", "102"}
        assert not {u for u in units if u[:1].isdigit()}

    def test_the_brassee_units_take_2018s_sized_labels(self):
        """Codes 217 and 218 respell what 2018-19 serves as a size of Tohoungolo."""
        s07 = self._s07()
        for code, label in ((217, "Tohoungolo (Avec 1 brassée)"), (218, "Tohoungolo (Avec 2 brassées)")):
            rows = s07[s07["s07bq03b"] == code]
            assert (rows["s07bq03c"] == 0).all() and len(rows) > 0
            f = _served("food_acquired").xs(label, level="u")
            purchased = f.xs("purchased", level="s")["Quantity"].astype(float).sum()
            net = (rows["s07bq03a"].astype(float) - rows["s07bq04"].astype(float).fillna(0)
                   - rows["s07bq05"].astype(float).fillna(0)).clip(lower=0)
            assert purchased == pytest.approx(net.sum())

    def test_every_item_label_of_the_file_is_mapped(self):
        table = df_from_orgfile(countries_root() / "Benin" / "_" / "categorical_mapping.org",
                                name="harmonize_food")
        keys = set(table["Original Label"].astype(str).str.strip())
        labels = set(_raw("s07b_me_ben2021.dta", labels=True)["s07bq01"].astype(str).str.strip())
        assert not labels - keys

    # ---- crop_production

    def _s16c(self):
        return _raw("s16c_me_ben2021.dta")

    def _sales(self):
        s16d = _dedup(_raw("s16d_me_ben2021.dta"))
        return s16d[(s16d["s16dq05a"] > 0) | (s16d["s16dq06"] > 0)]

    def test_s16d_joins_s16c_by_code_and_not_by_label(self):
        c = self._s16c()
        d = _dedup(_raw("s16d_me_ben2021.dta"))
        code_c = set(zip(c["grappe"], c["menage"], c["s16cq04"].astype(int)))
        code_d = set(zip(d["grappe"], d["menage"], d["s16dq01"].astype(int)))
        assert code_d <= code_c
        lc = _raw("s16c_me_ben2021.dta", labels=True)
        ld = _raw("s16d_me_ben2021.dta", labels=True).loc[d.index]
        label_c = set(zip(lc["grappe"], lc["menage"], lc["s16cq04"].astype(str)))
        label_d = set(zip(ld["grappe"], ld["menage"], ld["s16dq01"].astype(str)))
        assert label_d - label_c

    def test_a_sale_is_on_its_single_row_or_on_one_farm_level_row(self):
        c = self._s16c()
        sales = self._sales()
        lines = c.groupby(["grappe", "menage", "s16cq04"]).size()
        count = lines.reindex(pd.MultiIndex.from_frame(
            sales[["grappe", "menage", "s16dq01"]])).fillna(0).to_numpy()
        assert (count == 0).sum() == 0
        cp = _served("crop_production").reset_index()
        assert cp["Value_sold"].astype(float).sum() == pytest.approx(sales["s16dq06"].astype(float).sum())
        assert cp["Quantity_sold"].astype(float).sum() == pytest.approx(sales["s16dq05a"].astype(float).sum())
        assert cp["Value_sold"].notna().sum() == len(sales)
        farm = cp[(cp["plot_id"] == "Farm-level") & cp["Value_sold"].notna()]
        assert len(farm) == (count > 1).sum()
        keyed = cp["Derivation"].astype("string").str.contains("hh-crop-sale-single-row",
                                                               regex=False).fillna(False)
        assert keyed.sum() == (count == 1).sum()

    def _oracle_totals(self):
        """The dispositions rule from the codes, by hand: (grappe, menage, crop) -> sum."""
        c = self._s16c()
        d = _dedup(_raw("s16d_me_ben2021.dta"))
        facts = c.assign(open=c["s16cq11"] != 1, recorded=c["s16cq16a"].notna()).groupby(
            ["grappe", "menage", "s16cq04"]).agg(lines=("open", "size"), open=("open", "any"),
                                                 recorded=("recorded", "any"))
        long = pd.concat([pd.DataFrame({
            "grappe": d["grappe"].to_numpy(), "menage": d["menage"].to_numpy(),
            "s16cq04": d["s16dq01"].to_numpy(),
            "q": pd.to_numeric(d[f"{v}a"], errors="coerce").astype(float).to_numpy(),
            "u": pd.to_numeric(d[f"{v}b"], errors="coerce").astype(float).to_numpy()})
            for v in ("s16dq02", "s16dq03", "s16dq05", "s16dq13")])
        long = long[long["q"] > 0]
        stated = long.groupby(["grappe", "menage", "s16cq04"]).agg(
            q=("q", "sum"), units=("u", "nunique"), nu=("u", lambda s: s.isna().any()))
        pairs = facts.join(stated, how="inner")
        return pairs[(pairs["units"] == 1) & ~pairs["nu"] & ~pairs["open"] & ~pairs["recorded"]]

    def test_the_harvest_total_is_the_dispositions_where_the_rule_holds(self):
        totals = self._oracle_totals()
        c = self._s16c()
        cp = _served("crop_production").reset_index()
        keyed = cp["Derivation"].astype("string").str.contains("harvest-from-dispositions",
                                                               regex=False).fillna(False)
        assert keyed.sum() == len(totals)
        assert (cp.loc[keyed, "plot_id"] == "Farm-level").sum() == (totals["lines"] > 1).sum()
        assert cp.loc[keyed, "Quantity"].astype(float).sum() == pytest.approx(totals["q"].sum())
        # Every other Quantity is the file's, and no recorded quantity moved.
        assert cp.loc[~keyed, "Quantity"].astype(float).sum() == pytest.approx(
            c["s16cq16a"].astype(float).sum())
        assert (cp.loc[keyed, "u"] != "Unknown").all()
        assert cp.index.is_unique

    def test_a_plot_row_is_a_harvest_row(self):
        c = self._s16c()
        cp = _served("crop_production").reset_index()
        plots = cp[cp["plot_id"] != "Farm-level"]
        assert len(plots) == len(c)
        fields = [f"{int(a)}_{int(b)}" for a, b in zip(c["s16cq02"], c["s16cq03"])]
        assert set(zip(plots["i"], plots["plot_id"])) == set(zip(_ids(c), fields))

    def test_the_window_key_is_on_the_unfinished_rows_that_record_a_quantity(self):
        c = self._s16c()
        expected = ((c["s16cq11"] == 2) & c["s16cq16a"].notna()).sum()
        cp = _served("crop_production")
        keyed = cp["Derivation"].astype("string").str.contains("harvest-to-date",
                                                               regex=False).fillna(False)
        assert keyed.sum() == expected > 0

    def test_the_inputs_join_to_the_keyed_rows(self):
        c = ll.Country("Benin")
        cp = _served("crop_production").reset_index()
        for name in ("hh-crop-sale-single-row", "harvest-to-date", "harvest-from-dispositions"):
            raw = c.derivation_inputs(KEY.format(name), wave=WAVE)
            keyed = cp[cp["Derivation"].astype("string").str.contains(name, regex=False).fillna(False)]
            assert set(zip(keyed["i"], keyed["crop"])) <= set(raw.index.droplevel("t")), name

    def test_condition_and_the_answer_are_the_files_by_code(self):
        c = self._s16c()
        cp = _served("crop_production").reset_index()
        stated = cp[cp["condition_native"].notna()]
        want = c["s16cq16d"].dropna().astype(int)
        assert stated["condition"].value_counts().to_dict() == want.map(CONDITION).value_counts().to_dict()
        assert (stated["condition_native"].astype("string").value_counts().to_dict()
                == want.map(CONDITION_LABEL).value_counts().to_dict())
        assert (cp.loc[cp["condition_native"].isna(), "condition"] == "unknown_condition").all()

    def test_a_derived_or_farm_level_row_has_no_state(self):
        cp = _served("crop_production").reset_index()
        derived = cp["Derivation"].astype("string").str.contains("harvest-from-dispositions",
                                                                 regex=False).fillna(False)
        farm = cp["plot_id"] == "Farm-level"
        assert (cp.loc[derived | farm, "condition"] == "unknown_condition").all()
        assert cp.loc[derived | farm, "condition_native"].isna().all()

    # ---- the other agriculture tables

    def test_plot_inputs_read_the_purchase_not_the_gift(self):
        s16b = _raw("s16b_me_ben2021.dta")
        s16b = s16b[s16b["s16bq02"] == 1]
        p = _served("plot_inputs")
        assert len(p) == len(s16b)
        assert p["Purchased"].astype("boolean").sum() == (s16b["s16bq08"] == 1).sum()
        assert p["Received_as_gift"].astype("boolean").sum() == (s16b["s16bq05"] == 1).sum()
        assert p["Quantity"].astype(float).sum() == pytest.approx(s16b["s16bq03a"].astype(float).sum())
        assert p["Quantity_purchased"].astype(float).sum() == pytest.approx(s16b["s16bq09a"].astype(float).sum())
        assert p["Quantity_received_as_gift"].astype(float).sum() == pytest.approx(s16b["s16bq07a"].astype(float).sum())
        # The two seed slots 2018-19's table did not name are not lost in a catch-all.
        crops = p.index.get_level_values("crop").to_series()
        assert crops.eq("Arachide").sum() == (s16b["s16bq01"] == 19).sum()
        assert crops.eq("Hévéa, teck").sum() == (s16b["s16bq01"] == 21).sum()

    def test_a_unit_code_with_no_label_stays_its_code(self):
        s16b = _raw("s16b_me_ben2021.dta")
        s16b = s16b[s16b["s16bq02"] == 1]
        labels = get_dataframe(ROOT / "s16b_me_ben2021.dta", categories_only=True)["s16bq03b"]
        assert 6 not in {int(k) for k in labels}
        units = _served("plot_inputs").index.get_level_values("u").to_series()
        assert units.eq("6").sum() == (s16b["s16bq03b"] == 6).sum() > 0

    def test_plot_features_area_prefers_the_measure_under_the_rule_of_2018(self):
        s16a = _raw("s16a_me_ben2021.dta")
        f = _served("plot_features").reset_index()
        assert len(f) == len(s16a)
        q47 = pd.to_numeric(s16a["s16aq47"], errors="coerce").astype(float)
        q9 = pd.to_numeric(s16a["s16aq09a"], errors="coerce").astype(float)
        estimate = q9.where(s16a["s16aq09b"] != 2, q9 / 10000)
        area = q47.where((s16a["s16aq45"] == 1) & q47.notna(), estimate)
        area = area.where((area > 0) & (area <= 1000))
        assert f["Area"].astype(float).sum() == pytest.approx(area.sum())

    def test_plot_labor_totals_are_the_grids(self):
        s16a = _raw("s16a_me_ben2021.dta")
        family = s16a.filter(regex=r"^s16aq(33|35|37)b_\d+$").astype(float).fillna(0).to_numpy().sum()
        hired = 0.0
        wage = 0.0
        for base in ("s16aq39", "s16aq41", "s16aq43"):
            for n in range(1, 5):
                hired += (s16a[f"{base}a_{n}"].astype(float) * s16a[f"{base}b_{n}"].astype(float)).sum()
                wage += s16a[f"{base}c_{n}"].astype(float).fillna(0).sum()
        got = _served("plot_labor").groupby(level="source")[["PersonDays", "Wage"]].sum()
        assert got.loc["family", "PersonDays"] == pytest.approx(family)
        assert got.loc["hired", "PersonDays"] == pytest.approx(hired)
        assert got.loc["hired", "Wage"] == pytest.approx(wage)

    def test_livestock_is_the_owned_lines_and_keeps_the_unlabelled_code(self):
        s17 = _raw("s17_me_ben2021.dta")
        owned = s17[s17["s17q03"] == 1]
        got = _served("livestock")
        assert len(got) == len(owned) < len(s17)
        assert got["HeadCount"].astype(float).sum() == pytest.approx(owned["s17q06"].astype(float).sum())
        assert got["HeadSold"].astype(float).sum() == pytest.approx(owned["s17q10"].astype(float).sum())
        cattle = got.xs("Cattle", level="animal")["HeadCount"].astype(float).sum()
        assert cattle == pytest.approx(owned.loc[owned["s17q01"] == 1, "s17q06"].astype(float).sum())
        labels = get_dataframe(ROOT / "s17_me_ben2021.dta", categories_only=True)["s17q01"]
        assert 12 not in {int(k) for k in labels}
        assert got.index.get_level_values("animal").to_series().eq("12").sum() == (owned["s17q01"] == 12).sum()

    def test_people_last7days_carries_the_members_only(self):
        s04 = _raw("s04a_me_ben2021.dta")
        s01 = _raw("s01_me_ben2021.dta")
        gone = s01.loc[s01["s01q00aa"] == 2, ["grappe", "menage", "membres__id"]]
        merged = s04.merge(gone.assign(_gone=True), on=["grappe", "menage", "membres__id"], how="left")
        kept = merged[merged["_gone"].isna()]
        assert merged.loc[merged["_gone"].notna(), ["s04q06", "s04q07", "s04q08"]].isna().all(axis=None)
        got = _served("people_last7days")
        assert len(got) == len(kept) < len(s04)
        assert got["farm_work"].astype("boolean").sum() == (kept["s04q06"] == 1).sum()
        assert got["wage_work"].astype("boolean").sum() == (kept["s04q08"] == 1).sum()
