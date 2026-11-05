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


class TestDeclared:
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
