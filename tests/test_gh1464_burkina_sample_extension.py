"""Burkina Faso 2014 ``sample`` holds every household of the p1 files (GH #1464).

``emc2014_welfare.dta`` holds 10,411 of the 10,800 households of the p1 roster and food
files, and ``v`` joins from ``sample``, so the other 389 came back with NA ``v`` on every
table that gets it there.  Ruling (@ligon, 2026-11-04): extend ``sample`` with them, ``v`` =
``zd``, ``weight`` NA, on Ethiopia's #1359 pattern; do not borrow ``hhweight1``.

``TestHook`` and ``TestWiring`` use invented frames and need no data.  ``TestOracle`` reads
the source files directly and imports nothing from ``Burkina_Faso/_/``.
"""
import importlib.util
import pathlib
import warnings

import numpy as np
import pandas as pd
import pytest
import yaml

import lsms_library as ll
from lsms_library import local_tools
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root


def _tests_conftest():
    """``tests/conftest.py``, loaded by path (the root conftest shadows it)."""
    path = pathlib.Path(__file__).with_name("conftest.py")
    spec = importlib.util.spec_from_file_location("lsms_tests_conftest", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


requires_s3 = _tests_conftest().requires_s3

COUNTRY = "Burkina_Faso"
ROOT = countries_root() / COUNTRY
WELFARE = "emc2014_welfare.dta"
FOOD_P1 = "emc2014_p1_conso7jours_16032015.dta"


def _module():
    spec = importlib.util.spec_from_file_location("_bf_2014_gh1464", ROOT / "2014" / "_" / "mapping.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _hook():
    return _module().sample


def _frame(rows):
    """Rows of (i, v, weight, strata, Rural), the wave frame the hook is handed."""
    flat = pd.DataFrame(rows, columns=["i", "v", "weight", "strata", "Rural"])
    flat.insert(0, "t", "2014")
    flat["panel_weight"] = flat["weight"]
    return flat.set_index(["t", "i"])[["v", "weight", "panel_weight", "strata", "Rural"]]


WELFARE_ROWS = [("a", 1, 2.0, "4", "Rural"), ("b", 2, 6.0, "5", "Urban")]
FOOD_ROWS = [("a", 1, np.nan, "BM_R", "Rural"), ("a", 1, np.nan, "BM_R", "Rural"),
             ("b", 2, np.nan, "BM_U", "Urban")]


# Independent literal oracle transcribed from the sealed own2014 triples (GH1573).
# Old 4/5 <-> BM_R/BM_U fixture values below remain invented pairing controls.
_STRATA_LABELS_1573 = {
    '1': 'Hauts-Bassins Urbain',
    '2': 'Hauts-Bassins Rural',
    '3': 'Boucle du Mouhoun Urbain',
    '4': 'Boucle du Mouhoun Rural',
    '5': 'Sahel Urbain',
    '6': 'Sahel Rural',
    '7': 'Est Urbain',
    '8': 'Est Rural',
    '9': 'Sud-Ouest Urbain',
    '10': 'Sud-Ouest Rural',
    '11': 'Centre-Nord Urbain',
    '12': 'Centre-Nord Rural',
    '13': 'Centre-Ouest Urbain',
    '14': 'Centre-Ouest Rural',
    '15': 'Plateau-Central Urbain',
    '16': 'Plateau-Central Rural',
    '17': 'Nord Urbain',
    '18': 'Nord Rural',
    '19': 'Centre-Est Urbain',
    '20': 'Centre-Est Rural',
    '21': 'Centre Urbain',
    '22': 'Centre Rural',
    '23': 'Cascades Urbain',
    '24': 'Cascades Rural',
    '25': 'Centre-Sud Urbain',
    '26': 'Centre-Sud Rural',
}


class TestHook:
    def test_welfare_rows_stay_first_and_unchanged(self):
        frame = _frame(WELFARE_ROWS + FOOD_ROWS + [("c", 2, np.nan, "BM_U", "Urban")])
        out = _hook()(frame)
        expected = frame.iloc[:2].copy()
        expected["strata"] = ["Boucle du Mouhoun Rural", "Sahel Urbain"]
        pd.testing.assert_frame_equal(out.iloc[:2], expected, check_exact=True)

    def test_an_added_household_is_one_row_with_no_weight(self):
        added = [("c", 2, np.nan, "BM_U", "Urban")] * 3 + [("d", 1, np.nan, "BM_R", "Rural")]
        out = _hook()(_frame(WELFARE_ROWS + FOOD_ROWS + added))
        assert list(out.index.get_level_values("i")) == ["a", "b", "c", "d"]
        assert out["v"].tolist() == [1, 2, 2, 1]
        assert out["weight"].isna().tolist() == [False, False, True, True]
        assert out["panel_weight"].isna().tolist() == [False, False, True, True]
        assert out["Rural"].tolist() == ["Rural", "Urban", "Urban", "Rural"]

    def test_strata_is_spelled_as_the_welfare_file_spells_it(self):
        out = _hook()(_frame(WELFARE_ROWS + FOOD_ROWS + [("c", 2, np.nan, "BM_U", "Urban")]))
        assert out["strata"].tolist() == ["Boucle du Mouhoun Rural", "Sahel Urbain", "Sahel Urbain"]

    def test_no_added_household_returns_the_welfare_rows(self):
        frame = _frame(WELFARE_ROWS + FOOD_ROWS)
        expected = frame.iloc[:2].copy()
        expected["strata"] = ["Boucle du Mouhoun Rural", "Sahel Urbain"]
        pd.testing.assert_frame_equal(_hook()(frame), expected, check_exact=True)

    def test_attrs_survive(self):
        frame = _frame(WELFARE_ROWS + FOOD_ROWS)
        frame.attrs = {"marker": 1}
        assert _hook()(frame).attrs == {"marker": 1}

    @pytest.mark.parametrize("row", [
        ("c", 3, np.nan, "BM_U", "Urban"),    # a second zd
        ("c", 2, np.nan, "BM_U", "Rural"),    # a second milieu
        ("c", 2, np.nan, "BM_R", "Urban"),    # a second strate
        ("c", 2, np.nan, None, "Urban"),      # a populated strate and a missing one
    ])
    def test_a_household_whose_rows_disagree_is_refused(self, row):
        rows = WELFARE_ROWS + FOOD_ROWS + [("c", 2, np.nan, "BM_U", "Urban"), row]
        mod = _module()
        with pytest.raises(mod.EmcSampleError, match="more than one"):
            mod.sample(_frame(rows))

    def test_a_strate_no_shared_household_uses_is_refused(self):
        rows = WELFARE_ROWS + FOOD_ROWS + [("c", 2, np.nan, "XX_U", "Urban")]
        mod = _module()
        with pytest.raises(mod.EmcSampleError, match="never use"):
            mod.sample(_frame(rows))

    def test_a_translation_that_is_not_one_to_one_is_refused(self):
        rows = WELFARE_ROWS + [("a", 1, np.nan, "BM_R", "Rural"), ("b", 2, np.nan, "BM_R", "Urban")]
        mod = _module()
        with pytest.raises(mod.EmcSampleError, match="one-to-one"):
            mod.sample(_frame(rows))

    def test_a_repeated_welfare_household_is_refused(self):
        mod = _module()
        with pytest.raises(mod.EmcSampleError, match="repeats"):
            mod.sample(_frame(WELFARE_ROWS + WELFARE_ROWS[:1]))

    def test_the_refusal_is_none_of_the_types_the_build_paths_catch(self):
        """Written when the v-join and the derived paths swallowed these (GH #1546, #1532)."""
        caught = (FileNotFoundError, KeyError, ValueError, AttributeError, RuntimeError, OSError)
        assert not issubclass(_module().EmcSampleError, caught)

    def test_the_v_join_does_not_swallow_the_refusal(self, monkeypatch):
        mod = _module()
        country = ll.Country(COUNTRY, preload_panel_ids=False)

        def refuse(*args, **kwargs):
            raise mod.EmcSampleError("the two files disagree")

        monkeypatch.setattr(country, "sample", refuse, raising=False)
        frame = pd.DataFrame({"x": [1]}, index=pd.MultiIndex.from_tuples([("a", "2014")], names=["i", "t"]))
        with pytest.raises(mod.EmcSampleError):
            country._join_v_from_sample(frame)


class TestStrataLabels1573:
    """Only own recorded strata labels change; no row or weight construction."""

    def test_the_26_literal_pairs_are_reversible_and_use_current_region_aliases(self):
        mod = _module()
        assert mod._EMC_2014_STRATA_LABELS == _STRATA_LABELS_1573
        assert set(mod._EMC_2014_STRATA_LABELS) == {str(i) for i in range(1, 27)}
        assert len(set(mod._EMC_2014_STRATA_LABELS.values())) == 26
        tables = local_tools.all_dfs_from_orgfile(ROOT / "_" / "categorical_mapping.org")
        preferred = set(tables["Region"]["Preferred Label"])
        for label in _STRATA_LABELS_1573.values():
            region, milieu = label.rsplit(" ", 1)
            assert region in preferred and milieu in {"Urbain", "Rural"}

    def test_all_26_labels_and_every_non_strata_value_and_axis_are_exact(self):
        frame = _frame([(f"h{n}", n, float(n), str(n), "Rural") for n in range(1, 27)])
        frame.attrs = {"id_converted": True, "source_note": {"native": "EMC"}}
        before = frame.copy(deep=True)
        expected = frame.copy(deep=True)
        # The unchanged1464 concat returns object strata at this boundary.
        expected["strata"] = pd.Series(list(_STRATA_LABELS_1573.values()),
                                        index=expected.index, dtype=object)
        out = _hook()(frame)
        pd.testing.assert_frame_equal(out, expected, check_exact=True)
        pd.testing.assert_frame_equal(frame, before, check_exact=True)
        assert out.attrs == before.attrs == expected.attrs
        inverse = {v: k for k, v in _STRATA_LABELS_1573.items()}
        native_at_boundary = pd.Series([str(n) for n in range(1, 27)],
                                       index=before.index, name="strata", dtype=object)
        pd.testing.assert_series_equal(out["strata"].replace(inverse), native_at_boundary, check_exact=True)

    @pytest.mark.parametrize("native", [None, pd.NA, np.nan])
    def test_true_missing_is_not_a_label_or_a_new_household_default(self, native):
        frame = _frame([("a", "1", 2.0, native, "Rural")])
        before = frame.copy(deep=True)
        out = _hook()(frame)
        pd.testing.assert_frame_equal(out, before, check_exact=True)
        pd.testing.assert_frame_equal(frame, before, check_exact=True)

    @pytest.mark.parametrize("native", ["27", "Unknown", "not-listed", 99])
    def test_unknown_nonmissing_values_remain_native_without_coercion(self, native):
        frame = _frame([("a", "1", 2.0, native, "Rural")])
        before = frame.copy(deep=True)
        expected = before.copy(deep=True)
        # Exact-source9f witness: empty-new concat produces object for strings,
        # float64 for this out-of-formatter numeric toy. Decoding changes neither.
        expected["strata"] = pd.Series([99.0] if native == 99 else [native],
                                        index=expected.index,
                                        dtype="float64" if native == 99 else object)
        out = _hook()(frame)
        pd.testing.assert_frame_equal(out, expected, check_exact=True)
        pd.testing.assert_frame_equal(frame, before, check_exact=True)

    def test_actual_scalar_formatter_reaches_string_key_without_new_conversion(self):
        mod = _module()
        assert mod.strata(1) == mod.strata(1.0) == "1"
        frame = _frame([("a", "1", 2.0, mod.strata(1.0), "Urban")])
        expected = frame.copy(deep=True)
        expected["strata"] = pd.Series(["Hauts-Bassins Urbain"],
                                        index=expected.index, dtype=object)
        pd.testing.assert_frame_equal(mod.sample(frame), expected, check_exact=True)

    def test_the_food_proof_precedes_one_shared_and_added_label_decoding(self):
        # Genuine own code3/4 labels; Rural is not used to infer a stratum.
        rows = [("a", "1", 2.0, "3", "Urban"), ("b", "2", 6.0, "4", "Rural"),
                ("a", "1", np.nan, "BM_U", "Urban"),
                ("b", "2", np.nan, "BM_R", "Rural"),
                ("c", "2", np.nan, "BM_R", "Rural"),
                ("c", "2", np.nan, "BM_R", "Rural")]
        frame = _frame(rows)
        frame.attrs = {"id_converted": True, "proof": "kept"}
        before = frame.copy(deep=True)
        expected = _frame(rows[:2] + [("c", "2", np.nan, "4", "Rural")])
        expected["strata"] = ["Boucle du Mouhoun Urbain", "Boucle du Mouhoun Rural",
                               "Boucle du Mouhoun Rural"]
        expected.attrs = dict(frame.attrs)
        out = _hook()(frame)
        pd.testing.assert_frame_equal(out, expected, check_exact=True)
        pd.testing.assert_frame_equal(frame, before, check_exact=True)
        assert out.attrs == expected.attrs
        assert out.loc[("2014", "c"), ["weight", "panel_weight"]].isna().all()

    def test_missing_rural_is_not_filled_when_recorded_stratum_is_decoded(self):
        frame = _frame([("a", "1", 2.0, "1", pd.NA)])
        expected = frame.copy(deep=True)
        expected["strata"] = pd.Series(["Hauts-Bassins Urbain"],
                                        index=expected.index, dtype=object)
        pd.testing.assert_frame_equal(_hook()(frame), expected, check_exact=True)

    def test_existing_labels_are_idempotent_without_a_global_numeric_mapping(self):
        frame = _frame([("a", "1", 2.0, "Hauts-Bassins Urbain", "Urban")])
        expected = frame.copy(deep=True)
        expected["strata"] = pd.Series(["Hauts-Bassins Urbain"],
                                        index=expected.index, dtype=object)
        pd.testing.assert_frame_equal(_hook()(frame), expected, check_exact=True)


class TestWiring:
    def test_a_refusal_reaches_the_caller_of_the_wave_build(self, monkeypatch, tmp_path):
        """Through Wave.grab_data, with invented frames and a private data root."""
        monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path))
        monkeypatch.setenv("LSMS_NO_CACHE", "1")
        frames = {
            WELFARE: pd.DataFrame({"grappe": [2], "menage": [1], "hhweight": [9.0],
                                   "strate": [4], "milieu": ["Rural"]}),
            FOOD_P1: pd.DataFrame({"zd": [2, 3, 3], "menage": [1, 7, 7], "hhweight1": [5.0] * 3,
                                   "strate": ["BM_R"] * 3, "milieu": ["RURAL", "RURAL", "URBAIN"]}),
        }
        monkeypatch.setattr(local_tools, "get_dataframe",
                            lambda path, **kwargs: frames[str(path).split("/Data/")[-1]].copy())
        with pytest.raises(Exception, match="more than one") as caught:
            ll.Country(COUNTRY)["2014"].grab_data("sample")
        assert type(caught.value).__name__ == "EmcSampleError"
        assert not isinstance(caught.value, (ValueError, KeyError, RuntimeError))

    def test_no_passage_weight_is_read(self):
        with open(ROOT / "2014" / "_" / "data_info.yml", encoding="utf-8") as f:
            block = yaml.safe_load(f)["sample"]
        assert block["myvars"]["weight"] == "hhweight"
        assert block["myvars"]["panel_weight"] == "hhweight"
        assert block["file"][0] == WELFARE
        assert set(block["file"][1][FOOD_P1]) == {"i", "v"}

    def test_the_yaml_reaches_the_hook_with_no_weight_on_the_added_file(self, monkeypatch):
        """Through Wave.grab_data, so the file list and its missing column are the real ones.
        The food file carries hhweight1, which must stay out."""
        monkeypatch.setenv("LSMS_NO_CACHE", "1")
        frames = {
            WELFARE: pd.DataFrame({"grappe": [2], "menage": [1], "hhweight": [9.0],
                                   "strate": [4], "milieu": ["Rural"]}),
            FOOD_P1: pd.DataFrame({"zd": [2, 2, 3, 3], "menage": [1, 1, 7, 7],
                                   "hhweight1": [5.0] * 4, "strate": ["BM_R"] * 4,
                                   "milieu": ["RURAL"] * 4}),
        }

        def read(path, **kwargs):
            return frames[str(path).split("/Data/")[-1]].copy()

        monkeypatch.setattr(local_tools, "get_dataframe", read)
        out = ll.Country(COUNTRY)["2014"].grab_data("sample").reset_index().set_index("i")
        assert list(out.index) == ["EMC_2001", "EMC_3007"]
        assert out.loc["EMC_2001", "weight"] == 9.0
        assert pd.isna(out.loc["EMC_3007", "weight"]) and pd.isna(out.loc["EMC_3007", "panel_weight"])
        assert str(out.loc["EMC_3007", "v"]) == "3"
        assert out.loc["EMC_3007", "strata"] == "Boucle du Mouhoun Rural"
        assert out.loc["EMC_3007", "Rural"] == "Rural"


def _emc_id(zd, menage):
    return f"EMC_{int(zd)}{int(menage):03d}"


@pytest.fixture(scope="module")
def raw():
    welfare = get_dataframe(str(ROOT / "2014" / "Data" / WELFARE))
    food = get_dataframe(str(ROOT / "2014" / "Data" / FOOD_P1))
    return welfare, food


@pytest.fixture(scope="module")
def served():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        frame = ll.Country(COUNTRY).sample()
    flat = frame.reset_index()
    flat["t"] = flat["t"].astype(str)
    return flat[flat["t"] == "2014"].set_index("i")


@requires_s3
class TestOracle:
    def test_the_households_are_the_union_of_the_two_files(self, raw, served):
        welfare, food = raw
        expect = ({_emc_id(z, m) for z, m in zip(food["zd"], food["menage"])}
                  | {_emc_id(g, m) for g, m in zip(welfare["grappe"], welfare["menage"])})
        assert set(served.index) == expect and served.index.is_unique

    def test_welfare_households_keep_their_weight_and_the_added_have_none(self, raw, served):
        welfare, food = raw
        ids = [_emc_id(g, m) for g, m in zip(welfare["grappe"], welfare["menage"])]
        raw_weight = pd.Series(welfare["hhweight"].to_numpy(dtype=float), index=ids)
        got = served.loc[ids, "weight"].astype(float)
        np.testing.assert_allclose(got.to_numpy(), (raw_weight / raw_weight.mean()).to_numpy())
        added = served.index.difference(ids)
        assert len(added) == 389
        assert served.loc[added, ["weight", "panel_weight"]].isna().all(axis=None)
        # Not for want of a number: the food file has a passage weight for every one of them.
        keyed = food.assign(i=[_emc_id(z, m) for z, m in zip(food["zd"], food["menage"])])
        assert keyed.loc[keyed["i"].isin(added), "hhweight1"].notna().all()

    def test_an_added_households_v_rural_and_strata_are_its_own(self, raw, served):
        welfare, food = raw
        ids = {_emc_id(g, m) for g, m in zip(welfare["grappe"], welfare["menage"])}
        keyed = food.assign(i=[_emc_id(z, m) for z, m in zip(food["zd"], food["menage"])])
        keyed["strate_code"] = keyed["strate"]
        by_hh = keyed.drop_duplicates("i").set_index("i")
        # The translation, from the households both files hold.
        shared = pd.DataFrame({"i": [_emc_id(g, m) for g, m in zip(welfare["grappe"], welfare["menage"])],
                               "number": welfare["strate"].astype(int).astype(str)})
        shared = shared.merge(by_hh[["strate_code"]], left_on="i", right_index=True).dropna()
        table = shared.drop_duplicates(["number", "strate_code"])
        assert len(table) == 26 and table["number"].is_unique and table["strate_code"].is_unique
        to_number = table.set_index("strate_code")["number"]
        milieu = {"RURAL": "Rural", "URBAIN": "Urban"}
        for i in set(served.index) - ids:
            row = by_hh.loc[i]
            assert str(served.loc[i, "v"]) == str(int(row["zd"])), i
            assert served.loc[i, "Rural"] == milieu[row["milieu"]], i
            assert served.loc[i, "strata"] == _STRATA_LABELS_1573[to_number[row["strate_code"]]], i

    @pytest.mark.parametrize("table", ["household_roster", "food_expenditures", "food_prices",
                                       "food_quantities", "housing", "individual_education"])
    def test_no_2014_row_of_a_p1_table_lacks_v(self, table):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            frame = getattr(ll.Country(COUNTRY), table)(waves=["2014"])
        assert frame.reset_index()["v"].notna().all()

    def test_no_2014_household_is_labelled_a_mover(self):
        """`roster_to_characteristics` fills a NaN `v` with 'Mover', so `v.notna()` cannot
        see this: 379 of the 389 were served `v = 'Mover'`, and none of them moved."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            frame = ll.Country(COUNTRY).household_characteristics(waves=["2014"])
        assert (frame.reset_index()["v"].astype(str) != "Mover").all()

    def test_the_v_it_joins_is_the_one_food_acquired_reads_from_its_own_files(self, served):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fa = ll.Country(COUNTRY).food_acquired(waves=["2014"])
        pairs = fa.reset_index()[["i", "v"]].drop_duplicates().set_index("i")["v"].astype(str)
        assert pairs.index.is_unique
        joined = served["v"].astype(str).reindex(pairs.index)
        assert (joined == pairs).all()
