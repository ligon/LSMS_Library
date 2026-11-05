"""GH #1437: Mali's 2021-22 harvest total, and the harvest-to-date window key.

The instrument sends a finished harvest to the quantity, and the file has it on 22 of
5,237.  Ruling 2026-11-04, option (b): where every s16c row of a household's crop is
finished, none records a quantity and the s16d dispositions share one unit, their sum is
the harvest, on the single row or the crop's Farm-level row, under a registered key.
Separately, a recorded quantity on an unfinished row is a part of the season, and takes an
identity key naming that window, in 2018-19 too.

``TestRules`` and ``TestRegistry`` need no microdata.  ``TestServed`` does
(``requires_s3``); its oracle reads the ``.dta`` files and imports nothing from ``Mali/_/``.
"""
from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.derivations import MULTI_SEP, records_for, resolve_callable
from lsms_library.local_tools import get_dataframe
from lsms_library.paths import countries_root

ROOT = countries_root() / "Mali"
SALE = "Mali::crop_production::hh-crop-sale-single-row"
WINDOW = "Mali::crop_production::harvest-to-date"
TOTAL = "Mali::crop_production::harvest-from-dispositions"
FARM = "Farm-level"
DISPOSITIONS = ("s16dq02", "s16dq03", "s16dq05", "s16dq13")


def _derive():
    return records_for("Mali", "crop_production")[TOTAL].resolve("function")


def _stamp():
    return resolve_callable("lsms_library.countries.Mali._.mali:stamp_harvest_to_date")


def _served(rows):
    """(i, plot_id, crop, Quantity, u, finished, Derivation)"""
    return pd.DataFrame(rows, columns=["i", "plot_id", "crop", "Quantity", "u", "finished",
                                       "Derivation"]).astype(
        {"i": "string", "plot_id": "string", "crop": "string", "Quantity": "float64",
         "u": "string", "finished": "string", "Derivation": "string"})


def _records(rows):
    """(i, crop, consumed, unit, given, unit, sold, unit, stock, unit)"""
    cols = ["i", "crop"] + [f"{v}{p}" for v in DISPOSITIONS for p in "ab"]
    out = pd.DataFrame(rows, columns=cols)
    return out.astype({"i": "string", "crop": "string",
                       **{f"{v}a": "float64" for v in DISPOSITIONS},
                       **{f"{v}b": "string" for v in DISPOSITIONS}})


ONE_UNIT = ("h", "Mil", 2.0, "Kg", 1.0, "Kg", 3.0, "Kg", 4.0, "Kg")


class TestRules:
    def test_the_sum_goes_on_the_single_row_with_its_unit_and_the_key(self):
        got = _derive()(_served([("h", "1_1", "Mil", np.nan, pd.NA, "Oui", pd.NA)]),
                        _records([ONE_UNIT]))
        assert len(got) == 1
        row = got.iloc[0]
        assert (row.plot_id, row.Quantity, row.u, row.Derivation) == ("1_1", 10.0, "Kg", TOTAL)

    def test_a_zero_or_missing_disposition_does_not_count_as_a_unit(self):
        rec = ("h", "Mil", 0.0, "Tonne", np.nan, pd.NA, 3.0, "Kg", 4.0, "Kg")
        got = _derive()(_served([("h", "1_1", "Mil", np.nan, pd.NA, "Oui", pd.NA)]), _records([rec]))
        assert got.Quantity.tolist() == [7.0] and got.u.tolist() == ["Kg"]

    def test_two_rows_mean_one_new_farm_level_row_and_no_split(self):
        got = _derive()(_served([("h", "1_1", "Mil", np.nan, pd.NA, "Oui", pd.NA),
                                 ("h", "2_1", "Mil", np.nan, pd.NA, "Oui", pd.NA)]),
                        _records([ONE_UNIT]))
        assert len(got) == 3
        farm = got[got.plot_id == FARM].iloc[0]
        assert (farm.Quantity, farm.u, farm.Derivation) == (10.0, "Kg", TOTAL)
        assert got[got.plot_id != FARM].Quantity.isna().all()

    def test_a_farm_level_sale_row_takes_the_total_rather_than_a_second_row(self):
        served = _served([("h", "1_1", "Mil", np.nan, pd.NA, "Oui", pd.NA),
                          ("h", "2_1", "Mil", np.nan, pd.NA, "Oui", pd.NA),
                          ("h", FARM, "Mil", np.nan, pd.NA, pd.NA, pd.NA)])
        served["Value_sold"] = [np.nan, np.nan, 300.0]
        got = _derive()(served, _records([ONE_UNIT]))
        farm = got[got.plot_id == FARM]
        assert len(farm) == 1
        assert farm.Quantity.tolist() == [10.0] and farm.Value_sold.tolist() == [300.0]
        assert farm.Derivation.tolist() == [TOTAL]

    def test_a_sale_key_on_the_single_row_is_joined_sorted(self):
        got = _derive()(_served([("h", "1_1", "Mil", np.nan, pd.NA, "Oui", SALE)]),
                        _records([ONE_UNIT]))
        assert got.Derivation.tolist() == [MULTI_SEP.join(sorted((SALE, TOTAL)))]

    @pytest.mark.parametrize("rows", [
        [("h", "1_1", "Mil", np.nan, pd.NA, "Non", pd.NA)],                # unfinished
        [("h", "1_1", "Mil", np.nan, pd.NA, "Oui", pd.NA),
         ("h", "2_1", "Mil", np.nan, pd.NA, "Non", pd.NA)],                # mixed
        [("h", "1_1", "Mil", 9.0, "Kg", "Oui", pd.NA)],                    # recorded
        [("h", "1_1", "Mil", 9.0, "Kg", "Oui", pd.NA),
         ("h", "2_1", "Mil", np.nan, pd.NA, "Oui", pd.NA)],                # recorded beside
        [("h", "1_1", "Mil", np.nan, pd.NA, pd.NA, pd.NA)],                # unanswered
        [("h", "1_1", "Mil", np.nan, pd.NA, "Oui", pd.NA),
         ("h", "2_1", "Mil", np.nan, pd.NA, pd.NA, pd.NA)],                # unanswered beside
    ])
    def test_what_is_held_out_keeps_what_it_had(self, rows):
        served = _served(rows)
        got = _derive()(served, _records([ONE_UNIT]))
        assert len(got) == len(served)
        assert got.Derivation.isna().all()
        pd.testing.assert_series_equal(got.Quantity, served.Quantity, check_names=False)

    def test_two_units_stay_na(self):
        rec = ("h", "Mil", 2.0, "Kg", 0.0, pd.NA, 3.0, "Sac (100 Kg)", np.nan, pd.NA)
        got = _derive()(_served([("h", "1_1", "Mil", np.nan, pd.NA, "Oui", pd.NA)]), _records([rec]))
        assert got.Quantity.isna().all() and got.u.isna().all() and got.Derivation.isna().all()

    def test_no_disposition_stays_na_and_no_zero_is_written(self):
        rec = ("h", "Mil", 0.0, pd.NA, 0.0, pd.NA, np.nan, pd.NA, np.nan, pd.NA)
        got = _derive()(_served([("h", "1_1", "Mil", np.nan, pd.NA, "Oui", pd.NA)]), _records([rec]))
        assert got.Quantity.isna().all()

    def test_a_crop_with_no_s16d_record_stays_na(self):
        got = _derive()(_served([("h", "1_1", "Mil", np.nan, pd.NA, "Oui", pd.NA)]),
                        _records([("g",) + ONE_UNIT[1:]]))
        assert got.Quantity.isna().all() and len(got) == 1

    def test_the_window_key_marks_an_unfinished_row_with_a_quantity_and_nothing_else(self):
        got = _stamp()(_served([("h", "1_1", "Mil", 5.0, "Kg", "Non", pd.NA),
                                ("h", "2_1", "Mil", 5.0, "Kg", "Non", SALE),
                                ("h", "3_1", "Mil", np.nan, pd.NA, "Non", pd.NA),
                                ("h", "4_1", "Mil", 5.0, "Kg", "Oui", pd.NA),
                                ("h", "5_1", "Mil", np.nan, pd.NA, "Recolte nulle", pd.NA)]))
        assert got.Quantity.tolist()[:2] == [5.0, 5.0]
        assert got.Derivation.tolist()[0] == WINDOW
        assert got.Derivation.tolist()[1] == MULTI_SEP.join(sorted((SALE, WINDOW)))
        assert got.Derivation[2:].isna().all()


class TestRegistry:
    @pytest.mark.parametrize("key,waves,columns", [
        # Quantity_kg joined in GH #1436: on an unfinished row it covers the same window.
        (WINDOW, ["2018-19", "2021-22"], {"Quantity", "Quantity_kg"}),
        (TOTAL, ["2021-22"], {"Quantity", "u"}),
    ])
    def test_registered_and_resolves(self, key, waves, columns):
        rec = records_for("Mali", "crop_production")[key]
        assert list(rec.waves) == waves and set(rec.columns) == columns
        assert callable(rec.resolve("function")) and callable(rec.resolve("inputs"))

    def test_the_window_function_is_the_identity(self):
        fn = records_for("Mali", "crop_production")[WINDOW].resolve("function")
        q = pd.Series([1.5, np.nan])
        assert fn(q) is q


def _household(frame):
    return (frame["grappe"].astype(int).astype(str) + "0"
            + frame["menage"].astype(int).astype(str).str.zfill(2))


def _plot(frame):
    return frame["s16cq02"].astype(int).astype(str) + "_" + frame["s16cq03"].astype(int).astype(str)


@pytest.mark.requires_s3
class TestServed:
    @pytest.fixture(scope="class")
    def flat(self):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return ll.Country("Mali").crop_production().reset_index()

    @pytest.fixture(scope="class")
    def w21(self, flat):
        return flat[flat.t == "2021-22"].astype({"i": str, "plot_id": str, "crop": str})

    @pytest.fixture(scope="class")
    def oracle(self):
        """(i, crop code) -> the pair's facts, from the two files directly."""
        path = ROOT / "2021-22" / "Data"
        c = get_dataframe(str(path / "s16c_me_mli2021.dta"), convert_categoricals=False)
        c_lab = get_dataframe(str(path / "s16c_me_mli2021.dta"))
        d = get_dataframe(str(path / "s16d_me_mli2021.dta"), convert_categoricals=False)
        lines = pd.DataFrame({"i": _household(c).values, "crop": c["s16cq04"].values,
                              "finished": c_lab["s16cq11"].astype("string").str.strip().values,
                              "q": pd.to_numeric(c["s16cq16a"]).values})
        rows = []
        for key, group in lines.groupby(["i", "crop"]):
            rows.append({"i": key[0], "crop": key[1], "n": len(group),
                         "all_finished": (group.finished == "Oui").all(),
                         "recorded": group.q.notna().any()})
        pairs = pd.DataFrame(rows).set_index(["i", "crop"])
        stated = {}
        for rec, i_ in zip(d.itertuples(), _household(d)):
            parts = [(float(getattr(rec, f"{v}a")), getattr(rec, f"{v}b")) for v in DISPOSITIONS
                     if pd.notna(getattr(rec, f"{v}a")) and float(getattr(rec, f"{v}a")) > 0]
            stated[(i_, rec.s16dq01)] = parts
        pairs["units"] = [len({u for _, u in stated.get(k, [])}) for k in pairs.index]
        pairs["total"] = [sum(q for q, _ in stated.get(k, [])) for k in pairs.index]
        pairs["eligible"] = pairs.all_finished & ~pairs.recorded & pairs.units.eq(1)
        return pairs

    def test_the_keyed_rows_are_exactly_the_eligible_pairs(self, w21, oracle):
        keyed = w21[w21.Derivation.astype("string").str.contains(TOTAL, regex=False).fillna(False)]
        assert len(keyed) == int(oracle.eligible.sum())
        assert not keyed.duplicated(["i", "crop"]).any()
        # On the single row, or Farm-level for two or more.
        assert int((keyed.plot_id == FARM).sum()) == int((oracle.eligible & oracle.n.gt(1)).sum())

    def test_the_sum_of_kept_quantities_is_the_sum_of_dispositions(self, w21, oracle):
        keyed = w21[w21.Derivation.astype("string").str.contains(TOTAL, regex=False).fillna(False)]
        assert np.isclose(float(keyed.Quantity.sum()), float(oracle.loc[oracle.eligible, "total"].sum()))

    def test_every_recorded_quantity_is_as_the_file_has_it(self, w21):
        path = ROOT / "2021-22" / "Data" / "s16c_me_mli2021.dta"
        c = get_dataframe(str(path), convert_categoricals=False)
        recorded = pd.DataFrame({"i": _household(c).values, "plot_id": _plot(c).values,
                                 "q": pd.to_numeric(c["s16cq16a"]).astype("float64").values})
        recorded = recorded[recorded.q.notna()]
        served = w21[~w21.Derivation.astype("string").str.contains(TOTAL, regex=False).fillna(False)
                     & w21.Quantity.notna()]
        assert len(served) == len(recorded)
        assert np.isclose(float(served.Quantity.sum()), float(recorded.q.sum()))

    @pytest.mark.parametrize("wave,name,finished,quantity", [
        ("2018-19", "s16c_me_mli2018.dta", "s16cq10", "s16cq12a"),
        ("2021-22", "s16c_me_mli2021.dta", "s16cq11", "s16cq16a"),
    ])
    def test_the_window_key_is_on_exactly_the_unfinished_recorded_rows(self, flat, wave, name,
                                                                        finished, quantity):
        path = str(ROOT / wave / "Data" / name)
        c = get_dataframe(path)
        n = int((c[finished].astype("string").str.strip().eq("Non")
                 & pd.to_numeric(c[quantity], errors="coerce").notna()).sum())
        rows = flat[flat.t == wave]
        has = rows.Derivation.astype("string").str.contains(WINDOW, regex=False).fillna(False)
        assert int(has.sum()) == n
        assert rows.loc[has, "Quantity"].notna().all()

    def test_a_row_never_carries_both_harvest_keys(self, flat):
        d = flat.Derivation.astype("string").fillna("")
        assert not (d.str.contains(WINDOW, regex=False) & d.str.contains(TOTAL, regex=False)).any()

    @pytest.mark.parametrize("key,wave,keys", [
        (WINDOW, "2018-19", ["t", "i", "plot_id", "crop"]),
        (WINDOW, "2021-22", ["t", "i", "plot_id", "crop"]),
        (TOTAL, "2021-22", ["t", "i", "crop"]),
    ])
    def test_derivation_inputs_join_the_keyed_rows(self, flat, key, wave, keys):
        inputs = ll.Country("Mali").derivation_inputs(key, wave=wave).reset_index()
        rows = flat[(flat.t == wave)
                    & flat.Derivation.astype("string").str.contains(key, regex=False).fillna(False)]
        joined = rows.astype({k: str for k in keys}).merge(
            inputs.astype({k: str for k in keys}), on=keys, validate="one_to_one")
        assert len(joined) == len(rows) == len(inputs)
