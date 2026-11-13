"""Served-wave corrections of 2026-11-12 (rulings @ligon 2026-11-12).

- Mali 2018-19: the rice-ration block is withheld (GH #1707, "Remove, registered").
- Senegal 2021-22: salt at 0.0199997 kg x hhsize is withheld ("Remove, registered").
- Senegal 2018-19: two quantities below 1e-3 are served NA (GH #1519, "Exact-value NA rule").
- Niger 2018-19: an off-grid s07bq07a is flagged and served (GH #1708, "Flag now, diagnose"),
  and the purchase it values is revalued at a local median ("Revalue at local median").
- Uganda 2009-10: a missing ea is filled from the same household (GH #1536, "Fill, registered").

Three tiers: synthetic frames through the real wave modules (no microdata);
``TestServed``, which pins the SERVED tables (red team of 2026-11-12, F2: the
selector pins alone stayed green with a hook unwired); and ``TestHeld``, which
reads the raw files through ``get_dataframe`` and pins the counts the rulings
were made on.  Each module is loaded by path, as the framework loads it; none
reads data at import.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library.derivations import derivation_records, resolve_callable
from lsms_library.paths import countries_root

KEYS = {
    "Mali": ["Mali::food_acquired::rice-ration-block-withheld"],
    "Senegal": ["Senegal::food_acquired::salt-per-capita-withheld",
                "Senegal::food_acquired::near-zero-quantity-withheld"],
    "Niger": ["Niger::food_acquired::computed-last-purchase-quantity",
              "Niger::food_acquired::flagged-purchase-local-median"],
    "Uganda": ["Uganda::sample::ea-same-household"],
}


def _load(country, wave, name):
    path = countries_root() / country / wave / "_" / name
    spec = importlib.util.spec_from_file_location(f"_t1707_{country}_{wave}_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(countries_root() / country / "_"))
    try:
        spec.loader.exec_module(module)
    finally:
        sys.path.pop(0)
    return module


@pytest.fixture(scope="module")
def mali():
    return _load("Mali", "2018-19", "mapping.py")


@pytest.fixture(scope="module")
def senegal21():
    return _load("Senegal", "2021-22", "mapping.py")


@pytest.fixture(scope="module")
def senegal18():
    return _load("Senegal", "2018-19", "mapping.py")


@pytest.fixture(scope="module")
def niger():
    return _load("Niger", "2018-19", "mapping.py")


def _load_country_module(country, name):
    path = countries_root() / country / "_" / name
    spec = importlib.util.spec_from_file_location(f"_t1707_{country}_{path.stem}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def uganda():
    return _load_country_module("Uganda", "uganda.py")


class TestRegistry:
    @pytest.mark.parametrize("country,key", [(c, k) for c, ks in KEYS.items() for k in ks])
    def test_registered_and_resolvable(self, country, key):
        rec = derivation_records(country)[key]
        assert callable(resolve_callable(rec.function))
        assert callable(resolve_callable(rec.inputs))

    @pytest.mark.parametrize("key", ["Mali::food_acquired::rice-ration-block-withheld",
                                     "Senegal::food_acquired::salt-per-capita-withheld",
                                     "Senegal::food_acquired::near-zero-quantity-withheld"])
    def test_withholdings_are_rows_zero(self, key):
        assert derivation_records(key.split("::")[0])[key].rows == 0

    def test_salt_declares_no_household_size_file(self):
        """Red team F1 (2026-11-12): the build reads s07b alone, so the welfare file
        is not a build input of food_acquired and is not declared as one."""
        from lsms_library.yaml_utils import load_yaml
        info = load_yaml(countries_root() / "Senegal" / "2021-22" / "_" / "data_info.yml")
        assert "household_size" not in info["food_acquired"]
        assert "ehcvm_welfare" not in str(info["food_acquired"])


def _mali_raw(**over):
    nan = np.nan
    base = dict(vague=nan, s07bq01=1, s07bq03a=1.4 * 5, s07bq03b=100, s07bq03c=0,
                s07bq04=nan, s07bq05=nan, s07bq07a=nan, s07bq08=nan)
    rows = [{**base, **over}, dict(base, vague=1.0, s07bq03a=2.0)]
    return pd.DataFrame(rows)


class TestMaliRule:
    def test_the_block_is_the_missing_vague(self, mali):
        assert mali.rice_ration_rows(_mali_raw()).tolist() == [True, False]

    def test_a_multiple_of_another_household_size_is_still_the_block(self, mali):
        assert mali.rice_ration_rows(_mali_raw(s07bq03a=1.4 * 9)).tolist() == [True, False]

    @pytest.mark.parametrize("over", [{"s07bq03a": 2.5}, {"s07bq01": 2}, {"s07bq03b": 101},
                                      {"s07bq03c": 1}, {"s07bq04": 1.0}, {"s07bq05": 0.5},
                                      {"s07bq07a": 1.0}, {"s07bq08": 250.0}])
    def test_a_record_without_the_fingerprint_stops_the_build(self, mali, over):
        with pytest.raises(mali.RiceRationBlockError):
            mali.rice_ration_rows(_mali_raw(**over))

    def test_the_error_is_not_a_generic_build_failure(self, mali):
        assert not issubclass(mali.RiceRationBlockError, (ValueError, RuntimeError, KeyError))


class TestSenegalRules:
    def test_salt_is_a_whole_multiple_of_the_constant(self, senegal21):
        c = 0.0199997
        q = [c * 5, 0.02 * 5, c * 6, c * 5, c * 5 + 1e-6, c * 0.5, 0.0, c * 12]
        prod = [139, 139, 139, 1, 139, 139, 139, 139]
        raw = pd.DataFrame({"s07bq01": prod, "s07bq03b": [100] * 8, "s07bq03c": [0] * 8,
                            "s07bq03a": q})
        assert senegal21.salt_per_capita_rows(raw).tolist() == [
            True, False, True, False, False, False, False, True]

    @pytest.mark.parametrize("over", [{"s07bq03b": 101}, {"s07bq03c": 1}, {"s07bq01": 138}])
    def test_salt_outside_its_cell_is_never_selected(self, senegal21, over):
        raw = pd.DataFrame([{"s07bq01": 139, "s07bq03b": 100, "s07bq03c": 0,
                             "s07bq03a": 0.0199997 * 4, **over}])
        assert not senegal21.salt_per_capita_rows(raw).any()

    def test_the_build_reads_s07b_alone(self, senegal21, monkeypatch):
        """Red team F1: the build path must not read the welfare file, so that it
        cannot fail when that file is unavailable.  The hook's own reads are recorded."""
        c = 0.0199997
        fake = pd.DataFrame({"s07bq01": [139, 1], "s07bq03b": [100, 100], "s07bq03c": [0, 0],
                             "s07bq03a": [c * 3, 2.0]})
        read = []

        def get_dataframe(path, *a, **k):
            read.append(str(path))
            if "ehcvm_welfare" in str(path):
                raise FileNotFoundError(path)
            return fake.copy()
        monkeypatch.setattr(senegal21.tools, "get_dataframe", get_dataframe)
        df = pd.DataFrame({"Quantity": fake["s07bq03a"].to_numpy()},
                          index=pd.MultiIndex.from_arrays([["a", "b"], ["Sel", "Riz"]],
                                                          names=["i", "j"]))
        out = senegal21._withhold_salt_per_capita_rows(df)
        assert [p.rsplit("/", 1)[-1] for p in read] == ["s07b_me_sen2021.dta"]
        assert out.index.get_level_values("j").tolist() == [senegal21._WITHHELD_J, "Riz"]

    def test_near_zero_needs_both_quantities(self, senegal18):
        q3 = [4.2e-19, 4.2e-19, 0.5, 0.0, np.nan]
        q7 = [1.0e-20, 0.5, 1.0e-20, 1.0e-20, 1.0e-20]
        assert senegal18.near_zero_quantity_rows(q3, q7).tolist() == [True, False, False, False, False]

    def test_the_bound_is_one_thousandth(self, senegal18):
        assert senegal18.NEAR_ZERO_BOUND == 1e-3
        assert senegal18.near_zero_quantity_rows([9.9e-4], [9.9e-4]).tolist() == [True]
        assert senegal18.near_zero_quantity_rows([1e-3], [1e-4]).tolist() == [False]


#: Synthetic s07b records for the GH #1708 revaluation: (grappe, region, product,
#: s07bq03a, s07bq04, s07bq05, s07bq07a, consumption unit, last-purchase unit, s07bq08).
#: Sizes are all "Taille unique", so no unit-size label (and no config) is involved.
#: A flagged record has an off-grid s07bq07a (1/3).
_NIGER_RECORDS = [
    (1, 1, 1, 2.0, 0.0, 0.0, 1.0, "Kg", "Kg", 100.0),        # 0 genuine, cluster 1
    (1, 1, 1, 2.0, 0.0, 0.0, 1.0, "Kg", "Kg", 110.0),        # 1 genuine, cluster 1
    (1, 1, 1, 2.0, 0.0, 0.0, 1.0, "Kg", "Kg", 120.0),        # 2 genuine, cluster 1
    (2, 1, 1, 2.0, 0.0, 0.0, 1.0, "Kg", "Kg", 200.0),        # 3 genuine, cluster 2
    (1, 1, 1, 2.0, 0.0, 0.0, 1 / 3, "Kg", "Kg", 50.0),       # 4 flagged: cluster median 110
    (2, 1, 1, 3.0, 0.0, 0.0, 1 / 3, "Kg", "Kg", 50.0),       # 5 flagged: 1 cluster report -> region 115
    (3, 2, 1, 1.0, 0.0, 0.0, 1 / 3, "Kg", "Kg", 50.0),       # 6 flagged: no genuine price in region 2
    (1, 1, 1, 4.0, 0.0, 0.0, 1 / 3, "Bol", "Kg", 50.0),      # 7 flagged, consumed in Bol: Bol cell 30
    (1, 1, 1, 1.0, 0.0, 0.0, 2.0, "Kg", "Bol", 50.0),        # 8 genuine Bol price 25
    (1, 1, 1, 1.0, 0.0, 0.0, 2.0, "Kg", "Bol", 60.0),        # 9 genuine Bol price 30
    (1, 1, 1, 1.0, 0.0, 0.0, 2.0, "Kg", "Bol", 70.0),        # 10 genuine Bol price 35
    (1, 1, 1, 2.0, 2.0, 0.0, 1 / 3, "Kg", "Kg", 50.0),       # 11 flagged, all own-produced
    (1, 1, 1, 3.0, 0.0, 1.0, 1 / 3, "Kg", "Kg", 50.0),       # 12 flagged, one unit a gift
    (3, 2, 1, 2.0, 0.0, 0.0, 1.0, "Kg", "Kg", 300.0),        # 13 genuine, cluster 3, region 2
    (3, 2, 1, 2.0, 0.0, 0.0, 1.0, "Kg", "Kg", 310.0),        # 14 genuine, cluster 3, region 2
]
#: The served Expenditure of each revalued record's purchased row.  Record 6 has 2
#: genuine reports in its cluster and its region: NA under the region floor of 3 (305
#: under a floor of 1).  Record 7 is flagged, but #876 serves it NA (consumed in Bol,
#: last bought in Kg), so it is not revalued ("Only previously valued").
_NIGER_EXPECTED = {"h4": 220.0, "h5": 345.0, "h6": np.nan, "h12": 220.0}


def _niger_frames(niger):
    """(raw, wide, region) for _NIGER_RECORDS, as the hook receives them."""
    cols = ["grappe", "s00q01", "s07bq01", "s07bq03a", "s07bq04", "s07bq05", "s07bq07a",
            "unit3", "unit7", "s07bq08"]
    rec = pd.DataFrame(_NIGER_RECORDS, columns=cols)
    raw = rec[["grappe", "s07bq01", "s07bq03a", "s07bq04", "s07bq05", "s07bq07a", "s07bq08"]].copy()
    raw["s07bq06"] = 2.0
    ids = [f"h{n}" for n in range(len(rec))]
    index = pd.MultiIndex.from_arrays(
        [["2018-19"] * len(rec), rec["grappe"].astype(int).astype(str), [1] * len(rec), ids,
         ["Mil"] * len(rec), rec["unit3"]], names=["t", "v", "visit", "i", "j", "u"])
    wide = pd.DataFrame({
        "Quantity": rec["s07bq03a"].to_numpy(), "Produced": rec["s07bq04"].to_numpy(),
        "GiftQuantity": rec["s07bq05"].to_numpy(), "ConsumptionUnitCode": rec["unit3"].to_numpy(),
        "ConsumptionUnitSize": ["Taille unique"] * len(rec),
        "LastPurchaseQuantity": rec["s07bq07a"].to_numpy(), "LastPurchaseUnitCode": rec["unit7"].to_numpy(),
        "LastPurchaseUnitSize": ["Taille unique"] * len(rec), "LastPurchaseValue": rec["s07bq08"].to_numpy(),
    }, index=index)
    return raw, wide, rec["s00q01"].to_numpy(dtype=float)


class TestNigerRule:
    def test_off_the_quarter_grid(self, niger):
        x = [0.25, 1.0, 0.125, 0.5516, 0.1, 0.0, np.nan, 2.75, 1 / 3]
        assert niger.computed_last_purchase_rows(x).tolist() == [
            False, False, True, True, True, False, False, False, True]

    def test_the_floors(self, niger):
        """Rulings @ligon 2026-11-12: cluster floor 3, and "Region floor of 3"."""
        assert niger.LOCAL_MEDIAN_MIN_CLUSTER_REPORTS == 3
        assert niger.LOCAL_MEDIAN_MIN_REGION_REPORTS == 3
        # "Only previously valued" is the rule, not a switch.
        assert not hasattr(niger, "LOCAL_MEDIAN_REVALUES_EVERY_FLAGGED_PURCHASE")

    def test_only_previously_valued_rows_are_revalued(self, niger):
        """Ruling "Only previously valued": a flagged purchase #876 serves NA stays NA."""
        from lsms_library.ehcvm import derive_ehcvm_purchase_value
        raw, wide, region = _niger_frames(niger)
        frame = niger._valuation_frame(raw, wide, region)
        hit = niger._revalued_records(frame)
        assert np.flatnonzero(hit).tolist() == [4, 5, 6, 12]
        value_876 = derive_ehcvm_purchase_value(
            niger._purchased_quantity(frame), frame["s07bq03b"], frame["s07bq03c"],
            frame["s07bq07a"], frame["s07bq07b"], frame["s07bq07c"], frame["s07bq08"])
        flagged_purchase = frame["flagged"].to_numpy() & (niger._purchased_quantity(frame) > 0)
        assert (hit == (flagged_purchase & np.isfinite(value_876))).all()
        assert flagged_purchase[7] and np.isnan(value_876[7]) and not hit[7]

    def test_the_registered_function_on_synthetic_inputs(self, niger):
        raw, wide, region = _niger_frames(niger)
        frame = niger._valuation_frame(raw, wide, region)
        frame.index = pd.MultiIndex.from_arrays(
            [["2018-19"] * len(frame), [f"h{n}" for n in range(len(frame))], ["Mil"] * len(frame)],
            names=["t", "i", "j"])
        got = niger.derive_flagged_purchase_local_median(frame)
        assert got.index.get_level_values("i").tolist() == list(_NIGER_EXPECTED)
        np.testing.assert_allclose(got["Expenditure"].to_numpy(dtype=float),
                                   list(_NIGER_EXPECTED.values()), equal_nan=True)
        assert got["level"].astype(object).where(got["level"].notna(), None).tolist() == [
            "cluster", "region", None, "cluster"]
        assert got["cluster_reports"].tolist() == [3, 1, 2, 3]
        assert got["region_reports"].tolist() == [4, 4, 2, 4]

    def test_a_flagged_record_never_prices_another(self, niger):
        """The flagged records' own s07bq08 / s07bq07a (150 here) is the producer's
        value: it must not enter any median."""
        raw, wide, region = _niger_frames(niger)
        local = niger.local_median_unit_values(niger._valuation_frame(raw, wide, region))
        assert local.loc[4, "unit_value"] == 110.0

    def test_the_hook_writes_by_position(self, niger):
        from lsms_library.ehcvm import food_acquired_ehcvm
        raw, wide, region = _niger_frames(niger)
        frame = niger._valuation_frame(raw, wide, region)
        out = food_acquired_ehcvm(wide, niger.FOOD_ACQUIRED_DERIVATION)
        got = niger._revalue_flagged_purchases(out, wide, frame)
        s = got.index.get_level_values("s")
        i = got.index.get_level_values("i")
        key = niger.LOCAL_MEDIAN_KEY
        stamped = _carries(got, key)
        assert set(i[stamped]) == set(_NIGER_EXPECTED) and set(s[stamped]) == {"purchased"}
        purchased = got[s == "purchased"].droplevel(["t", "v", "j", "u", "s"])
        for hh, value in _NIGER_EXPECTED.items():
            assert (np.isnan(value) and np.isnan(purchased.loc[hh, "Expenditure"])) \
                or purchased.loc[hh, "Expenditure"] == pytest.approx(value)
        # Every other row is untouched, and every revalued row keeps its #876 key.
        base = out[~stamped]
        pd.testing.assert_frame_equal(got[~stamped], base)
        assert _carries(got[stamped], niger.FOOD_ACQUIRED_DERIVATION).all()
        assert purchased.loc["h0", "Expenditure"] == 200.0
        assert np.isnan(purchased.loc["h8", "Expenditure"])   # #876: Kg against a Bol price
        assert np.isnan(purchased.loc["h7", "Expenditure"])   # flagged, #876 NA: stays NA

    def test_the_hook_refuses_rows_out_of_order(self, niger):
        from lsms_library.ehcvm import food_acquired_ehcvm
        raw, wide, region = _niger_frames(niger)
        frame = niger._valuation_frame(raw, wide, region)
        out = food_acquired_ehcvm(wide, niger.FOOD_ACQUIRED_DERIVATION)
        with pytest.raises(niger.LocalMedianJoinError):
            niger._revalue_flagged_purchases(out.iloc[::-1], wide, frame)
        with pytest.raises(niger.LocalMedianJoinError):
            niger._valuation_frame(raw.iloc[::-1].reset_index(drop=True), wide, region)

    def test_the_error_is_not_a_generic_build_failure(self, niger):
        assert not issubclass(niger.LocalMedianJoinError, (ValueError, RuntimeError, KeyError))


class TestUgandaRule:
    def test_comm_waves_first(self, uganda):
        out = uganda._choose_ea(pd.Series(["12", None, None], dtype=object),
                                pd.Series([None, "40", None], dtype=object))
        assert out.tolist()[:2] == ["12", "40"] and out.isna().tolist()[2]

    def test_agreement_is_required(self, uganda):
        with pytest.raises(uganda.OriginEaError):
            uganda._choose_ea(pd.Series(["12"], dtype=object), pd.Series(["13"], dtype=object))

    def test_two_labels_for_one_household_across_the_covers_are_refused(self, uganda, monkeypatch):
        """The per-household guard of the two sub-df hooks (red team of 2026-11-12):
        two comm covers that give one household different labels must raise, not
        pick one."""
        monkeypatch.setattr(uganda, "_ea_fill_targets", lambda t: {"a", "b"})
        col = uganda._EA_COMM_WAVES_COL
        df = pd.DataFrame({"t": ["2009-10"] * 4, "i": ["a", "a", "b", "c"],
                           col: ["12", "13", "40", "41"]}).set_index(["t", "i"])
        with pytest.raises(uganda.OriginEaError):
            uganda._ea_from_another_wave(df, col)
        out = uganda._ea_from_another_wave(df.iloc[[0, 0, 2, 3]], col)
        assert out[col].to_dict() == {("2009-10", "a"): "12", ("2009-10", "b"): "40"}

    def test_derive_from_inputs(self, uganda):
        inputs = pd.DataFrame({
            "t": ["2009-10"] * 3, "i": ["a", "a", "b"],
            "_source": ["2010-11/Data/GSEC1.dta", "2013-14/Data/GSEC1.dta", "2013-14/Data/GSEC1.dta"],
            "HHID": ["a", "x", "y"], "comm": [np.nan, np.nan, np.nan],
            "HHID_old": [np.nan, "a", "b"], "ea": [np.nan, 7, 8],
        }).set_index(["t", "i"])
        # A comm-wave row whose comm reads no label contributes nothing.
        out = uganda.derive_ea_same_household(inputs)
        assert out.loc[("2009-10", "a")] == "7" and out.loc[("2009-10", "b")] == "8"

    def test_a_filled_row_keeps_no_comm_key(self, uganda):
        flat = pd.DataFrame({"t": ["2009-10", "2009-10"], "i": ["a", "b"],
                             "ea": pd.Series(["12", None], dtype=object),
                             "_ea_comm_waves": [np.nan, "40"], "_ea_2013_14": [np.nan, np.nan]})
        out = uganda._fill_ea_same_household(flat)
        assert out["ea"].tolist() == ["12", "40"]
        assert pd.isna(out["Derivation"].iloc[0])
        assert out["Derivation"].iloc[1] == "Uganda::sample::ea-same-household"
        assert "_ea_comm_waves" not in out and "_ea_2013_14" not in out


@pytest.fixture(scope="module")
def served():
    """``Country(c).food_acquired()``, built once per country for this module."""
    import warnings
    import lsms_library as ll
    cache = {}

    def get(country):
        if country not in cache:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                cache[country] = ll.Country(country).food_acquired()
        return cache[country]
    return get


def _carries(frame, key):
    return frame["Derivation"].astype("string").fillna("").str.contains(key, regex=False).to_numpy()


@pytest.mark.requires_s3
class TestServed:
    """The served tables, not the selectors (red team of 2026-11-12, F2): unwiring a
    hook must turn these red even though the raw selectors in TestHeld still pass."""

    def test_mali_2018_19_withholds_the_rice_ration_block(self, served):
        import warnings
        import lsms_library as ll
        x = served("Mali").xs("2018-19", level="t")
        assert len(x) == 157_856                       # 161,212 before GH #1707
        j = x.index.get_level_values("j").astype(str)
        u = x.index.get_level_values("u").astype(str)
        s = x.index.get_level_values("s").astype(str)
        rice = np.asarray((j == "Riz local Gambiaka") & (u == "Kg") & (s == "purchased"))
        assert int(rice.sum()) == 2_276                # 5,632 before
        assert round(float(x.loc[rice, "Quantity"].sum()), 1) == 23_581.7   # 55,895.1 before
        key = "Mali::food_acquired::rice-ration-block-withheld"
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            block = ll.Country("Mali").derivation_inputs(key, wave="2018-19")
        assert len(block) == 3_357
        # Of the 3,357 block households, only the one that also answered a rice-kg
        # purchase of its own still has a served rice-kg purchased row.
        hit = set(block.index.get_level_values("i")) & set(x.index.get_level_values("i")[rice])
        assert len(hit) == 1
        assert not _carries(x, key).any()              # a deletion carries no key

    def test_niger_flags_exactly_the_purchases_of_a_computed_quantity(self, served):
        fa = served("Niger")
        key = "Niger::food_acquired::computed-last-purchase-quantity"
        has = _carries(fa, key)
        assert int(has.sum()) == 15_014
        assert set(fa.index.get_level_values("t")[has].astype(str)) == {"2018-19"}
        assert set(fa.index.get_level_values("s")[has].astype(str)) == {"purchased"}

    def test_niger_revalues_only_the_previously_valued_flagged_purchases(self, served):
        """Rulings "Revalue at local median" and "Only previously valued": the 12,227
        flagged purchased rows #876 values carry the key (15,014 under the first scope);
        the 2,787 it serves NA stay NA and carry the flag alone."""
        fa = served("Niger")
        flagged = _carries(fa, "Niger::food_acquired::computed-last-purchase-quantity")
        revalued = _carries(fa, "Niger::food_acquired::flagged-purchase-local-median")
        assert int(revalued.sum()) == 12_227
        assert not (revalued & ~flagged).any()
        left = flagged & ~revalued
        assert int(left.sum()) == 2_787
        assert fa.loc[left, "Expenditure"].isna().all()

    def test_niger_the_registered_function_reproduces_the_served_value(self, served):
        """`derivation_inputs` returns every record (the genuine ones are the prices),
        and the registered function, run on them, gives the served Expenditure of every
        revalued row, NA where it is NA."""
        import warnings
        import lsms_library as ll
        key = "Niger::food_acquired::flagged-purchase-local-median"
        c = ll.Country("Niger")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            inputs = c.derivation_inputs(key, wave="2018-19")
        assert len(inputs) == 107_269 and int(inputs["flagged"].sum()) == 15_264
        derived = c.derivations()[key].resolve()(inputs)
        assert len(derived) == 12_227                  # 15,014 under the first scope
        fa = served("Niger")
        x = fa[_carries(fa, key)]
        got = x["Expenditure"].groupby(level=["t", "i", "j"]).sum(min_count=1).sort_index()
        want = derived["Expenditure"].groupby(level=["t", "i", "j"]).sum(min_count=1)
        assert set(got.index) == set(want.index)
        np.testing.assert_allclose(got.to_numpy(dtype=float),
                                   want.reindex(got.index).to_numpy(dtype=float),
                                   rtol=1e-9, equal_nan=True)


@pytest.mark.requires_s3
class TestHeld:
    """The counts the rulings were made on, from the raw files."""

    def test_mali_block(self, mali):
        raw = mali._rice_ration_raw()
        block = mali.rice_ration_rows(raw)
        assert int(block.sum()) == 3357
        assert round(float(pd.to_numeric(raw.loc[block, "s07bq03a"]).sum()), 1) == 32313.4

    def test_senegal_salt(self, senegal21):
        raw = senegal21._salt_raw()
        hit = senegal21.salt_per_capita_rows(raw)
        assert int(hit.sum()) == 100
        # The evidence the rule rests on, read here and never by the build: each
        # record's whole multiple of the constant is its household's size.
        persons = np.round(pd.to_numeric(raw.loc[hit, "s07bq03a"]).to_numpy()
                           / senegal21.SALT_KG_PER_PERSON)
        hhsize = senegal21._household_size(raw)[hit]
        assert (persons == hhsize).all()

    def test_senegal_near_zero(self, senegal18):
        raw = senegal18._near_zero_raw()
        hit = senegal18.near_zero_quantity_rows(raw["s07bq03a"], raw["s07bq07a"])
        assert int(hit.sum()) == 3
        q3 = pd.to_numeric(raw.loc[hit, "s07bq03a"])
        q7 = pd.to_numeric(raw.loc[hit, "s07bq07a"])
        assert np.allclose(q3 / q7, 40.0)
        assert set(pd.to_numeric(raw.loc[hit, "s07bq01"])) == {132}

    def test_niger_flagged(self, niger):
        raw = niger._computed_raw()
        assert int(niger.computed_last_purchase_rows(raw["s07bq07a"]).sum()) == 15264

    def test_uganda_inputs_join_and_the_function_reproduces_the_value(self):
        """As test_gh1459 does for the two comm keys: the stamped rows are exactly the
        households the inputs return, and the registered function, run on those raw
        inputs, gives the served ``ea`` on every one."""
        import warnings
        import lsms_library as ll
        key = "Uganda::sample::ea-same-household"
        c = ll.Country("Uganda")
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            raw = c.derivation_inputs(key, wave="2009-10")
            frame = c.sample().xs("2009-10", level="t")
        assert raw.index.get_level_values("i").nunique() == 514
        stamped = frame.index[frame["Derivation"].astype("string")
                              .str.contains(key, regex=False).fillna(False)]
        assert set(raw.index.get_level_values("i")) == set(stamped)
        derived = c.derivations()[key].resolve()(raw).droplevel("t")
        assert len(derived) == 514
        assert derived.to_dict() == frame.loc[derived.index, "ea"].to_dict()
