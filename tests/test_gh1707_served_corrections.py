"""Served-wave corrections of 2026-11-12 (rulings @ligon 2026-11-12).

- Mali 2018-19: the rice-ration block is withheld (GH #1707, "Remove, registered").
- Senegal 2021-22: salt at 0.0199997 kg x hhsize is withheld ("Remove, registered").
- Senegal 2018-19: two quantities below 1e-3 are served NA (GH #1519, "Exact-value NA rule").
- Niger 2018-19: an off-grid s07bq07a is flagged and served (GH #1708, "Flag now, diagnose").
- Uganda 2009-10: a missing ea is filled from the same household (GH #1536, "Fill, registered").

Two tiers: synthetic frames through the real wave modules (no microdata), and
``TestHeld``, which reads the raw files through ``get_dataframe`` and pins the
counts the rulings were made on.  Each module is loaded by path, as the
framework loads it; none reads data at import.
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
    "Niger": ["Niger::food_acquired::computed-last-purchase-quantity"],
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

    def test_salt_household_size_file_is_declared(self):
        from lsms_library.yaml_utils import load_yaml
        info = load_yaml(countries_root() / "Senegal" / "2021-22" / "_" / "data_info.yml")
        assert info["food_acquired"]["household_size"]["file"] == "ehcvm_welfare_sen2021.dta"


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
    def test_salt_constant_is_exact(self, senegal21):
        raw = pd.DataFrame({"s07bq01": [139, 139, 139, 1], "s07bq03b": [100, 100, 100, 100],
                            "s07bq03c": [0, 0, 0, 0], "hhsize": [5, 5, 6, 5],
                            "s07bq03a": [0.0199997 * 5, 0.02 * 5, 0.0199997 * 5, 0.0199997 * 5]})
        assert senegal21.salt_per_capita_rows(raw).tolist() == [True, False, False, False]

    def test_salt_without_household_size_is_never_selected(self, senegal21):
        raw = pd.DataFrame({"s07bq01": [139], "s07bq03b": [100], "s07bq03c": [0],
                            "hhsize": [np.nan], "s07bq03a": [0.0]})
        assert not senegal21.salt_per_capita_rows(raw).any()

    def test_near_zero_needs_both_quantities(self, senegal18):
        q3 = [4.2e-19, 4.2e-19, 0.5, 0.0, np.nan]
        q7 = [1.0e-20, 0.5, 1.0e-20, 1.0e-20, 1.0e-20]
        assert senegal18.near_zero_quantity_rows(q3, q7).tolist() == [True, False, False, False, False]

    def test_the_bound_is_one_thousandth(self, senegal18):
        assert senegal18.NEAR_ZERO_BOUND == 1e-3
        assert senegal18.near_zero_quantity_rows([9.9e-4], [9.9e-4]).tolist() == [True]
        assert senegal18.near_zero_quantity_rows([1e-3], [1e-4]).tolist() == [False]


class TestNigerRule:
    def test_off_the_quarter_grid(self, niger):
        x = [0.25, 1.0, 0.125, 0.5516, 0.1, 0.0, np.nan, 2.75, 1 / 3]
        assert niger.computed_last_purchase_rows(x).tolist() == [
            False, False, True, True, True, False, False, False, True]


class TestUgandaRule:
    def test_comm_waves_first(self, uganda):
        out = uganda._choose_ea(pd.Series(["12", None, None], dtype=object),
                                pd.Series([None, "40", None], dtype=object))
        assert out.tolist()[:2] == ["12", "40"] and out.isna().tolist()[2]

    def test_agreement_is_required(self, uganda):
        with pytest.raises(uganda.OriginEaError):
            uganda._choose_ea(pd.Series(["12"], dtype=object), pd.Series(["13"], dtype=object))

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


@pytest.mark.requires_s3
class TestHeld:
    """The counts the rulings were made on, from the raw files."""

    def test_mali_block(self, mali):
        raw = mali._rice_ration_raw()
        block = mali.rice_ration_rows(raw)
        assert int(block.sum()) == 3357
        assert round(float(pd.to_numeric(raw.loc[block, "s07bq03a"]).sum()), 1) == 32313.4

    def test_senegal_salt(self, senegal21):
        assert int(senegal21.salt_per_capita_rows(senegal21._salt_raw()).sum()) == 100

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
