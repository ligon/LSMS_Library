"""Ethiopia ``sample.strata``: a registered construction, its values unchanged (GH #1741).

Source-free.  The operands are invented, and nothing here reads survey data or
builds a table.  That the served frame carries the key is checked, where the data
are held, by ``test_sampling.py::test_the_tag_agrees_with_the_served_strata``,
which picks these waves up from ``sampling.yml``.
"""
from __future__ import annotations

import ast
import importlib.util
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library.derivations import derivation_records, resolve_callable
from lsms_library.paths import countries_root
from lsms_library.sampling import sampling_records

KEY = "Ethiopia::sample::region-rural-stratum"
WAVES = ["2011-12", "2013-14", "2015-16", "2018-19", "2021-22"]
ROOT = countries_root() / "Ethiopia"


@pytest.fixture
def eth():
    spec = importlib.util.spec_from_file_location("_ethiopia1741", ROOT / "_" / "ethiopia.py")
    module = importlib.util.module_from_spec(spec)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        spec.loader.exec_module(module)
    return module


def before_1741(region, rural):
    """The wave scripts' three statements, verbatim, as they stood at 47df62d49."""
    sample = pd.DataFrame({"Rural": rural})
    region = region.astype(str).str.strip()
    sample["strata"] = region + " " + sample["Rural"].fillna("")
    sample["strata"] = sample["strata"].str.strip()
    return sample["strata"]


# -- the values ------------------------------------------------------------

OPERANDS = {
    "labels": (["Amhara", "Tigray", "Addis Ababa"], ["Rural", "Urban", "Urban"]),
    "padded region": ([" Afar", "SNNP ", " Somalie "], ["Rural", "Urban", "Rural"]),
    "missing rural serves the region alone": (["Amhara", "Tigray"], ["Rural", None]),
    "missing region": (["Amhara", None], ["Rural", "Urban"]),
    "both missing": (["Amhara", None], ["Rural", None]),
}


@pytest.mark.parametrize("name", sorted(OPERANDS))
@pytest.mark.parametrize("categorical", [False, True])
def test_the_function_is_the_expression_the_scripts_used(eth, name, categorical):
    region, rural = (pd.Series(x, dtype=object) for x in OPERANDS[name])
    if categorical:                      # `saq01` arrives as a categorical
        region = region.astype("category")
    got = eth.derive_region_rural_stratum(region, rural)
    pd.testing.assert_series_equal(got, before_1741(region, rural),
                                   check_names=False, check_exact=True)


def test_the_expected_labels_and_the_region_alone_fallback(eth):
    """Literal, so a change to both copies of the expression is still caught."""
    got = eth.derive_region_rural_stratum(
        pd.Series([" AMHARA ", "Diredwa", "Tigray"], dtype=object),
        pd.Series(["Rural", "Urban", None], dtype=object))
    assert got.tolist() == ["AMHARA Rural", "Diredwa Urban", "Tigray"]


# -- the stamp -------------------------------------------------------------

def _sample():
    out = pd.DataFrame({
        "i": ["a", "b", "c"], "v": ["1", "1", "2"], "weight": [1.0, np.nan, 0.0],
        "Rural": ["Rural", "Urban", None],
        "strata": pd.Series(["Afar Rural", "Afar Urban", None], dtype=object),
        "panel_weight": [1.0, np.nan, 0.0], "t": "2018-19"})
    out.attrs = {"kept": {"x": 1}}
    return out


def test_the_stamp_is_on_every_row_with_a_stratum_and_no_other(eth):
    sample = _sample()
    before = sample.copy(deep=True)
    out = eth.stamp_region_rural_stratum(sample)
    assert out["Derivation"].isna().tolist() == [False, False, True]
    assert out["Derivation"].dropna().unique().tolist() == [KEY]
    pd.testing.assert_frame_equal(out[list(before.columns)], before, check_exact=True)
    assert out.attrs == before.attrs
    pd.testing.assert_frame_equal(sample, before, check_exact=True)   # not mutated
    assert "Derivation" not in sample


def _covers():
    """Invented 2018-19-shaped covers: two HH households, one on the PP cover only."""
    hh = pd.DataFrame({"household_id": ["h1", "h2"], "ea_id": ["e1", "e2"],
                       "pw_w4": [2.0, 6.0], "saq01": [4, 3], "saq14": [1, 2]})
    labels = hh.assign(saq01=["OROMIA", "AMHARA"], saq14=["RURAL", "URBAN"])
    pp = pd.DataFrame({"household_id": ["p1", "h1", "p1"], "ea_id": ["e3", "e1", "e3"],
                       "pw_w4": [1.0, 2.0, 1.0], "saq01": [3, 4, 3], "saq14": [1, 1, 1]})
    return hh, labels, pp


RURAL_MAP = {"RURAL": "Rural", "URBAN": "Urban"}
COLMAP = {"hhid": "household_id", "ea": "ea_id", "weight": "pw_w4", "rural": "saq14"}


def _native(eth):
    hh, labels, pp = _covers()
    sample = pd.DataFrame({"i": labels["household_id"], "v": labels["ea_id"],
                           "weight": labels["pw_w4"].astype(float),
                           "Rural": labels["saq14"].map(RURAL_MAP)})
    sample["strata"] = eth.derive_region_rural_stratum(labels["saq01"], sample["Rural"])
    sample["panel_weight"] = sample["weight"]
    sample["t"] = "2018-19"
    return eth.with_agriculture_cover_households(sample, "2018-19", hh, labels, pp,
                                                 COLMAP, RURAL_MAP)


def test_an_appended_household_is_stamped_too(eth):
    out = eth.stamp_region_rural_stratum(_native(eth))
    assert out["i"].tolist() == ["h1", "h2", "p1"]
    assert out["strata"].tolist() == ["OROMIA Rural", "AMHARA Urban", "AMHARA Rural"]
    assert (out["Derivation"] == KEY).all()


def test_the_append_uses_the_registered_function(eth, monkeypatch):
    real, calls = eth.derive_region_rural_stratum, []
    monkeypatch.setattr(eth, "derive_region_rural_stratum",
                        lambda region, rural: calls.append(1) or real(region, rural))
    _native(eth)
    assert len(calls) == 2               # the cover's rows, and the appended ones


# -- the inputs ------------------------------------------------------------

def test_the_inputs_cover_every_row_and_give_the_served_label_back(eth, monkeypatch):
    hh, labels, pp = _covers()

    def read(path, convert_categoricals=True):
        if path.endswith("sect_cover_pp_w4.dta"):
            return pp.copy()
        assert path.endswith("sect_cover_hh_w4.dta"), path
        return (labels if convert_categoricals else hh).copy()

    monkeypatch.setattr(eth, "get_dataframe", read)
    got = eth.inputs_region_rural_stratum("2018-19")
    assert got.index.names == ["t", "i"] and got.index.is_unique
    assert set(got.index.get_level_values("i")) == {"h1", "h2", "p1"}
    assert got["module"].tolist() == ["sect_cover_hh", "sect_cover_hh", "sect_cover_pp"]
    assert got["saq01_code"].tolist() == [4, 3, 3]
    again = eth.derive_region_rural_stratum(got["saq01"], got["saq14"].map(RURAL_MAP))
    assert again.tolist() == _native(eth)["strata"].tolist()


def test_an_undeclared_wave_is_refused(eth):
    with pytest.raises(ValueError, match="no Ethiopia cover"):
        eth.inputs_region_rural_stratum("1999-00")


# -- the scripts, read with ast --------------------------------------------

def _calls(node, name):
    return [n for n in ast.walk(node) if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Name) and n.func.id == name]


@pytest.mark.parametrize("wave", WAVES)
def test_each_wave_script_builds_then_appends_then_stamps(wave):
    tree = ast.parse((ROOT / wave / "_" / "sample.py").read_text(encoding="utf-8"))
    writes = [n for n in ast.walk(tree) if isinstance(n, ast.Assign)
              and any(isinstance(t, ast.Subscript) and ast.unparse(t) == "sample['strata']"
                      for t in n.targets)]
    assert len(writes) == 1
    assert ast.unparse(writes[0].value) == \
        "derive_region_rural_stratum(df['saq01'], sample['Rural'])"
    (append,), (stamp,) = (_calls(tree, "with_agriculture_cover_households"),
                           _calls(tree, "stamp_region_rural_stratum"))
    index = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and ast.unparse(n.func) == "sample.set_index"]
    assert len(index) == 1
    assert writes[0].lineno < append.lineno < stamp.lineno < index[0].lineno


# -- the registry and the tag ----------------------------------------------

def test_the_registry_entry():
    rec = derivation_records("Ethiopia")[KEY]
    assert rec.table == "sample" and list(rec.columns) == ["strata"]
    assert list(rec.waves) == WAVES
    assert callable(resolve_callable(rec.function))
    assert callable(resolve_callable(rec.inputs))
    assert rec.function.endswith(":derive_region_rural_stratum")


def test_the_cross_is_recorded_as_a_choice_and_not_as_documented():
    rec = derivation_records("Ethiopia")[KEY]
    (cross,) = [a for a in rec.assumptions if "region crossed with Rural / Urban" in a["text"]]
    assert cross["basis"] == "modelling-choice"
    assert not {"questionnaire", "variable-label"} & {a["basis"] for a in rec.assumptions}


@pytest.mark.parametrize("wave", WAVES)
def test_every_wave_is_tagged_constructed_under_that_key(wave):
    rec = sampling_records("Ethiopia")[wave]
    assert rec.strata_holds == "constructed"
    assert rec.served_strata["derivation"] == KEY


def test_the_boundary_declarations_are_untouched():
    """A second wave key in the file would silently drop the first's `panel`."""
    recs = sampling_records("Ethiopia")
    assert {w: recs[w].cluster_id_continuity for w in WAVES} == {
        "2011-12": None, "2013-14": "wave-scoped", "2015-16": "stable",
        "2018-19": "unobservable", "2021-22": "stable"}
    assert recs["2013-14"]["panel"]["cluster_id_measurement"]["linked_households"] == 3776
