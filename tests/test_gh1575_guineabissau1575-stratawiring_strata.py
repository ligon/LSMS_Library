"""Guinea-Bissau GH1575 wiring, using invented frames only (ledger §§2–5)."""
import copy
import importlib.util
import re
from pathlib import Path
from types import MethodType, SimpleNamespace

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal, assert_series_equal

from lsms_library import country as country_module, data_access, ehcvm, local_tools
from lsms_library.country import Wave
from lsms_library.derivations import attach, call_inputs, derivation_records
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml


COUNTRY = "Guinea-Bissau"
WAVE = "2018-19"
ROOT = countries_root() / COUNTRY
KEY = ehcvm.stratum_derivation_key(COUNTRY)
COVER = "s00_me_gnb2018.dta"
WEIGHTS = "ehcvm_ponderations_gnb2018.dta"


@pytest.fixture(autouse=True)
def no_native_or_api(monkeypatch):
    def refused(*args, **kwargs):
        raise AssertionError("pure wiring test attempted a native/API read")
    monkeypatch.setattr(country_module.Country, "__init__", refused)
    monkeypatch.setattr(local_tools, "get_dataframe", refused)
    monkeypatch.setattr(data_access, "get_data_file", refused)
    monkeypatch.setattr(pd, "read_stata", refused)


@pytest.fixture
def mapping():
    spec = importlib.util.spec_from_file_location("_gb1575_mapping", ROOT / WAVE / "_" / "mapping.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _sample():
    return load_yaml(ROOT / WAVE / "_" / "data_info.yml")["sample"]


def _wave(mapping):
    # Exercise the real mapper without constructing a Country or loading a table.
    functions = local_tools.get_formatting_functions(ROOT / "_" / "guinea-bissau.py", name="_gb1575_country")
    functions.update(vars(mapping))
    wave = SimpleNamespace(
        resources={"sample": _sample()}, file_path=ROOT / WAVE,
        formatting_functions=functions,
        country=SimpleNamespace(resources={"Data Scheme": {"sample": {}}}),
    )
    wave.column_mapping = MethodType(Wave.column_mapping, wave)
    wave._cartesian_keys = MethodType(Wave._cartesian_keys, wave)
    return wave


def _cover():
    return pd.DataFrame({
        "grappe": [43, 11, 43, 17, 19, 23], "menage": [2, 1, 1, 1, 1, 1],
        "s00q01": [" bafata ", "sab", "bafata", "BOLAMA_BIJAGOS", "gabu", pd.NA],
        "s00q04": ["Rural", "Urbano", "Urbano", "Rural", "  ", "Rural"],
    })


def _extract(wave, block):
    frames = []
    for name in block["dfs"]:
        cols = wave.column_mapping(name, block[name])
        assert cols.pop("df_edit") is None
        assert len(cols) == 1
        filename, spec = next(iter(cols.items()))
        frames.append(local_tools.df_data_grabber(
            str(wave.file_path / "Data" / filename), spec["idxvars"], **spec["myvars"]).reset_index())
    out = Wave._merge_subframes(wave, *frames, block["merge_on"], "sample", *block["dfs"])
    out["t"] = WAVE
    return out.set_index(block["final_index"])


def test_declaration_changes_only_the_strata_operand():
    block = copy.deepcopy(_sample())
    assert block["df_cover"]["myvars"].pop("strata") == [
        "s00q01", "s00q04", {"mapping": "strata_label"}]
    assert block == {
        "dfs": ["df_cover", "df_weights"],
        "df_cover": {"file": COVER, "idxvars": {"v": "grappe", "i": ["grappe", "menage"]},
                     "myvars": {"Rural": ["s00q04", {"mapping": {
                         "Rural": "Rural", "rural": "Rural", "RURAL": "Rural",
                         "Urbano": "Urban", "urbain": "Urban", "URBAIN": "Urban"}}]}},
        "df_weights": {"file": WEIGHTS, "idxvars": {"v": "grappe"},
                       "myvars": {"weight": "hhweight", "panel_weight": "hhweight"}},
        "merge_on": ["v"], "final_index": ["i", "t"],
    }


@pytest.mark.parametrize("weight_mode", ["complete", "missing", "orphan"])
def test_actual_yaml_extraction_merge_and_hook_conserve_nonstrata(monkeypatch, mapping, weight_mode):
    weights = pd.DataFrame({"grappe": [43, 11, 17, 19, 23], "hhweight": [7.25, 3.0, 5.5, 9.0, 2.0]})
    if weight_mode == "missing":
        weights = weights.iloc[:2]
    if weight_mode == "orphan":
        weights = pd.concat([weights, pd.DataFrame({"grappe": [97], "hhweight": [13.0]})], ignore_index=True)
    frames = {COVER: _cover(), WEIGHTS: weights}
    calls = []
    def provider(path, **kwargs):
        path = Path(path)
        assert path.parent == ROOT / WAVE / "Data"
        assert kwargs == {"convert_categoricals": True, "encoding": None}
        calls.append(path.name)
        return frames[path.name].copy(deep=True)
    monkeypatch.setattr(local_tools, "get_dataframe", provider)
    wave = _wave(mapping)
    old = copy.deepcopy(_sample())
    old["df_cover"]["myvars"]["strata"] = "s00q01"
    before = _extract(wave, old)
    extracted = _extract(wave, _sample())
    extracted.attrs = {"id_converted": True, "sentinel": {"rowkeys": ["untouched"]}}
    original = extracted.copy(deep=True)
    after = mapping.sample(extracted)
    assert_frame_equal(extracted, original)
    assert after.attrs == extracted.attrs
    assert_frame_equal(after.drop(columns=["strata", "Derivation"]), before.drop(columns="strata"))
    assert_series_equal(after["strata"], original["strata"])
    expected = {"43002": "bafata Rural", "11001": "sab Urbano", "43001": "bafata Urbano",
                "17001": "BOLAMA_BIJAGOS Rural", "19001": pd.NA, "23001": pd.NA}
    for (household, _), value in after["strata"].items():
        if pd.isna(household) or pd.isna(expected[household]):
            assert pd.isna(value)
        else:
            assert value == expected[household]
    assert isinstance(after["Derivation"].dtype, pd.StringDtype)
    assert after["Derivation"].isna().equals(after["strata"].isna())
    assert set(after["Derivation"].dropna()) == {KEY}
    assert calls == [COVER, WEIGHTS, COVER, WEIGHTS]


@pytest.mark.parametrize("region,milieu", [
    (pd.NA, "Rural"), (None, "Urbano"), (float("nan"), "Rural"),
    ("bafata", pd.NA), ("bafata", None), ("", "Rural"), ("  ", "Urbano"),
    ("bafata", ""), ("bafata", "  "),
])
def test_own_yaml_missing_or_blank_operand_is_not_region_only(monkeypatch, mapping, region, milieu):
    frame = _cover().iloc[:1].copy()
    frame["s00q01"], frame["s00q04"] = region, milieu
    monkeypatch.setattr(local_tools, "get_dataframe", lambda *a, **k: frame.copy())
    cols = _wave(mapping).column_mapping("df_cover", _sample()["df_cover"])[COVER]
    out = local_tools.df_data_grabber(COVER, cols["idxvars"], **cols["myvars"])
    assert len(out) == 1 and out["strata"].isna().all()
    assert mapping.sample(out)["Derivation"].isna().all()


@pytest.mark.parametrize("region,milieu", [(1, "Rural"), ("bafata", 2), (1.0, 2.0)])
def test_own_yaml_keeps_dedicated_nonstring_refusal(monkeypatch, mapping, region, milieu):
    frame = _cover().iloc[:1].copy()
    frame["s00q01"], frame["s00q04"] = region, milieu
    monkeypatch.setattr(local_tools, "get_dataframe", lambda *a, **k: frame.copy())
    cols = _wave(mapping).column_mapping("df_cover", _sample()["df_cover"])[COVER]
    with pytest.raises(ehcvm.StratumError, match="never from codes") as caught:
        local_tools.df_data_grabber(COVER, cols["idxvars"], **cols["myvars"])
    assert not isinstance(caught.value, (ValueError, KeyError, RuntimeError, AttributeError))


def test_empty_and_typed_attrs_contract(mapping):
    frame = pd.DataFrame({"strata": pd.Series([], dtype="string"), "v": pd.Series([], dtype="string")})
    frame.attrs = {"id_converted": True, "derivations": {"sentinel": {"unchanged": {"rows": 0}}}}
    original = frame.copy(deep=True)
    out = mapping.sample(frame)
    assert_frame_equal(frame, original)
    assert out.attrs == original.attrs
    assert isinstance(out["Derivation"].dtype, pd.StringDtype)
    attach(out, COUNTRY, "sample")
    assert out.attrs["id_converted"] is True
    assert out.attrs["derivations"]["sentinel"] == original.attrs["derivations"]["sentinel"]
    assert out.attrs["derivations"][COUNTRY][KEY] == {"rows": 0, "columns": ["strata"], "in": "sample"}


def test_registry_and_shared_callable_bindings(mapping):
    text = (ROOT / "_" / "derivations.yml").read_text()
    assert len(re.findall(rf"^{re.escape(KEY)}:", text, flags=re.M)) == 1
    rec = derivation_records(COUNTRY)[KEY]
    assert list(rec.waves) == [WAVE] and list(rec.columns) == ["strata"]
    assert dict(rec.rows) == {} and list(rec.raw_variables) == ["s00q01", "s00q04"]
    assert rec.resolve() is ehcvm.derive_region_milieu_stratum
    assert rec.resolve("inputs").__name__ == "inputs_strata"
    assert rec.inputs == "lsms_library.countries.Guinea-Bissau.2018-19._.mapping:inputs_strata"
    assert mapping.strata_label is ehcvm.strata_label
    assert mapping._inputs_region_milieu_stratum is ehcvm.inputs_region_milieu_stratum
    assert mapping._stamp_region_milieu_stratum is ehcvm.stamp_region_milieu_stratum


def test_registered_inputs_use_actual_cover_mapping_and_default_wave_scope(monkeypatch, mapping):
    wave = _wave(mapping)
    calls = []
    class InventedCountry:
        def __getitem__(self, requested):
            calls.append(requested)
            if requested != WAVE:
                raise KeyError("invented fixture has no configured sample for " + requested)
            return wave
    def country(name):
        assert name == COUNTRY
        return InventedCountry()
    monkeypatch.setattr(country_module, "Country", country)
    def provider(path, **kwargs):
        assert Path(path) == ROOT / WAVE / "Data" / COVER
        assert kwargs == {"convert_categoricals": True, "encoding": None}
        return _cover().copy()
    monkeypatch.setattr(local_tools, "get_dataframe", provider)
    rec = derivation_records(COUNTRY)[KEY]
    inputs = call_inputs(rec)
    assert calls == [WAVE]
    assert inputs.index.names == ["t", "i"]
    assert list(inputs.columns) == ["s00q01", "s00q04"]
    assert inputs.index.get_level_values("i").tolist() == ["43002", "11001", "43001", "17001", "19001", "23001"]
    assert_frame_equal(inputs.reset_index(drop=True), _cover()[["s00q01", "s00q04"]])
    # Scope is metadata: explicit unsupported inputs must propagate their error.
    with pytest.raises(KeyError, match="no configured sample"):
        call_inputs(rec, wave="2021-22")
    assert calls == [WAVE, "2021-22"]


def test_composite_aliases_are_full_own_region_crossproduct_with_bare_entries_preserved():
    tables = local_tools.all_dfs_from_orgfile(ROOT / "_" / "categorical_mapping.org")
    region = dict(zip(tables["Region"].iloc[:, 0], tables["Region"]["Preferred Label"]))
    table = tables["strata"]
    assert not table.iloc[:, 0].duplicated().any()
    strata = dict(zip(table.iloc[:, 0], table["Preferred Label"]))
    bare = {"BOLAMA_BIJAGOS": "Bolama-Bijagos", "bafata": "Bafata", "biombo": "Biombo",
            "cacheu": "Cacheu", "gabu": "Gabu", "oio": "Oio", "quinara": "Quinara",
            "sab": "SAB", "tombali": "Tombali"}
    expected = {f"{raw} {milieu}": f"{preferred} {milieu}"
                for raw, preferred in region.items() for milieu in ("Urbano", "Rural")}
    assert strata == {**bare, **expected}
    assert strata.get("unrecorded-region Urbano", "unrecorded-region Urbano") == "unrecorded-region Urbano"
