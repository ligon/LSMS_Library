"""Togo's selected GH1575 wiring, 2018 and 2021-22, using invented frames only.

These controls exercise the actual YAML, column mapper, extraction, join and
thin hooks. They establish no native card, population or allocation count.
The baseline uses the same specification with the former region-only field
and no new sample hook (ledger 1575-togo1575-stratawiring §§2–5).  2021-22 has
the same construction since the ruling of 2026-11-12 ("Same rule as #1575"),
with its own cover and its own labels: code 6 is ``Grand Lomé`` there and
``Lomé commune`` in 2018, and neither is renamed (GH #1697).
"""
from copy import deepcopy
import importlib
import importlib.util
from pathlib import Path
import re
from types import SimpleNamespace

import pandas as pd
import pytest

import lsms_library.country as C
from lsms_library import data_access, ehcvm, local_tools
from lsms_library.paths import countries_root, data_root
from lsms_library.yaml_utils import load_yaml

derivations = importlib.import_module("lsms_library.derivations")

ROOT = countries_root()
OWN = ROOT / "Togo"
WAVE_DIR = OWN / "2018"
KEY = "Togo::sample::region-milieu-stratum"
SPEC = load_yaml(WAVE_DIR / "_" / "data_info.yml")["sample"]
MAPPING_PATH = WAVE_DIR / "_" / "mapping.py"
_module_spec = importlib.util.spec_from_file_location("_togo_2018_gh1575", MAPPING_PATH)
MAPPING = importlib.util.module_from_spec(_module_spec)
_module_spec.loader.exec_module(MAPPING)
LATER = "2021-22"
LATER_DIR = OWN / LATER
LATER_SPEC = load_yaml(LATER_DIR / "_" / "data_info.yml")["sample"]
LATER_MAPPING_PATH = LATER_DIR / "_" / "mapping.py"
_later_spec = importlib.util.spec_from_file_location("_togo_2021_gh1575", LATER_MAPPING_PATH)
LATER_MAPPING = importlib.util.module_from_spec(_later_spec)
_later_spec.loader.exec_module(LATER_MAPPING)


@pytest.fixture(autouse=True)
def no_data(monkeypatch, tmp_path):
    """The GH812 invented-provider seam; every other data route is refused."""
    def refused(*args, **kwargs):
        pytest.fail("GH1575 pure control attempted a real data/API/cache route")

    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path / "empty-data"))
    monkeypatch.setenv("LSMS_NO_CACHE", "1")
    data_root.cache_clear()
    monkeypatch.setattr(local_tools, "get_dataframe", refused)
    monkeypatch.setattr(C, "get_dataframe", refused)
    monkeypatch.setattr(data_access, "get_data_file", refused)
    monkeypatch.setattr(C, "to_parquet", refused)
    monkeypatch.setattr(local_tools, "to_parquet", refused)
    monkeypatch.setattr(C, "Country", refused)
    yield
    data_root.cache_clear()


@pytest.fixture
def synthetic(monkeypatch):
    functions = local_tools.get_formatting_functions(
        OWN / "_" / "togo.py", name="_togo_country_gh1575")
    functions.update(local_tools.get_formatting_functions(
        MAPPING_PATH, name="_togo_mapping_gh1575"))
    later_functions = local_tools.get_formatting_functions(
        OWN / "_" / "togo.py", name="_togo_country_gh1575_later")
    later_functions.update(local_tools.get_formatting_functions(
        LATER_MAPPING_PATH, name="_togo_mapping_gh1575_later"))
    scheme = load_yaml(OWN / "_" / "data_scheme.yml")
    # Use the real Wave methods with a source-only parent, never Country APIs.
    parent = SimpleNamespace(name="Togo", resources=scheme)
    wave = C.Wave("2018", "2018", parent)
    monkeypatch.setattr(C.Wave, "resources", property(lambda self: self._test_resources))
    monkeypatch.setattr(C.Wave, "formatting_functions",
                        property(lambda self: dict(self._test_functions)))
    reads = []
    frames = {}

    def provider(path, **kwargs):
        path = Path(path).resolve()
        assert path in frames, f"undeclared synthetic source: {path}"
        assert kwargs.get("convert_categoricals") is True
        reads.append(path)
        return frames[path].copy(deep=True)

    monkeypatch.setattr(local_tools, "get_dataframe", provider)

    later = C.Wave(LATER, LATER, parent)

    def bind(cover, weights, *, baseline=False, wave_label="2018"):
        """Bind one wave's sample block to invented frames.  ``wave_label='2021-22'``
        binds that wave's own block and functions instead, leaving 2018's frames bound."""
        target, spec, fns, wdir = ((later, LATER_SPEC, later_functions, LATER_DIR)
                                   if wave_label == LATER else
                                   (wave, SPEC, functions, WAVE_DIR))
        block = deepcopy(spec)
        target._test_functions = dict(fns)
        if baseline:
            block["df_cover"]["myvars"]["strata"] = "s00q01"
            target._test_functions.pop("sample", None)
        target._test_resources = {"sample": block}
        if wave_label != LATER:
            frames.clear()
        for name, frame in (("df_cover", cover), ("df_weights", weights)):
            frames[(wdir / "Data" / block[name]["file"]).resolve()] = frame
        return target

    return bind, reads


def _cover():
    # Repeated cluster, deliberately unsorted households; geography is invented.
    return pd.DataFrame({
        "grappe": [8, 2, 8, 5], "menage": [2, 1, 1, 1],
        "s00q01": [" Invented Coast ", "Invented Inland", "Invented Coast", "Invented Hill"],
        "s00q04": ["Urbain", "Rural", "Urbain", "rural"],
        "unrequested": ["a", "b", "c", "d"],
    })


def _weights():
    # A cover-only cluster and a weight-only cluster exercise the existing outer join.
    return pd.DataFrame({"grappe": [8, 2, 99], "hhweight": [17.25, pd.NA, 41.5]})


def _later_cover():
    # 2021-22's own region-6 label beside an ordinary one; geography otherwise invented.
    return pd.DataFrame({
        "grappe": [8, 2, 8], "menage": [2, 1, 1],
        "s00q01": ["Grand Lomé", "Invented Inland", " Grand Lomé "],
        "s00q04": ["Urbain", "Rural", "Urbain"],
    })


def _later_weights():
    # One row per household in 2021-22, keyed like the cover (merge_on: i).
    return pd.DataFrame({"grappe": [8, 2, 8], "menage": [2, 1, 1], "poids": [3.5, 2.0, 3.5]})


def test_only_the_strata_declaration_changes_the_sample_contract():
    expected = {
        "dfs": ["df_cover", "df_weights"],
        "df_cover": {
            "file": "../Data1/s00_me_tgo2018.dta",
            "idxvars": {"v": "grappe", "i": ["grappe", "menage"]},
            "myvars": {
                "strata": ["s00q01", "s00q04", {"mapping": "strata_label"}],
                "Rural": ["s00q04", {"mapping": {
                    "Rural": "Rural", "rural": "Rural", "RURAL": "Rural",
                    "Urbain": "Urban", "urbain": "Urban", "URBAIN": "Urban"}}],
            }},
        "df_weights": {
            "file": "../Data1/ehcvm_ponderations_tgo2018.dta",
            "idxvars": {"v": "grappe"},
            "myvars": {"weight": "hhweight", "panel_weight": "hhweight"}},
        "merge_on": ["v"], "final_index": ["i", "t"],
    }
    assert SPEC == expected
    assert MAPPING.strata_label is ehcvm.strata_label
    assert MAPPING._inputs_region_milieu_stratum is ehcvm.inputs_region_milieu_stratum
    assert MAPPING._stamp_region_milieu_stratum is ehcvm.stamp_region_milieu_stratum


def test_full_non_strata_frame_order_ids_weights_rural_and_attrs_are_conserved(synthetic):
    bind, reads = synthetic
    cover, weights = _cover(), _weights()
    originals = [cover.copy(deep=True), weights.copy(deep=True)]
    before = bind(cover, weights, baseline=True).grab_data("sample")
    after = bind(cover, weights).grab_data("sample")
    pd.testing.assert_frame_equal(after.drop(columns=["strata", "Derivation"]),
                                  before.drop(columns="strata"), check_exact=True)
    assert after.attrs == before.attrs
    assert after.index.names == ["i", "t"]
    assert after["v"].tolist() == ["2", "5", "8", "8", "99"]
    assert after.index.get_level_values("i").tolist()[:4] == ["2001", "5001", "8002", "8001"]
    assert after["strata"].iloc[:4].tolist() == [
        "Invented Inland Rural", "Invented Hill rural", "Invented Coast Urbain", "Invented Coast Urbain"]
    assert pd.isna(after["strata"].iloc[-1])
    assert after["Derivation"].dtype == pd.StringDtype()
    assert after["Derivation"].iloc[:4].eq(KEY).all()
    assert pd.isna(after["Derivation"].iloc[-1])
    assert len(reads) == 4
    for frame, original in zip((cover, weights), originals):
        pd.testing.assert_frame_equal(frame, original, check_exact=True)


@pytest.mark.parametrize("region,milieu", [
    (pd.NA, "Urbain"), ("Invented Coast", pd.NA), (None, "Rural"),
    ("Invented Coast", float("nan")), ("", "Rural"), ("Invented Coast", "  "),
    (" \t", "Rural"),
])
def test_missing_or_blank_component_stays_na_without_old_region_fill(synthetic, region, milieu):
    bind, _ = synthetic
    cover = _cover().iloc[:1].copy()
    cover["s00q01"], cover["s00q04"] = region, milieu
    out = bind(cover, _weights().iloc[:1]).grab_data("sample")
    assert len(out) == 1
    assert out["strata"].isna().all() and out["Derivation"].isna().all()
    assert out["weight"].iloc[0] == 17.25


@pytest.mark.parametrize("field,value", [("s00q01", 1), ("s00q04", 2.0), ("s00q01", False)])
def test_nonstring_code_refusal_propagates_through_actual_yaml_extraction(synthetic, field, value):
    bind, _ = synthetic
    cover = _cover().iloc[:1].copy()
    cover[field] = value
    with pytest.raises(ehcvm.StratumError, match="never from codes") as caught:
        bind(cover, _weights().iloc[:1]).grab_data("sample")
    assert type(caught.value) is ehcvm.StratumError
    assert not isinstance(caught.value, (ValueError, KeyError, RuntimeError))


@pytest.mark.parametrize("milieu", ["Rural", "rural", "RURAL", "Urbain", "urbain", "URBAIN"])
def test_strata_keeps_native_case_while_existing_rural_mapping_stays_exact(synthetic, milieu):
    bind, _ = synthetic
    cover = _cover().iloc[:1].copy()
    cover["s00q04"] = milieu
    out = bind(cover, _weights().iloc[:1]).grab_data("sample")
    assert out["strata"].iloc[0] == f"Invented Coast {milieu}"
    assert out["Rural"].iloc[0] == ("Rural" if milieu.lower() == "rural" else "Urban")


def test_hook_preserves_complete_payload_axes_and_typed_attrs_without_mutating_input():
    index = pd.MultiIndex.from_tuples([("b", "2018"), ("a", "2018")], names=["i", "t"])
    frame = pd.DataFrame({"v": ["8", "2"], "strata": ["Invented Coast Urbain", pd.NA],
                          "weight": [17.25, pd.NA], "panel_weight": [17.25, pd.NA],
                          "Rural": ["Urban", "Rural"], "other": ["x", "y"]}, index=index)
    food = derivations.derivation_records("Togo")[
        "Togo::food_acquired::7day-purchase-at-last-purchase-unit-value"]
    frame.attrs = {"id_converted": True, "existing_record": food, "other": ("kept", 9)}
    original = frame.copy(deep=True)
    out = MAPPING.sample(frame)
    pd.testing.assert_frame_equal(out.drop(columns="Derivation"), original, check_exact=True)
    pd.testing.assert_frame_equal(frame, original, check_exact=True)
    assert out.attrs == original.attrs
    assert type(out.attrs["existing_record"]) is type(food)
    assert out["Derivation"].tolist()[0] == KEY and pd.isna(out["Derivation"].iloc[1])
    empty = MAPPING.sample(frame.iloc[:0])
    pd.testing.assert_frame_equal(empty.drop(columns="Derivation"), frame.iloc[:0])
    assert empty["Derivation"].dtype == pd.StringDtype()


def test_registry_is_unique_scoped_and_uses_the_existing_optional_carrier():
    text = (OWN / "_" / "derivations.yml").read_text(encoding="utf-8")
    assert len(re.findall(rf"^{re.escape(KEY)}:", text, re.M)) == 1
    rec = derivations.derivation_records("Togo")[KEY]
    assert list(rec.waves) == ["2018", LATER] and list(rec.columns) == ["strata"]
    assert rec.rows == {} and list(rec.raw_variables) == ["s00q01", "s00q04"]
    assert rec.resolve() is ehcvm.derive_region_milieu_stratum
    assert rec.resolve("inputs").__name__ == "inputs_strata"
    assert rec.inputs == "lsms_library.countries.Togo.2018._.mapping:inputs_strata"
    assert "Actual-row validation UNRUN" in rec.validated_against
    canonical = load_yaml(ROOT.parent / "data_info.yml")["Columns"]["sample"]
    assert canonical["Derivation"]["optional"] is True
    assert KEY not in derivations.records_for("Togo", "food_acquired")


def test_inputs_follow_own_literal_source_and_household_formatter_at_input_grain(synthetic, monkeypatch):
    bind, reads = synthetic
    cover, later_cover = _cover(), _later_cover()
    wave = bind(cover, _weights())
    later = bind(later_cover, _later_weights(), wave_label=LATER)
    visits = []

    class SourceOnlyCountry:
        def __getitem__(self, label):
            visits.append(label)
            return {"2018": wave, LATER: later}[label]

    def country(name):
        assert name == "Togo"
        return SourceOnlyCountry()

    monkeypatch.setattr(C, "Country", country)
    rec = derivations.derivation_records("Togo")[KEY]
    # The scoped call reads each wave of the record's scope from that wave's OWN cover,
    # under that wave's own household formatter, with the labels as the file has them.
    inputs = derivations.call_inputs(rec)
    assert visits == ["2018", LATER]
    assert reads == [(WAVE_DIR / "Data1" / "s00_me_tgo2018.dta").resolve(),
                     (LATER_DIR / "Data" / "s00_me_tgo2021.dta").resolve()]
    assert inputs.index.names == ["t", "i"]
    assert inputs.index.get_level_values("t").tolist() == ["2018"] * 4 + [LATER] * 3
    assert inputs.index.get_level_values("i").tolist() == [
        "8002", "2001", "8001", "5001", "E2_8002", "E2_2001", "E2_8001"]
    pd.testing.assert_frame_equal(
        inputs.reset_index(drop=True),
        pd.concat([cover[["s00q01", "s00q04"]], later_cover[["s00q01", "s00q04"]]],
                  ignore_index=True), check_exact=True)
    # A named wave reads that wave alone.
    only = derivations.call_inputs(rec, wave=LATER)
    assert visits == ["2018", LATER, LATER] and len(reads) == 3
    assert only.index.get_level_values("i").tolist() == ["E2_8002", "E2_2001", "E2_8001"]


def test_2021_22_declares_the_same_construction_with_its_own_cover_and_hook():
    later = LATER_SPEC["df_cover"]
    assert later["file"] == "s00_me_tgo2021.dta"
    assert later["myvars"]["strata"] == SPEC["df_cover"]["myvars"]["strata"] == [
        "s00q01", "s00q04", {"mapping": "strata_label"}]
    assert LATER_MAPPING.strata_label is ehcvm.strata_label
    assert LATER_MAPPING._stamp_region_milieu_stratum is ehcvm.stamp_region_milieu_stratum
    # Everything else in 2021-22's block is the wiring of GH #1697, unchanged.
    assert set(LATER_SPEC) == {"dfs", "df_cover", "df_weights", "merge_on", "final_index"}
    assert LATER_SPEC["merge_on"] == ["i"] and LATER_SPEC["final_index"] == ["i", "t"]
    assert LATER_SPEC["df_weights"]["myvars"] == {"weight": "poids", "panel_weight": "poids"}


def test_2021_22_serves_its_own_lome_label_and_conserves_every_other_field(synthetic):
    bind, _ = synthetic
    before = bind(_later_cover(), _later_weights(), baseline=True, wave_label=LATER).grab_data("sample")
    after = bind(_later_cover(), _later_weights(), wave_label=LATER).grab_data("sample")
    pd.testing.assert_frame_equal(after.drop(columns=["strata", "Derivation"]),
                                  before.drop(columns="strata"), check_exact=True)
    strata = after["strata"].to_dict()
    assert strata == {("E2_2001", LATER): "Invented Inland Rural",
                      ("E2_8001", LATER): "Grand Lomé Urbain",
                      ("E2_8002", LATER): "Grand Lomé Urbain"}
    # 2018's region-6 label is not 2021-22's, and nothing makes them one.
    assert not any("Lomé commune" in v for v in strata.values())
    assert after["Derivation"].eq(KEY).all()
