"""Benin GH1575 wiring, 2018-19 and 2021-22, using invented frames and actual extractors only.

No native coding/card, observed count, Country build or cache is an oracle here.
Ledger: .coder/ledger/1575-benin1575-stratawiring.md §§2–5.  2021-22 has the same
construction since the ruling of 2026-11-12 ("Same rule as #1575"), from its own cover
(GH #1697); its weights file's own ``strat`` is not read.
"""
from copy import deepcopy
import importlib
import re
from types import MethodType, SimpleNamespace

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from lsms_library import country as country_module, ehcvm, local_tools
from lsms_library.country import Wave
from lsms_library.derivations import call_inputs, derivation_records
from lsms_library.paths import countries_root
from lsms_library.yaml_utils import load_yaml

ROOT = countries_root()
WAVE = "2018-19"
LATER = "2021-22"
KEY = "Benin::sample::region-milieu-stratum"
MODULE = "lsms_library.countries.Benin.2018-19._.mapping"
LATER_MODULE = "lsms_library.countries.Benin.2021-22._.mapping"

# The exact pre-change sample declaration at source9f, independent of candidate.
ORIGINAL = {
    "dfs": ["df_cover", "df_weights"],
    "df_cover": {
        "file": "s00_me_ben2018.dta",
        "idxvars": {"v": "grappe", "i": ["grappe", "menage"]},
        "myvars": {"strata": "s00q01", "Rural": ["s00q04", {"mapping": {
            "Rural": "Rural", "rural": "Rural", "RURAL": "Rural",
            "Urbain": "Urban", "urbain": "Urban", "URBAIN": "Urban",
        }}]},
    },
    "df_weights": {
        "file": "ehcvm_ponderations_ben2018.dta",
        "idxvars": {"v": "grappe"},
        "myvars": {"weight": "hhweight", "panel_weight": "hhweight"},
    },
    "merge_on": ["v"], "final_index": ["i", "t"],
}


@pytest.fixture(autouse=True)
def no_providers(monkeypatch):
    def denied(*args, **kwargs):
        pytest.fail("unexpected real data/provider or Country/Feature call")
    monkeypatch.setattr(local_tools, "get_dataframe", denied)
    monkeypatch.setattr(local_tools, "to_parquet", denied)
    monkeypatch.setattr(country_module, "Country", denied)
    from lsms_library import data_access, feature
    monkeypatch.setattr(data_access, "get_data_file", denied)
    monkeypatch.setattr(feature.Feature, "__call__", denied)


@pytest.fixture
def wiring():
    module = importlib.import_module(MODULE)
    # Own unchanged household formatter; no country constructor or data call.
    ids = importlib.import_module("lsms_library.countries.Benin._.benin")
    spec = load_yaml(ROOT / "Benin" / WAVE / "_" / "data_info.yml")["sample"]
    scheme = load_yaml(ROOT / "Benin" / "_" / "data_scheme.yml")
    wave = SimpleNamespace(
        resources={"sample": spec}, year=WAVE,
        name="Benin/2018-19", file_path=ROOT / "Benin" / WAVE,
        country=SimpleNamespace(resources=scheme, name="Benin"),
        formatting_functions={"i": ids.i, "strata_label": module.strata_label,
                              "sample": module.sample},
    )
    wave.column_mapping = MethodType(Wave.column_mapping, wave)
    wave._cartesian_keys = MethodType(Wave._cartesian_keys, wave)
    return module, wave, spec


def _later_wave(country):
    """2021-22's own block and functions, on the same source-only seam as ``wiring``."""
    module = importlib.import_module(LATER_MODULE)
    spec = load_yaml(ROOT / "Benin" / LATER / "_" / "data_info.yml")["sample"]
    wave = SimpleNamespace(
        resources={"sample": spec}, year=LATER,
        name="Benin/2021-22", file_path=ROOT / "Benin" / LATER, country=country,
        formatting_functions={"i": module.i, "strata_label": module.strata_label,
                              "sample": module.sample},
    )
    wave.column_mapping = MethodType(Wave.column_mapping, wave)
    wave._cartesian_keys = MethodType(Wave._cartesian_keys, wave)
    return module, wave, spec


def _later_frames():
    return {
        "s00_me_ben2021.dta": pd.DataFrame({
            "grappe": [42, 7, 42], "menage": [9, 2, 1],
            "s00q01": ["littoral", " Autre inventé ", "littoral"],
            "s00q04": ["Urbain", "Rural", "Urbain"],
        }),
        "ehcvm_ponderations_ben2021.dta": pd.DataFrame({
            "grappe": [42, 7, 42], "menage": [9, 2, 1], "poids": [17.25, 4.5, 17.25],
        }),
    }


def _extract(wave, block, frames, monkeypatch, t=WAVE):
    """Use actual YAML mapping/extraction/merge, with a closed invented provider."""
    seen = []

    def invented(path, **kwargs):
        name = str(path).rsplit("/", 1)[-1]
        assert name in frames
        assert kwargs == {"convert_categoricals": True, "encoding": None}
        seen.append(name)
        return frames[name].copy(deep=True)

    monkeypatch.setattr(local_tools, "get_dataframe", invented)
    extracted = []
    for name in block["dfs"]:
        mapping = wave.column_mapping(name, block[name])
        assert mapping.pop("df_edit") is None
        assert len(mapping) == 1
        filename, cols = next(iter(mapping.items()))
        frame = local_tools.df_data_grabber(filename, cols["idxvars"], **cols["myvars"])
        frame["t"] = t
        extracted.append(frame.reset_index())
    flat = Wave._merge_subframes(
        wave, *extracted, ["t", *block["merge_on"]], "sample", *block["dfs"],
        how=block.get("merge_how", "outer"),
    )
    return flat.set_index(block["final_index"]), seen


def _frames():
    # Deliberately unsorted, duplicate clusters, absent weight and weight-only row.
    return {
        "s00_me_ben2018.dta": pd.DataFrame({
            "grappe": [42, 7, 42, 3], "menage": [9, 2, 1, 4],
            "s00q01": [" Département inventé ", "Autre inventé", "Département inventé", "Autre inventé"],
            "s00q04": ["Urbain", "rural", "Rural", "RURAL"],
        }),
        "ehcvm_ponderations_ben2018.dta": pd.DataFrame({
            "grappe": [42, 99, 7], "hhweight": [17.25, 4.5, float("nan")],
        }),
    }


def test_only_the_strata_declaration_changes(wiring):
    module, wave, spec = wiring
    expected = deepcopy(ORIGINAL)
    expected["df_cover"]["myvars"]["strata"] = ["s00q01", "s00q04", {"mapping": "strata_label"}]
    assert spec == expected
    assert module.strata_label is ehcvm.strata_label
    assert module._inputs_region_milieu_stratum is ehcvm.inputs_region_milieu_stratum
    assert module._stamp_region_milieu_stratum is ehcvm.stamp_region_milieu_stratum
    mapping = wave.column_mapping("sample", spec["df_cover"])
    assert mapping.pop("df_edit") is module.sample
    assert mapping["s00_me_ben2018.dta"]["myvars"]["strata"][1] is ehcvm.strata_label


def test_full_nonstrata_payload_order_ids_weights_and_attrs_are_conserved(wiring, monkeypatch):
    module, wave, spec = wiring
    old, old_reads = _extract(wave, ORIGINAL, _frames(), monkeypatch)
    candidate, new_reads = _extract(wave, spec, _frames(), monkeypatch)
    assert old_reads == new_reads == ["s00_me_ben2018.dta", "ehcvm_ponderations_ben2018.dta"]
    assert_frame_equal(candidate.drop(columns="strata"), old.drop(columns="strata"))
    # Exact own ID construction, order and orphan preservation, independent of strata.
    assert candidate.reset_index()["v"].tolist() == ["3", "42", "42", "7", "99"]
    assert candidate.index.get_level_values("i").tolist()[:4] == ["3004", "42009", "42001", "7002"]
    assert candidate["weight"].iloc[1:3].tolist() == [17.25, 17.25]
    assert candidate["panel_weight"].equals(candidate["weight"])
    assert candidate["Rural"].iloc[:4].tolist() == ["Rural", "Urban", "Rural", "Rural"]
    assert candidate["strata"].iloc[:4].tolist() == [
        "Autre inventé RURAL", "Département inventé Urbain", "Département inventé Rural", "Autre inventé rural"]
    assert pd.isna(candidate["strata"].iloc[4])
    rec = derivation_records("Benin")[KEY]
    candidate.attrs = {"id_converted": True, "derivations": {"Benin": {KEY: rec}},
                       "lineage": {"synthetic": ["cover", "weights"]}}
    before = candidate.copy(deep=True)
    out = module.sample(candidate)
    assert_frame_equal(candidate, before)
    assert_frame_equal(out.drop(columns="Derivation"), before)
    assert out.attrs == before.attrs
    assert type(out.attrs["derivations"]["Benin"][KEY]) is type(rec)
    assert isinstance(out["Derivation"].dtype, pd.StringDtype)
    assert out["Derivation"].iloc[:4].tolist() == [KEY] * 4
    assert pd.isna(out["Derivation"].iloc[4])


@pytest.mark.parametrize("region,milieu", [
    (None, "Rural"), (pd.NA, "Urbain"), (float("nan"), "Rural"),
    ("Inventé", None), ("Inventé", pd.NA), ("Inventé", float("nan")),
    ("", "Rural"), ("  ", "Urbain"), ("Inventé", ""), ("Inventé", " \t"),
    (pd.NA, pd.NA),
])
def test_missing_or_blank_operand_never_falls_back_to_region(wiring, monkeypatch, region, milieu):
    module, wave, spec = wiring
    frames = _frames()
    frames["s00_me_ben2018.dta"] = pd.DataFrame({
        "grappe": [42], "menage": [9], "s00q01": [region], "s00q04": [milieu]})
    frame, _ = _extract(wave, spec, frames, monkeypatch)
    out = module.sample(frame)
    assert out["strata"].isna().all()
    assert out["Derivation"].isna().all()


@pytest.mark.parametrize("region,milieu", [(1, "Rural"), ("Inventé", 2), (1.5, "Urbain"), ("Inventé", False)])
def test_actual_declared_formatter_preserves_dedicated_refusal(wiring, monkeypatch, region, milieu):
    _, wave, spec = wiring
    frames = _frames()
    frames["s00_me_ben2018.dta"] = pd.DataFrame({
        "grappe": [42], "menage": [9], "s00q01": [region], "s00q04": [milieu]})
    with pytest.raises(ehcvm.StratumError, match="never from codes") as caught:
        _extract(wave, spec, frames, monkeypatch)
    assert not isinstance(caught.value, (ValueError, KeyError, RuntimeError, AttributeError))


def test_empty_stamp_and_existing_sample_rowkey_contract(wiring):
    module, _, _ = wiring
    frame = pd.DataFrame({"strata": pd.Series(dtype="string"), "v": pd.Series(dtype="string")})
    frame.attrs = {"id_converted": True}
    result = module.sample(frame)
    assert result.empty and isinstance(result["Derivation"].dtype, pd.StringDtype)
    assert_frame_equal(result.drop(columns="Derivation"), frame)
    # The unchanged shared helper owns sample's carrier; do not invent a union.
    frame = pd.DataFrame({"strata": ["Inventé Rural", pd.NA], "Derivation": ["old", "old"]})
    expected = ehcvm.stamp_region_milieu_stratum(frame, "Benin")
    assert_frame_equal(module.sample(frame), expected)


def test_registry_scope_optional_carrier_and_actual_input_callable(wiring, monkeypatch):
    module, wave, _ = wiring
    registry = ROOT / "Benin" / "_" / "derivations.yml"
    assert len(re.findall(rf"^{re.escape(KEY)}:", registry.read_text(), re.M)) == 1
    rec = derivation_records("Benin")[KEY]
    assert rec.resolve() is ehcvm.derive_region_milieu_stratum
    # Registry resolution loads country files by path, creating a fresh module.
    assert rec.inputs == MODULE + ":inputs_strata"
    assert rec.resolve("inputs").__code__ == module.inputs_strata.__code__
    assert list(rec.waves) == [WAVE, LATER]
    assert list(rec.columns) == ["strata"] and rec.rows == {}
    assert list(rec.raw_variables) == ["s00q01", "s00q04"]
    assert "actual-row validation UNRUN" in rec.validated_against
    schema = load_yaml(ROOT.parent / "data_info.yml")
    assert schema["Columns"]["sample"]["Derivation"]["optional"] is True
    reads, visits = [], []
    _, later, _ = _later_wave(wave.country)
    sources = {
        str(ROOT / "Benin" / WAVE / "Data" / "s00_me_ben2018.dta"): _frames()["s00_me_ben2018.dta"],
        str(ROOT / "Benin" / LATER / "Data" / "s00_me_ben2021.dta"): _later_frames()["s00_me_ben2021.dta"],
    }

    class InventedCountry:
        def __getitem__(self, selected_wave):
            visits.append(selected_wave)
            return {WAVE: wave, LATER: later}[selected_wave]

    def country(name):
        assert name == "Benin"
        return InventedCountry()

    def cover(path, **kwargs):
        assert path in sources
        reads.append(path)
        return sources[path].copy()

    monkeypatch.setattr(country_module, "Country", country)
    monkeypatch.setattr(local_tools, "get_dataframe", cover)
    # The scoped call reads each wave of the record's scope from its OWN cover, under
    # its own household formatter, with the labels as the file has them.
    inputs = call_inputs(rec)
    assert visits == [WAVE, LATER] and reads == list(sources)
    assert inputs.index.names == ["t", "i"]
    assert inputs.index.get_level_values("t").tolist() == [WAVE] * 4 + [LATER] * 3
    assert inputs.index.get_level_values("i").tolist() == [
        "42009", "7002", "42001", "3004", "E2_42009", "E2_7002", "E2_42001"]
    expected = pd.concat([_frames()["s00_me_ben2018.dta"][["s00q01", "s00q04"]],
                          _later_frames()["s00_me_ben2021.dta"][["s00q01", "s00q04"]]],
                         ignore_index=True)
    assert_frame_equal(inputs.reset_index(drop=True), expected)
    only = call_inputs(rec, wave=LATER)
    assert visits == [WAVE, LATER, LATER] and len(reads) == 3
    assert only.index.get_level_values("i").tolist() == ["E2_42009", "E2_7002", "E2_42001"]


def test_2021_22_declares_the_same_construction_from_its_own_cover(wiring):
    _, wave, spec = wiring
    module, later, later_spec = _later_wave(wave.country)
    assert later_spec["df_cover"]["file"] == "s00_me_ben2021.dta"
    assert later_spec["df_cover"]["myvars"]["strata"] == spec["df_cover"]["myvars"]["strata"]
    assert module.strata_label is ehcvm.strata_label
    assert module._stamp_region_milieu_stratum is ehcvm.stamp_region_milieu_stratum
    # The weights file's own `strat` is a comparison (CONTENTS.org), never a served column.
    assert later_spec["df_weights"]["myvars"] == {"weight": "poids", "panel_weight": "poids"}
    assert "'strat'" not in str(later_spec)
    mapping = later.column_mapping("sample", later_spec["df_cover"])
    assert mapping.pop("df_edit") is module.sample
    assert mapping["s00_me_ben2021.dta"]["myvars"]["strata"][1] is ehcvm.strata_label


def test_2021_22_extraction_and_hook_conserve_every_other_field(wiring, monkeypatch):
    _, wave, _ = wiring
    module, later, later_spec = _later_wave(wave.country)
    old_spec = deepcopy(later_spec)
    old_spec["df_cover"]["myvars"]["strata"] = "s00q01"
    old, _ = _extract(later, old_spec, _later_frames(), monkeypatch, t=LATER)
    new, seen = _extract(later, later_spec, _later_frames(), monkeypatch, t=LATER)
    assert seen == ["s00_me_ben2021.dta", "ehcvm_ponderations_ben2021.dta"]
    assert_frame_equal(new.drop(columns="strata"), old.drop(columns="strata"))
    assert new["strata"].to_dict() == {("E2_42009", LATER): "littoral Urbain",
                                       ("E2_7002", LATER): "Autre inventé Rural",
                                       ("E2_42001", LATER): "littoral Urbain"}
    out = module.sample(new)
    assert_frame_equal(out.drop(columns="Derivation"), new)
    assert out["Derivation"].eq(KEY).all()
