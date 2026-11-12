"""GH1575: actual CIV wiring with invented, fenced providers; no native data.

Ledger 1575-cotedivoire1575-stratawiring §§2–5. These are construction controls,
not observed-cell counts or normal Country/Feature/cold-cache acceptance.  2021-22
has the same construction since the ruling of 2026-11-12 ("Same rule as #1575"),
from its own cover, with the region upper-cased first: that file labels 23 of its
33 regions in lower case on 2018-19's codes (GH #1697).
"""
from copy import deepcopy
import importlib.util
from pathlib import Path
import pickle
import re
from types import SimpleNamespace

import pandas as pd
import pytest

from lsms_library import country as country_module, data_access, ehcvm, local_tools
from lsms_library.country import Wave
from lsms_library.derivations import BASES, attach, call_inputs, derivation_records
from lsms_library.paths import countries_root
from lsms_library.population import population_records
from lsms_library.yaml_utils import load_yaml

COUNTRY = "CotedIvoire"
WAVE = "2018-19"
ROOT = countries_root() / COUNTRY
COVER = "Menage/s00_me_CIV2018.dta"
WEIGHTS = "Menage/ehcvm_ponderations_CIV2018.dta"
KEY = "CotedIvoire::sample::region-milieu-stratum"
MODULE = ROOT / WAVE / "_" / "mapping.py"
LATER = "2021-22"
LATER_COVER = "s00_me_civ2021.dta"
LATER_WEIGHTS = "ehcvm_ponderations_civ2021.dta"
LATER_MODULE = ROOT / LATER / "_" / "mapping.py"

# The complete pre-change sample declaration, including the original join.
ORIGINAL_SAMPLE = {
    "dfs": ["df_cover", "df_weights"],
    "df_cover": {
        "file": COVER, "idxvars": {"v": "grappe", "i": ["grappe", "menage"]},
        "myvars": {"strata": "s00q01", "Rural": ["s00q04", {"mapping": {
            "Rural": "Rural", "rural": "Rural", "RURAL": "Rural",
            "Urbain": "Urban", "urbain": "Urban", "URBAIN": "Urban"}}]},
    },
    "df_weights": {"file": WEIGHTS, "idxvars": {"v": "grappe"},
                   "myvars": {"weight": "hhweight", "panel_weight": "hhweight"}},
    "merge_on": ["v"], "final_index": ["i", "t"],
}


def _module(path=MODULE):
    spec = importlib.util.spec_from_file_location(f"_gh1575_civ_strata_{path.parent.parent.name}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _deny(*args, **kwargs):
    raise AssertionError("GH1575 pure test attempted an unfenced data/API/write call")


@pytest.fixture(autouse=True)
def no_data(monkeypatch, tmp_path):
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path / "empty-data"))
    monkeypatch.setenv("LSMS_NO_CACHE", "1")
    monkeypatch.setattr(local_tools, "get_dataframe", _deny)
    monkeypatch.setattr(data_access, "get_data_file", _deny)
    monkeypatch.setattr(local_tools, "to_parquet", _deny)
    monkeypatch.setattr(country_module, "to_parquet", _deny)
    monkeypatch.setattr(country_module.Country, "__init__", _deny)


@pytest.fixture
def make_wave(monkeypatch):
    """Real Wave extraction; only resources/providers are invented controls."""
    monkeypatch.setattr(Wave, "resources", property(lambda w: w._fixture_resources))
    monkeypatch.setattr(Wave, "formatting_functions", property(lambda w: dict(w._fixture_functions)))

    def make(*, original=False, label=WAVE):
        functions = local_tools.get_formatting_functions(
            ROOT / "_" / "cotedivoire.py", "_gh1575_civ_country")
        module = MODULE if label == WAVE else LATER_MODULE
        functions.update(local_tools.get_formatting_functions(module, f"_gh1575_civ_wave_{label}"))
        block = load_yaml(ROOT / label / "_" / "data_info.yml")["sample"]
        if original and label == WAVE:
            block = deepcopy(ORIGINAL_SAMPLE)
            functions.pop("sample", None)
        elif original:
            # 2021-22's block as wired before 2026-11-12: the region alone, upper-cased.
            block = deepcopy(block)
            block["df_cover"]["myvars"]["strata"] = ["s00q01", {"mapping": "Region"}]
            functions.pop("sample", None)
        country = SimpleNamespace(name=COUNTRY, resources=load_yaml(ROOT / "_" / "data_scheme.yml"))
        wave = Wave(label, label, country)
        wave._fixture_resources = {"sample": block}
        wave._fixture_functions = functions
        return wave

    return make


def _providers(monkeypatch, cover, weights, **later):
    frames = {COVER: cover, WEIGHTS: weights}
    frames.update({LATER_COVER: later.get("later_cover"), LATER_WEIGHTS: later.get("later_weights")})
    frames = {k: v for k, v in frames.items() if v is not None}
    calls = []

    def read(path, **kwargs):
        # Exact own literals, not guessed aliases or a filesystem reader.
        filename = str(path).split("/Data/", 1)[-1]
        assert filename in frames
        assert kwargs.get("convert_categoricals") is True
        calls.append(filename)
        return frames[filename].copy(deep=True)

    monkeypatch.setattr(local_tools, "get_dataframe", read)
    return calls


def test_only_strata_changes_in_the_complete_sample_declaration():
    actual = load_yaml(ROOT / WAVE / "_" / "data_info.yml")["sample"]
    expected = deepcopy(ORIGINAL_SAMPLE)
    expected["df_cover"]["myvars"]["strata"] = ["s00q01", "s00q04", {"mapping": "strata_label"}]
    assert actual == expected
    # 2021-22 (GH #1697) takes the same construction since 2026-11-12, on its own cover,
    # through a wave-local formatter that upper-cases the region and calls the shared
    # helper; nothing else in its block changes.
    later = load_yaml(ROOT / LATER / "_" / "data_info.yml")["sample"]
    assert later["df_cover"]["file"] == LATER_COVER
    assert later["df_cover"]["myvars"]["strata"] == [
        "s00q01", "s00q04", {"mapping": "strata_label_upper_region"}]
    assert later["dfs"] == ["df_weights", "df_cover"] and later["merge_how"] == "left"
    assert list(derivation_records(COUNTRY)[KEY].waves) == [WAVE, LATER]


def test_real_yaml_extraction_merge_and_hook_conserve_every_other_field(monkeypatch, make_wave):
    cover = pd.DataFrame({
        "grappe": [7, 2, 7, 4, 6, 9, 3], "menage": [3, 8, 1, 9, 2, 1, 4],
        "s00q01": [" Autonome d'Abidjan ", "Folon", "Autonome d'Abidjan", pd.NA,
                   "Gôh", "Yamoussoukro", "  "],
        "s00q04": [" Rural ", "Urbain", "Urbain", "Rural", pd.NA, "   ", "rural"],
        "unrequested": [901, 902, 903, 904, 905, 906, 907],
    })
    weights = pd.DataFrame({"grappe": [2, 7, 8, 4, 9],
                            "hhweight": [250.5, 88.0, 67.25, float("nan"), 13.0]})
    original_cover, original_weights = cover.copy(deep=True), weights.copy(deep=True)
    calls = _providers(monkeypatch, cover, weights)
    old = make_wave(original=True).grab_data("sample")
    new = make_wave().grab_data("sample")
    assert calls == [COVER, WEIGHTS, COVER, WEIGHTS]
    pd.testing.assert_frame_equal(new.drop(columns=["strata", "Derivation"]),
                                  old.drop(columns="strata"))
    assert new.attrs == old.attrs
    assert new.index.names == ["i", "t"]
    assert set(new.index.get_level_values("i").dropna()) == {
        "7003", "2008", "7001", "4009", "6002", "9001", "3004"}
    assert new.index.get_level_values("i").isna().sum() == 1  # weight-only outer orphan survives
    expected = {"7003": "Autonome d'Abidjan Rural", "2008": "Folon Urbain",
                "7001": "Autonome d'Abidjan Urbain"}
    for household, label in expected.items():
        assert new.loc[(household, WAVE), "strata"] == label
    absent = ~new.index.get_level_values("i").isin(expected)
    assert new.loc[absent, "strata"].isna().all()
    assert new["Derivation"].dtype == pd.StringDtype()
    assert (new.loc[~absent, "Derivation"] == KEY).all()
    assert new.loc[absent, "Derivation"].isna().all()
    assert new.loc[("7003", WAVE), "weight"] == 88.0  # raw, no normalization
    assert new.loc[("7003", WAVE), "Rural"] == " Rural "  # existing payload exact
    pd.testing.assert_frame_equal(cover, original_cover)
    pd.testing.assert_frame_equal(weights, original_weights)


@pytest.mark.parametrize("region,milieu", [
    (pd.NA, "Rural"), ("Folon", pd.NA), (None, "Rural"), ("Folon", float("nan")),
    ("", "Rural"), ("Folon", " \t "), ("  ", "Rural"), (pd.NA, pd.NA),
])
def test_missing_or_blank_has_no_region_only_fallback(monkeypatch, make_wave, region, milieu):
    cover = pd.DataFrame({"grappe": [2], "menage": [8], "s00q01": [region], "s00q04": [milieu]})
    weights = pd.DataFrame({"grappe": [2], "hhweight": [23.5]})
    _providers(monkeypatch, cover, weights)
    out = make_wave().grab_data("sample")
    assert out["strata"].isna().all() and out["Derivation"].isna().all()
    assert out.loc[("2008", WAVE), "weight"] == 23.5


@pytest.mark.parametrize("region,milieu,operand", [
    (1, "Rural", "region"), ("Folon", 2, "milieu"), (1.0, 2.0, "region"),
    (True, "Rural", "region"),
])
def test_nonstring_refusal_reaches_the_actual_wave_path(monkeypatch, make_wave, region, milieu, operand):
    cover = pd.DataFrame({"grappe": [2], "menage": [8], "s00q01": [region], "s00q04": [milieu]})
    calls = _providers(monkeypatch, cover, pd.DataFrame({"grappe": [2], "hhweight": [9.0]}))
    with pytest.raises(ehcvm.StratumError, match=operand + ".*never from codes") as caught:
        make_wave().grab_data("sample")
    assert type(caught.value) is ehcvm.StratumError
    assert not isinstance(caught.value, (ValueError, KeyError, RuntimeError, AttributeError))
    assert type(pickle.loads(pickle.dumps(caught.value))) is ehcvm.StratumError
    assert calls == [COVER]


@pytest.mark.parametrize("empty", [False, True])
def test_hook_preserves_axes_order_all_payload_and_typed_attrs(empty):
    index = pd.MultiIndex.from_tuples([("7003", WAVE), ("2008", WAVE)], names=["i", "t"])
    frame = pd.DataFrame({"strata": ["Autonome d'Abidjan Rural", pd.NA], "v": ["7", "2"],
                          "weight": [88.0, pd.NA], "panel_weight": [88.0, pd.NA],
                          "Rural": ["Rural", "Urban"], "extra": ["keep", "also keep"]}, index=index)
    if empty:
        frame = frame.iloc[:0]
    rec = derivation_records(COUNTRY)[KEY]
    population = population_records(COUNTRY)[WAVE]
    frame.attrs = {"id_converted": True, "population": {WAVE: population},
                   "typed_registry": rec, "custom": {"tuple": (1, "a")}}
    before = frame.copy(deep=True)
    out = _module().sample(frame)
    pd.testing.assert_frame_equal(out.drop(columns="Derivation"), before)
    pd.testing.assert_frame_equal(frame, before)
    assert out.attrs == before.attrs and out is not frame
    assert type(out.attrs["typed_registry"]) is type(rec)
    assert type(out.attrs["population"][WAVE]) is type(population)
    assert isinstance(out["Derivation"].dtype, pd.StringDtype)
    assert out.loc[out["strata"].isna(), "Derivation"].isna().all()
    attach(out, COUNTRY, "sample")
    summary = out.attrs["derivations"][COUNTRY][KEY]
    assert summary["rows"] == int(out["strata"].notna().sum())
    assert summary["columns"] == ["strata"]
    assert out.attrs["id_converted"] is True


def test_registry_is_unique_scoped_optional_and_resolves_unchanged_helpers():
    text = (ROOT / "_" / "derivations.yml").read_text()
    assert len(re.findall(rf"^{re.escape(KEY)}:", text, flags=re.M)) == 1
    rec = derivation_records(COUNTRY)[KEY]
    assert rec.waves == [WAVE, LATER] and rec.columns == ["strata"] and rec.rows == {}
    assert rec.raw_variables == ["s00q01", "s00q04"]
    assert rec.resolve() is ehcvm.derive_region_milieu_stratum
    assert rec.inputs == "lsms_library.countries.CotedIvoire.2018-19._.mapping:inputs_strata"
    assert callable(rec.resolve("inputs"))
    module = _module()
    assert module.strata_label is ehcvm.strata_label
    assert module._inputs_region_milieu_stratum is ehcvm.inputs_region_milieu_stratum
    assert module._stamp_region_milieu_stratum is ehcvm.stamp_region_milieu_stratum
    assert "Actual-row validation UNRUN" in rec.validated_against
    assert all(a["basis"] in BASES for a in rec.assumptions)
    schema = load_yaml(Path(country_module.__file__).parent / "data_info.yml")
    assert schema["Columns"]["sample"]["Derivation"]["optional"] is True


def test_registered_inputs_use_own_cover_and_original_names_only(monkeypatch, make_wave):
    wave, later = make_wave(), make_wave(label=LATER)
    requested = []

    class InventedCountry:
        def __getitem__(self, value):
            requested.append(value)
            return {WAVE: wave, LATER: later}[value]

    def provider(name):
        assert name == COUNTRY
        return InventedCountry()

    monkeypatch.setattr(country_module, "Country", provider)
    cover = pd.DataFrame({"grappe": [7, 2], "menage": [3, 8],
                          "s00q01": [" Autonome d'Abidjan ", "Folon"],
                          "s00q04": ["Rural", "Urbain"]})
    later_cover = pd.DataFrame({"grappe": [7, 2], "menage": [3, 8],
                                "s00q01": ["poro", "gÔh"], "s00q04": ["Rural", "Urbain"]})
    before, later_before = cover.copy(deep=True), later_cover.copy(deep=True)
    calls = _providers(monkeypatch, cover, pd.DataFrame(), later_cover=later_cover)
    rec = derivation_records(COUNTRY)[KEY]
    actual = call_inputs(rec)
    # Each wave's OWN cover, original names, and the labels AS THE FILE HAS THEM: the
    # upper-casing is the served value's, not the inputs'.
    expected = pd.concat([before[["s00q01", "s00q04"]], later_before[["s00q01", "s00q04"]]],
                         ignore_index=True)
    expected.index = pd.MultiIndex.from_tuples(
        [(WAVE, "7003"), (WAVE, "2008"), (LATER, "E2_7003"), (LATER, "E2_2008")], names=["t", "i"])
    pd.testing.assert_frame_equal(actual, expected)
    pd.testing.assert_frame_equal(cover, before)
    assert calls == [COVER, LATER_COVER] and requested == [WAVE, LATER]
    only = call_inputs(rec, wave=LATER)
    assert only.index.get_level_values("i").tolist() == ["E2_7003", "E2_2008"]
    assert requested == [WAVE, LATER, LATER] and calls == [COVER, LATER_COVER, LATER_COVER]


def _later_weights(grappes, menages):
    return pd.DataFrame({"grappe": grappes, "menage": menages,
                         "poids": [float(10 + k) for k in range(len(grappes))]})


def test_2021_22_upper_cases_the_region_and_conserves_every_other_field(monkeypatch, make_wave):
    later_cover = pd.DataFrame({
        "grappe": [7, 2, 7, 4, 6], "menage": [3, 8, 1, 9, 2],
        "s00q01": ["poro", "gÔh", "AUTONOME D'ABIDJAN", " poro ", pd.NA],
        "s00q04": ["Rural", "Urbain", "Urbain", "Rural", "Rural"],
    })
    # The weights file is primary (merge_how: left): a cover-only household is not served.
    weights = _later_weights([7, 2, 7, 4, 6], [3, 8, 1, 9, 2])
    calls = _providers(monkeypatch, pd.DataFrame(), pd.DataFrame(),
                       later_cover=later_cover, later_weights=weights)
    old = make_wave(original=True, label=LATER).grab_data("sample")
    new = make_wave(label=LATER).grab_data("sample")
    assert calls == [LATER_WEIGHTS, LATER_COVER] * 2
    pd.testing.assert_frame_equal(new.drop(columns=["strata", "Derivation"]),
                                  old.drop(columns="strata"))
    got = new["strata"].droplevel("t").to_dict()
    assert pd.isna(got.pop("E2_6002"))  # a missing region: NA, never the milieu alone
    assert got == {"E2_7003": "PORO Rural", "E2_2008": "GÔH Urbain",
                   "E2_7001": "AUTONOME D'ABIDJAN Urbain", "E2_4009": "PORO Rural"}
    # The region part is the same wave's Region, by the same formatter.
    region = make_wave(label=LATER).formatting_functions["Region"]
    assert {region(r) for r in ["poro", "gÔh"]} == {"PORO", "GÔH"}
    assert (new["Derivation"].isna() == new["strata"].isna()).all()
    assert (new.loc[new["strata"].notna(), "Derivation"] == KEY).all()


@pytest.mark.parametrize("region,milieu", [
    (pd.NA, "Rural"), ("poro", pd.NA), ("", "Rural"), ("poro", " \t "), ("  ", "Urbain"),
])
def test_2021_22_missing_or_blank_is_na(monkeypatch, make_wave, region, milieu):
    later_cover = pd.DataFrame({"grappe": [2], "menage": [8], "s00q01": [region], "s00q04": [milieu]})
    _providers(monkeypatch, pd.DataFrame(), pd.DataFrame(),
               later_cover=later_cover, later_weights=_later_weights([2], [8]))
    out = make_wave(label=LATER).grab_data("sample")
    assert out["strata"].isna().all() and out["Derivation"].isna().all()


@pytest.mark.parametrize("region,milieu,operand", [(3, "Rural", "region"), ("poro", 2, "milieu")])
def test_2021_22_refuses_a_code(monkeypatch, make_wave, region, milieu, operand):
    later_cover = pd.DataFrame({"grappe": [2], "menage": [8], "s00q01": [region], "s00q04": [milieu]})
    _providers(monkeypatch, pd.DataFrame(), pd.DataFrame(),
               later_cover=later_cover, later_weights=_later_weights([2], [8]))
    with pytest.raises(ehcvm.StratumError, match=operand + ".*never from codes") as caught:
        make_wave(label=LATER).grab_data("sample")
    assert type(caught.value) is ehcvm.StratumError


def test_2021_22_formatter_and_hook_are_the_shared_helpers():
    module = _module(LATER_MODULE)
    assert module._derive_region_milieu_stratum is ehcvm.derive_region_milieu_stratum
    assert module._stamp_region_milieu_stratum is ehcvm.stamp_region_milieu_stratum
    row = pd.Series(["gÔh", "Urbain"])
    assert module.strata_label_upper_region(row) == "GÔH Urbain"
    assert module.strata_label_upper_region(pd.Series(["poro", pd.NA])) is pd.NA
