"""Invented source-role controls for the test-only Niger CI proposal.

No producer, Country/Feature call, decoder or actual raw source is used.
Ledger 1687-niger-ci-oracle-19b6e1c5 §§2–5 binds the independent oracle.
"""
import importlib.util
from pathlib import Path

import pandas as pd
import pytest

from lsms_library.transformations import harvest_kg


def _load(name):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(name + ".py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


O = _load("test_niger_npk_grade")
C = _load("test_gh1460_crop_level")
SPECS = [(w, n, p) for w, specs in O.INPUT_SOURCES.items() for n, p in specs]


def _invented(wave, prefix):
    """Own fields differ by source. The unused money/unit-like field is a trap."""
    ecvma = wave in O.ECVMA
    suffix = lambda x: x.upper() if prefix == "AS02CQ" else x
    slot = prefix + ("02" if ecvma else "01")
    gate = prefix + ("03" if ecvma else "02")
    qty = prefix + suffix("05a" if ecvma else "03a")
    unit = prefix + suffix("05b" if ecvma else "03b")
    bought = prefix + ("07" if ecvma else "05")
    qb = prefix + suffix("08a" if ecvma else "07a")
    numeric = pd.DataFrame({slot: [5, 11, 12, 13, 14, 15],
                            gate: [1, 1, 1, 2, pd.NA, 1],
                            qty: [99., 0., pd.NA, 900., 901., pd.NA],
                            unit: [1, 99, pd.NA, 1, 1, pd.NA],
                            bought: [1, 2, 9, 1, 1, 9],
                            qb: [1.25, pd.NA, 8., 9., 10., pd.NA],
                            prefix + suffix("08b"): [7777.] * 6})
    labels = numeric.copy(deep=True)
    labels[slot] = ["own NPK card", "own seed card", "own seed card"] * 2
    labels[unit] = ["kg", "manquant", pd.NA, "kg", "kg", pd.NA]
    if not ecvma:
        labels[bought] = ["Oui", "Non", pd.NA, "Oui", "Oui", pd.NA]
    return numeric, labels


MAPS = dict(input_map={"own NPK card": O.GRADED, "own seed card": "Seed"},
            unit_map={"kg": "Kg", "manquant": "Unknown"})


@pytest.mark.parametrize("wave,name,prefix", SPECS)
def test_own_roles_keep_zero_missing_flags_and_amounts(wave, name, prefix):
    n, c = _invented(wave, prefix)
    before = n.copy(deep=True), c.copy(deep=True)
    out = O._project_input_source(wave, name, prefix, n, c, **MAPS)
    assert len(out) == 3  # No/NA gates and one all-payload-NA row excluded.
    assert out["Quantity"].tolist()[:2] == [99., 0.]
    assert pd.isna(out["Quantity"].iloc[2])
    assert out["Quantity_purchased"].iloc[0] == 1.25
    assert out["Purchased"].tolist()[:2] == [True, False]
    assert pd.isna(out["Purchased"].iloc[2])
    assert out["u"].tolist() == ["Kg", "Unknown", "Unknown"]
    pd.testing.assert_frame_equal(n, before[0])
    pd.testing.assert_frame_equal(c, before[1])


@pytest.mark.parametrize("wave,name,prefix", SPECS)
def test_wrong_source_field_roles_refuse(wave, name, prefix):
    n, c = _invented(wave, prefix)
    with pytest.raises(AssertionError, match="wrong source"):
        O._project_input_source(wave, "wrong-source.dta", prefix, n, c, **MAPS)


def test_two_sources_keep_equal_seed_slots_distinct():
    parts = []
    for wave, name, prefix in SPECS[:2]:
        n, c = _invented(wave, prefix)
        out = O._project_input_source(wave, name, prefix, n, c, **MAPS)
        parts.append(out)
        assert out["input_obs"].str.endswith("=5").iloc[0]
        assert out["input_obs"].iloc[1] != out["input_obs"].iloc[2]
    assert set(parts[0]["input_obs"]).isdisjoint(parts[1]["input_obs"])
    pd.testing.assert_frame_equal(parts[0].drop(columns=["input_obs", "source"]),
                                  parts[1].drop(columns=["input_obs", "source"]))


def test_source_union_responds_to_own_dry_operands(monkeypatch):
    monkeypatch.setattr(O, "_map_table", lambda name: MAPS[
        "input_map" if name == "harmonize_input" else "unit_map"])
    calls = []
    def reader(wave, name):
        prefix = dict(O.INPUT_SOURCES[wave])[name]
        calls.append((wave, name))
        n, c = _invented(wave, prefix)
        if prefix == "as03cq":
            n.loc[0, "as03cq05a"] = 17.5
            c.loc[0, "as03cq05b"] = pd.NA
        return n, c
    out = O._expected_plot_inputs(read=reader)
    assert calls == [(w, n) for w, n, _ in SPECS]
    dry = out[out["source"].str.endswith("ecvmaas3c_p1.dta")]
    assert len(dry) == 3 and dry["Quantity"].iloc[0] == 17.5
    assert dry["u"].eq("Unknown").all()
    assert len(out.loc[(out["t"] == "2011-12") & (out["input"] == O.GRADED)]) == 2


@pytest.mark.parametrize("wave,name,prefix", SPECS[3:])
def test_own_ehcvm_card_does_not_gain_an_ecvma_grade(wave, name, prefix):
    n, c = _invented(wave, prefix)
    c.loc[0, "s16bq01"] = "Engrais inorganiques - NPK/Formule unique"
    maps = dict(MAPS, input_map=dict(MAPS["input_map"], **{
        "Engrais inorganiques - NPK/Formule unique": O.BARE}))
    out = O._project_input_source(wave, name, prefix, n, c, **maps)
    assert out["input"].iloc[0] == O.BARE
    assert not out["input"].eq(O.GRADED).any()


@pytest.mark.parametrize("mutation", ["old-rainy-only", "drop-unknown", "blank-missing-flag", "wrong-wave"])
def test_old_coverage_and_missingness_mutations_are_detected(mutation):
    parts = [O._project_input_source(w, n, p, *_invented(w, p), **MAPS)
             for w, n, p in SPECS[:2]]
    expected = pd.concat(parts, ignore_index=True)
    actual = expected.copy(deep=True)
    if mutation == "old-rainy-only":
        actual = parts[0]
    elif mutation == "drop-unknown":
        actual = actual.loc[actual["u"] != "Unknown"]
    elif mutation == "blank-missing-flag":
        # A falsely retained hollow record changes the count obligation.
        extra = actual.iloc[[0]].copy()
        extra[["Quantity", "Quantity_purchased"]] = pd.NA
        extra["Purchased"] = False
        actual = pd.concat([actual, extra], ignore_index=True)
    else:
        actual.loc[actual["source"].str.endswith("ecvmaas3c_p1.dta"), "t"] = "2014-15"
    with pytest.raises(AssertionError):
        O._assert_plot_input_counts(actual, expected, by_wave=mutation == "wrong-wave")


@pytest.mark.parametrize("mutation", [None, "country-lost", "wrong-country", "crop-lost", "quantity"])
def test_legitimate_country_axis_and_complete_harvest_payload(mutation):
    native = C._crop_production("crop")
    assert "crop" in native.index.names and "j" not in native.index.names
    canonical = native.rename_axis(index={"crop": "j"})
    country = harvest_kg(native)
    feature = harvest_kg(pd.concat({"Niger": canonical}, names=["country"]))
    if mutation == "country-lost":
        feature = feature.droplevel("country")
    elif mutation == "wrong-country":
        feature = feature.rename(index={"Niger": "Other"}, level="country")
    elif mutation == "crop-lost":
        feature = feature.droplevel("j")
    elif mutation == "quantity":
        feature = feature.copy()
        feature.iloc[0, feature.columns.get_loc("Harvest_kg")] += 1
    if mutation is None:
        C._assert_niger_country_feature_harvest(country, feature)
        assert country["Harvest_kg"].sum() == 27.
    else:
        with pytest.raises(AssertionError):
            C._assert_niger_country_feature_harvest(country, feature)
