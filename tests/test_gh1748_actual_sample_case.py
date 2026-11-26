"""Check country routing for private CI sample diagnostics without survey IO."""
import importlib.util
from pathlib import Path
import re
from types import SimpleNamespace

import pandas as pd
import pytest


@pytest.fixture
def sample_tests():
    path = Path(__file__).with_name("test_sample.py")
    spec = importlib.util.spec_from_file_location("sample_actual_case_1748", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_static_cases_bind_exact_country_keys_and_leave_a_dynamic_remainder(sample_tests):
    m = sample_tests
    prefix = "test_covers_all_waves_"
    selected = {}
    for name, fn in vars(m.TestSample).items():
        if name.startswith(prefix):
            mark, = fn.pytestmark
            assert mark.name == "parametrize"
            column, values = mark.args
            assert column == "country_name"
            if values:
                country, = values
                assert name == prefix + re.sub(r"[^a-zA-Z0-9_]+", "_", country)
                assert country not in selected
                selected[country] = name
    assert set(selected) == set(m.SAMPLE_COUNTRIES) & m._NAMED_COVERAGE_COUNTRIES
    assert list(selected) == [c for c in m.SAMPLE_COUNTRIES if c in selected]
    for country in {"Guinea-Bissau", "Serbia and Montenegro", "South Africa", "Timor-Leste"}:
        if country in m.SAMPLE_COUNTRIES:
            assert country in selected
    mark, = m.TestSample.test_covers_all_waves.pytestmark
    assert mark.args == (
        "country_name", [c for c in m.SAMPLE_COUNTRIES if c not in selected]
    )


def test_named_case_owns_the_original_comparison_and_metadata_error(monkeypatch, sample_tests):
    m = sample_tests
    test = m.TestSample().test_covers_all_waves_Ethiopia
    sample = pd.DataFrame(index=pd.MultiIndex.from_tuples(
        [("invented", "2021-22")], names=["i", "t"]))
    monkeypatch.setattr(m.ll, "Country", lambda name: SimpleNamespace(
        waves=["2021-22", "2098-99"]))
    with pytest.raises(AssertionError, match="Ethiopia sample missing waves"):
        test("Ethiopia", sample)
    monkeypatch.setitem(m.PARTIALLY_WIRED_SAMPLE, "Ethiopia", {"2098-99"})
    test("Ethiopia", sample)

    def broken_metadata(name):
        raise ValueError("invented metadata error")

    monkeypatch.setattr(m.ll, "Country", broken_metadata)
    with pytest.raises(ValueError, match="invented metadata error"):
        test("Ethiopia", sample)
