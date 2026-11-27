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


@pytest.mark.parametrize("observed, route", [
    ("missing", "miss"),
    ("partial", "hit-partial"),
    ("complete", "hit-complete"),
    ("unknown", "unknown"),
    ("ambiguous", "unknown"),
])
def test_albania_route_is_bound_to_first_exact_sample(
        monkeypatch, tmp_path, sample_tests, observed, route):
    m = sample_tests
    waves = ["2002", "2003"]
    managed = tmp_path / "var" / "sample.parquet"
    monkeypatch.setattr(m, "data_root", lambda country: tmp_path)

    def frame(coverage):
        idx = pd.MultiIndex.from_tuples(
            [("invented", wave) for wave in coverage], names=["i", "t"])
        return pd.DataFrame({"v": "invented"}, index=idx)

    partial = frame(waves[:1])
    complete = frame(waves)
    calls = []

    class FakeCountry:
        def __init__(self, country):
            assert country == "Albania"
            self.waves = waves

        def _read_l2_country_cache(self, table, path, selection, **kwargs):
            if observed == "missing":
                return None, None, "invented miss"
            if observed == "unknown":
                return object()  # classification error must not change sample()
            return (complete if observed == "complete" else partial), waves, None

        def sample(self):
            calls.append("sample")
            self._read_l2_country_cache("sample", tmp_path / "foreign", waves)
            self._read_l2_country_cache("sample", managed, waves, _borrowed_input=True)
            self._read_l2_country_cache("sample", managed, waves)
            if observed == "ambiguous":
                self._read_l2_country_cache("sample", managed, waves)
            return partial

    monkeypatch.setattr(m.ll, "Country", FakeCountry)
    assert m._get_sample("Albania") is partial
    assert m._get_sample("Albania") is partial
    assert calls == ["sample"]
    assert m._ALBANIA_SNAPSHOT_ROUTE == route
    for alternative in {"miss", "hit-partial", "hit-complete", "unknown"}:
        if alternative == route:
            with pytest.raises(AssertionError, match="Incomplete first Albania sample fixture"):
                m._assert_albania_snapshot_branch_complete(alternative)
        else:
            m._assert_albania_snapshot_branch_complete(alternative)
    with pytest.raises(AssertionError, match="Albania sample missing waves"):
        m.TestSample().test_covers_all_waves_Albania("Albania", partial)


def test_locator_obeys_no_data_rule_before_sample(monkeypatch, sample_tests):
    m = sample_tests
    monkeypatch.setattr(m, "NO_DATA_COUNTRIES", {"Albania"})

    def should_not_load(*args, **kwargs):
        pytest.fail("sample must remain uncalled for unavailable Albania")

    monkeypatch.setattr(m, "_get_sample", should_not_load)
    m.test_albania_sample_incomplete_on_l2_miss()
    m.test_albania_sample_incomplete_on_l2_hit_partial()
    m.test_albania_sample_incomplete_after_l2_hit_complete()
    m.test_albania_sample_incomplete_on_unidentified_route()
