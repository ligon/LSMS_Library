"""Invented native/public carriers; no real Country, Feature or cached frames."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import sys

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

import lsms_library as ll
import lsms_library.country as country
from lsms_library import diagnostics as d


FEATURE = "invented_stage_table"


def frame(values, dtype):
    df = pd.DataFrame({"x": pd.array(values, dtype=dtype)})
    df.index = pd.MultiIndex.from_tuples(
        [("wave-A", "hh-1"), ("wave-A", "hh-2")], names=["t", "i"])
    df.attrs = {"nested": {"preserve": [1, None]}}
    return df


def scheme(local):
    return {FEATURE: {"index": ["t", "i"], "columns": local,
                      "optional": set()}}


@pytest.fixture
def canonical(monkeypatch):
    calls = []

    def install(types):
        def load():
            calls.append("canonical")
            return {FEATURE: types}
        monkeypatch.setattr(country, "_load_canonical_dtypes", load)
        return calls

    return install


@pytest.mark.parametrize("stage,dtype,wanted", [
    ("native", "Float64", "pass"),
    ("native", "string", "warn"),
    ("finalized", "Float64", "warn"),
    ("finalized", "string", "pass"),
])
def test_numeric_native_string_public(stage, dtype, wanted, canonical):
    calls = canonical({"x": "str"})
    df = frame([1, None] if dtype == "Float64" else ["1", None], dtype)
    before = df.copy(deep=True)
    local = scheme({"x": "float"})
    local_before = deepcopy(local)
    assert d._check_dtype_consistency(df, local, FEATURE, stage=stage).status == wanted
    assert calls == (["canonical"] if stage == "finalized" else [])
    assert_frame_equal(df, before)
    assert df.attrs == before.attrs
    assert local == local_before


@pytest.mark.parametrize("declared,dtype,values,wanted", [
    ("float", "Float64", [1, None], "pass"),
    ("int", "Float64", [1.5, None], "pass"),  # existing rough numeric check
    ("float", "string", ["1", None], "warn"),
    ("bool", "boolean", [True, None], "pass"),
    ("bool", "Int64", [1, None], "warn"),
    ("str", "object", ["opaque", None], "pass"),
    ("string", "string", ["opaque", None], "pass"),
])
def test_recognized_canonical_types_override_local(
        declared, dtype, values, wanted, canonical):
    canonical({"x": declared})
    df = frame(values, dtype)
    assert d._check_dtype_consistency(
        df, scheme({"x": "str" if declared in ("float", "int", "bool") else "float"}),
        FEATURE, stage="finalized").status == wanted


@pytest.mark.parametrize("unknown", ["datetime", "future_type", "FLOAT", ""])
@pytest.mark.parametrize("dtype,values,wanted", [
    ("Float64", [1, None], "pass"),
    ("string", ["1", None], "warn"),
])
def test_unknown_global_cannot_exempt_known_local(
        unknown, dtype, values, wanted, canonical):
    canonical({"x": unknown})
    assert d._check_dtype_consistency(
        frame(values, dtype), scheme({"x": "float"}), FEATURE,
        stage="finalized").status == wanted


@pytest.mark.parametrize("local", [{}, {FEATURE: {"columns": {}, "optional": set()}}])
@pytest.mark.parametrize("dtype,values,wanted", [
    ("string", ["a", None], "pass"),
    ("Float64", [1, None], "warn"),
])
def test_canonical_only_present_column_even_without_local_feature(
        local, dtype, values, wanted, canonical):
    canonical({"x": "str", "absent_global": "bool"})
    assert d._check_dtype_consistency(
        frame(values, dtype), local, FEATURE, stage="finalized").status == wanted


@pytest.mark.parametrize("stage", ["native", "finalized"])
def test_constraints_absent_unknown_and_no_declarations(stage, canonical):
    canonical({"absent_global": "str", "x": "datetime"})
    df = frame(["a", None], "string")
    local = scheme({"x": ["a", "b"], "absent_local": "float"})
    assert d._check_dtype_consistency(df, local, FEATURE, stage=stage).status == "pass"
    assert d._check_dtype_consistency(df, scheme({"x": "unknown"}), FEATURE,
                                      stage=stage).status == "pass"
    assert d._check_dtype_consistency(df.iloc[:0], local, FEATURE,
                                      stage=stage).status == "pass"
    assert d._check_dtype_consistency(df, {}, FEATURE, stage=stage).status == "pass"


def test_default_never_loads_canonical(canonical):
    canonical({"x": "str"})
    df = frame([1, None], "Float64")
    local = scheme({"x": "float"})
    implicit = d._check_dtype_consistency(df, local, FEATURE)
    explicit = d._check_dtype_consistency(df, local, FEATURE, stage="native")
    assert implicit == explicit and implicit.status == "pass"


@pytest.mark.parametrize("bad", ["api", "cache", "", None, 0])
def test_invalid_stage_before_schema_or_provider(monkeypatch, bad):
    def forbidden(*a, **kw):
        pytest.fail("invalid stage reached schema/provider work")
    monkeypatch.setattr(d, "_load_scheme", forbidden)
    monkeypatch.setattr(country, "_load_canonical_dtypes", forbidden)
    with pytest.raises(ValueError, match="stage must be 'native' or 'finalized'"):
        d.is_this_feature_sane(frame([1, None], "Float64"), "Alpha", FEATURE, stage=bad)
    with pytest.raises(ValueError, match="stage must be 'native' or 'finalized'"):
        d._check_dtype_consistency(frame([1, None], "Float64"), {}, FEATURE, stage=bad)


def test_first_five_warning_cap_order_and_no_input_cast(canonical):
    canonical({})
    df = pd.DataFrame({f"c{k}": pd.array(["1", None], dtype="string") for k in range(7)})
    old = df.copy(deep=True)
    check = d._check_dtype_consistency(
        df, scheme({f"c{k}": "float" for k in range(7)}), FEATURE, stage="finalized")
    assert check.status == "warn" and check.message.count("expected numeric") == 5
    assert "c4:" in check.message and "c5:" not in check.message
    assert_frame_equal(df, old)


def test_public_stage_changes_only_dtype_check(monkeypatch, canonical):
    canonical({"x": "str", "not_present": "float"})
    local = scheme({"x": "float"})
    before = deepcopy(local)
    monkeypatch.setattr(d, "_load_scheme", lambda name: local if name == "Alpha" else pytest.fail(name))
    spine_calls = []
    monkeypatch.setattr(d, "_check_index_overlap_with_spine",
                        lambda df, name: spine_calls.append(name) or d.Check("invented_spine", "pass"))
    df = frame(["a", None], "string")
    saved = df.copy(deep=True)
    reports = [d.is_this_feature_sane(df, "Alpha", FEATURE, **kw)
               for kw in [{}, {"stage": "native"}, {"stage": "finalized"}]]
    pick = lambda report: [(c.name, c.status, c.message) for c in report.checks]
    assert pick(reports[0]) == pick(reports[1])
    without_dtype = lambda report: [c for c in pick(report) if c[0] != "dtype_consistency"]
    assert without_dtype(reports[0]) == without_dtype(reports[2])
    dtype = lambda report: next(c for c in report.checks if c.name == "dtype_consistency")
    assert dtype(reports[0]).status == "warn" and dtype(reports[2]).status == "pass"
    assert not any("not_present" in c.message for c in reports[2].checks)
    assert spine_calls == ["Alpha"] * 3
    assert local == before
    assert_frame_equal(df, saved)
    assert df.attrs == saved.attrs


def test_validate_public_adapter_uses_finalized_without_real_country(monkeypatch, canonical):
    canonical({"x": "str"})
    df = frame(["a", "b"], "string")
    calls = []
    class InventedCountry:
        def __init__(self, name):
            assert name == "Alpha"
            calls.append(("constructor", name))
        def invented_stage_table(self):
            calls.append(("public", FEATURE))
            return df
    monkeypatch.setattr(ll, "Country", InventedCountry)
    monkeypatch.setattr(ll, "Feature", lambda *a, **kw: pytest.fail("no real Feature path"))
    monkeypatch.setattr(d, "_load_scheme", lambda name: scheme({"x": "float"}))
    monkeypatch.setattr(d, "_check_no_regression", lambda *a: d.Check("invented_regression", "pass"))
    result = d.validate_feature("Alpha", FEATURE, reference_country="Alpha")
    assert calls == [("constructor", "Alpha"), ("public", FEATURE)]
    assert next(c for c in result.checks if c.name == "dtype_consistency").status == "pass"


def test_actual_scan_adapter_explicit_finalized(monkeypatch, canonical):
    canonical({"x": "str"})
    df = frame(["a", "b"], "string")
    path = Path(__file__).resolve().parents[1] / "bench/feature_audit/scan.py"
    spec = importlib.util.spec_from_file_location("_gh1606_scan", path)
    scan = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, scan)
    spec.loader.exec_module(scan)
    calls = []
    class InventedCountry:
        def __init__(self, name):
            assert name == "Alpha"
        def invented_stage_table(self):
            calls.append("public")
            return df
    monkeypatch.setattr(ll, "Country", InventedCountry)
    monkeypatch.setattr(d, "_load_scheme", lambda name: scheme({"x": "float"}))
    monkeypatch.setattr(d, "_check_index_overlap_with_spine", lambda *a: d.Check("invented_spine", "pass"))
    monkeypatch.setattr(scan, "_null_read_findings", lambda *a: [])
    monkeypatch.setattr(scan, "_scale_findings", lambda *a: [])
    original = scan.is_this_feature_sane
    def check_stage(*args, **kwargs):
        assert kwargs == {"stage": "finalized"}
        calls.append("finalized")
        report = original(*args, **kwargs)
        assert next(c for c in report.checks if c.name == "dtype_consistency").status == "pass"
        return report
    monkeypatch.setattr(scan, "is_this_feature_sane", check_stage)
    findings, name, rows = scan._scan_one_country(FEATURE, "Alpha")
    assert (name, rows) == ("Alpha", 2)
    assert calls == ["public", "finalized"]
    assert not any(c.check == "dtype_consistency" for c in findings)
    assert not any(c.status in ("error", "fail") for c in findings)
