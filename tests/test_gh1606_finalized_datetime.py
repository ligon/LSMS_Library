"""Canonical datetime metadata with invented carriers; no country data or APIs."""
from copy import deepcopy
from datetime import date, timedelta, timezone

import numpy as np
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from lsms_library import diagnostics as d
import lsms_library.country as country


TABLE = "interview_date"
COLUMN = "Int_t"
NAT = np.iinfo(np.int64).min


def dates(unit="us", *, empty=False, all_nat=False):
    integers = [] if empty else [NAT, NAT] if all_nat else [0, NAT]
    return pd.Series(np.array(integers, dtype=np.int64).view(f"datetime64[{unit}]"))


def frame(series, column=COLUMN):
    df = pd.DataFrame({column: series})
    df.index = pd.MultiIndex.from_arrays(
        [["wave"] * len(df), [f"invented-{i}" for i in range(len(df))]],
        names=["t", "i"])
    df.attrs = {"opaque": {"keep": [1, None]}, "typed": date(2001, 2, 3)}
    return df


def scheme(declaration="str", column=COLUMN):
    return {TABLE: {"index": ["t", "i"], "columns": {column: declaration},
                    "optional": set()}}


def check(df, local=None, *, stage="finalized"):
    return d._check_dtype_consistency(df, scheme() if local is None else local,
                                      TABLE, stage=stage)


def test_actual_global_schema_types_all_have_rough_recognition():
    canonical = country._load_canonical_dtypes()
    declared = {kind for columns in canonical.values() for kind in columns.values()}
    assert "datetime" in declared
    assert declared <= set(d._DTYPE_MAP) | {"datetime"}
    assert "datetime" not in d._DTYPE_MAP  # native map is deliberately unchanged
    fields = [(table, col) for table, columns in canonical.items()
              for col, kind in columns.items() if kind == "datetime"]
    assert fields
    for table, column in fields:
        df = frame(dates(empty=True), column=column)
        local = {table: {"columns": {column: "str"}}}
        assert d._check_dtype_consistency(df, local, table, stage="finalized").status == "pass"


@pytest.mark.parametrize("column", ["Int_t", "Interview start", "Interview end"])
def test_actual_canonical_datetime_priority_and_wrong_string_negative(column):
    assert country._load_canonical_dtypes()[TABLE][column] == "datetime"
    typed = frame(dates(), column)
    text = frame(pd.Series(["2001-02-03", None], dtype="string"), column)
    typed_before, text_before = deepcopy(typed), deepcopy(text)
    local = scheme(column=column)
    local_before = deepcopy(local)
    assert d._check_dtype_consistency(typed, local, TABLE).status == "warn"
    assert d._check_dtype_consistency(typed, local, TABLE, stage="finalized").status == "pass"
    assert d._check_dtype_consistency(text, local, TABLE).status == "pass"
    wrong = d._check_dtype_consistency(text, local, TABLE, stage="finalized")
    assert wrong.status == "warn" and "expected datetime" in wrong.message
    assert_frame_equal(typed, typed_before)
    assert_frame_equal(text, text_before)
    assert local == local_before


@pytest.mark.parametrize("unit", ["s", "ms", "us", "ns"])
def test_datetime_units_and_nat_are_preserved(unit):
    df = frame(dates(unit))
    before = deepcopy(df)
    assert str(df[COLUMN].dtype) == f"datetime64[{unit}]"
    assert check(df).status == "pass"
    assert_frame_equal(df, before)
    assert df.attrs == before.attrs


@pytest.mark.parametrize("tz", [timezone.utc, timezone(timedelta(hours=5, minutes=45))])
def test_timezone_aware_carrier_is_recognized_without_conversion(tz):
    array = pd.array([pd.Timestamp(year=2001, month=2, day=3, tz=tz), pd.NaT],
                     dtype=pd.DatetimeTZDtype(unit="us", tz=tz))
    df = frame(pd.Series(array))
    before = deepcopy(df)
    assert check(df).status == "pass"
    assert_frame_equal(df, before)
    assert df[COLUMN].dtype == before[COLUMN].dtype


@pytest.mark.parametrize("empty", [False, True])
def test_all_nat_or_typed_empty_still_has_datetime_dtype(empty):
    df = frame(dates("ms", empty=empty, all_nat=True))
    before = deepcopy(df)
    assert check(df).status == "pass"
    assert_frame_equal(df, before)


@pytest.mark.parametrize("kind", ["string", "object_timestamp", "object_date", "Float64",
                                 "Int64", "boolean", "category"])
def test_wrong_carrier_is_not_parsed_or_silently_accepted(kind):
    values = {
        "string": lambda: pd.Series(["2001-02-03", None], dtype="string"),
        "object_timestamp": lambda: pd.Series([pd.Timestamp(year=2001, month=2, day=3), pd.NaT], dtype=object),
        "object_date": lambda: pd.Series([date(2001, 2, 3), None], dtype=object),
        "Float64": lambda: pd.Series([0.0, None], dtype="Float64"),
        "Int64": lambda: pd.Series([0, None], dtype="Int64"),
        "boolean": lambda: pd.Series([True, None], dtype="boolean"),
        "category": lambda: pd.Series(pd.Categorical([pd.Timestamp(year=2001, month=2, day=3), pd.NaT])),
    }
    df = frame(values[kind]())
    before = deepcopy(df)
    got = check(df)
    assert got.status == "warn" and "expected datetime" in got.message
    assert_frame_equal(df, before)


@pytest.mark.parametrize("local", [{}, {TABLE: {"columns": {}, "optional": set()}}])
def test_canonical_only_present_column_does_not_need_local_feature(local):
    old = deepcopy(local)
    assert check(frame(dates()), local).status == "pass"
    assert check(frame(pd.Series(["date-looking", None], dtype="string")), local).status == "warn"
    assert check(pd.DataFrame({"unrelated": [1, 2]}), local).status == "pass"
    assert local == old


@pytest.mark.parametrize("stage", ["native", "finalized"])
def test_local_only_datetime_remains_legacy_unsupported(monkeypatch, stage):
    monkeypatch.setattr(country, "_load_canonical_dtypes", lambda: {})
    # The follow-up recognizes GLOBAL declarations only, not a new local type rule.
    df = frame(pd.Series(["not-a-date", None], dtype="string"))
    assert check(df, scheme("datetime"), stage=stage).status == "pass"


@pytest.mark.parametrize("local_type", ["float", "str", ["opaque"]])
def test_global_datetime_overrides_each_conflicting_local_declaration(local_type):
    local = scheme(local_type)
    old = deepcopy(local)
    assert check(frame(dates()), local).status == "pass"
    assert check(frame(pd.Series(["opaque", None], dtype="string")), local).status == "warn"
    assert local == old


def test_checker_never_calls_parser_caster_or_mutates_carrier(monkeypatch):
    df = frame(dates())
    before = deepcopy(df)
    local = scheme()
    old = deepcopy(local)
    def forbidden(*args, **kwargs):
        pytest.fail("dtype grading must not parse or cast data")
    with monkeypatch.context() as patch:
        patch.setattr(pd, "to_datetime", forbidden)
        patch.setattr(pd.Series, "astype", forbidden)
        patch.setattr(pd.DataFrame, "astype", forbidden)
        patch.setattr(country, "_enforce_canonical_dtypes", forbidden)
        patch.setattr(country, "_coerce_datetime_column", forbidden)
        assert check(df, local).status == "pass"
    assert_frame_equal(df, before)
    assert df.attrs == before.attrs
    assert local == old


@pytest.mark.parametrize("empty", [False, True])
def test_other_public_checks_and_missingness_are_not_relaxed(monkeypatch, empty):
    local = scheme()
    old = deepcopy(local)
    df = frame(dates(empty=empty, all_nat=True))
    before = deepcopy(df)
    monkeypatch.setattr(d, "_load_scheme", lambda name: local if name == "Alpha" else pytest.fail(name))
    monkeypatch.setattr(d, "_check_index_overlap_with_spine", lambda *a: d.Check("invented_spine", "pass"))
    native = d.is_this_feature_sane(df, "Alpha", TABLE)
    public = d.is_this_feature_sane(df, "Alpha", TABLE, stage="finalized")
    other = lambda report: [(c.name, c.status, c.message) for c in report.checks if c.name != "dtype_consistency"]
    assert other(native) == other(public)
    assert next(c for c in public.checks if c.name == "dtype_consistency").status == "pass"
    failure = "not_empty" if empty else "no_all_null_columns"
    assert next(c for c in public.checks if c.name == failure).status == "fail"
    assert_frame_equal(df, before)
    assert local == old


@pytest.mark.parametrize("local_type,wanted", [("str", "warn"), ("float", "warn"), ("datetime", "pass")])
def test_native_forbids_canonical_loader_and_preserves_local_map(monkeypatch, local_type, wanted):
    def forbidden():
        pytest.fail("native grading loaded canonical schema")
    monkeypatch.setattr(country, "_load_canonical_dtypes", forbidden)
    before = dict(d._DTYPE_MAP)
    df = frame(dates())
    implicit = d._check_dtype_consistency(df, scheme(local_type), TABLE)
    explicit = check(df, scheme(local_type), stage="native")
    assert implicit == explicit and implicit.status == wanted
    assert d._DTYPE_MAP == before and "datetime" not in d._DTYPE_MAP


def test_datetime_recognition_does_not_broaden_other_global_or_future_types(monkeypatch):
    feature = "invented_mixed"
    canonical = {"when": "datetime", "amount": "float", "label": "str", "flag": "bool",
                 "future": "future_type"}
    local = {feature: {"columns": {"when": "str", "amount": "str", "label": "float",
                                    "flag": "float", "future": "float"}}}
    monkeypatch.setattr(country, "_load_canonical_dtypes", lambda: {feature: canonical})
    df = pd.DataFrame({"when": dates(), "amount": pd.array([1.0, None], dtype="Float64"),
                       "label": pd.array(["a", None], dtype="string"),
                       "flag": pd.array([True, None], dtype="boolean"),
                       "future": pd.array([2.0, None], dtype="Float64")})
    before, schema_before = deepcopy(df), deepcopy(local)
    assert d._check_dtype_consistency(df, local, feature, stage="finalized").status == "pass"
    wrong = df.copy()
    wrong["future"] = pd.array(["opaque", None], dtype="string")
    report = d._check_dtype_consistency(wrong, local, feature, stage="finalized")
    assert report.status == "warn" and report.message == "future: expected numeric, got string"
    assert_frame_equal(df, before)
    assert local == schema_before
