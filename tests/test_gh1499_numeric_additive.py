"""Numeric representation at the two actual additive collapse sites; no data IO."""
import copy
from decimal import Decimal
import traceback

import pandas as pd
import pytest

from lsms_library.country import (
    _normalize_dataframe_index, GrainCollapseWarning, GrainCollapseError,
)
from lsms_library.feature import _collapse_duplicate_index, _harmonize_country_frame


ROUTES = ["country", "feature"]


def frame(quantity, expenditure=None, *, dtype=None, price=False):
    index = pd.MultiIndex.from_tuples(
        [("wave", "001", "002", "03")] * len(quantity),
        names=["t", "i", "j", "u"],
    )
    data = {"Quantity": pd.Series(quantity, index=index, dtype=dtype)}
    if expenditure is not None:
        data["Expenditure"] = pd.Series(expenditure, index=index, dtype=dtype)
    df = pd.DataFrame(data, index=index)
    if price:
        df["Price"] = [pd.NA] * len(df)
    df.attrs = {"id_converted": True,
                "nested": {"literal": (Decimal("1.25"), pd.Timestamp("2001-02-03"))}}
    return df


def collapse(df, route, table="food_acquired"):
    if route == "country":
        schema = {"index": "(" + ", ".join(df.index.names) + ")"}
        return _normalize_dataframe_index(df, schema, "wave", table, "Alpha")
    return _collapse_duplicate_index(df, table, "Alpha")


def unchanged(actual, before):
    pd.testing.assert_frame_equal(actual, before, check_exact=True)
    assert actual.attrs == before.attrs
    assert type(actual.attrs["nested"]["literal"][0]) is Decimal
    assert type(actual.attrs["nested"]["literal"][1]) is pd.Timestamp


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("price", [False, True])
@pytest.mark.parametrize("dtype", ["object", "string", "category"])
def test_numeric_text_is_added_before_price(route, price, dtype):
    df = frame(["2", "3"], ["10", "12"], dtype=dtype, price=price)
    before = copy.deepcopy(df)
    out = collapse(df, route)
    assert len(out) == 1
    assert out["Quantity"].iloc[0] == 5
    assert out["Expenditure"].iloc[0] == 22
    assert pd.api.types.is_numeric_dtype(out["Quantity"].dtype)
    assert pd.api.types.is_numeric_dtype(out["Expenditure"].dtype)
    if price:
        assert out["Price"].iloc[0] == 22 / 5
    assert out.attrs == before.attrs
    unchanged(df, before)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("text,numeric", [
    (["-2.5", "+3e0"], [-2.5, 3.0]),
    ([" 2 ", 3], [2, 3]),
    ([".25", "0.75"], [0.25, 0.75]),
])
def test_valid_text_matches_typed_numeric_counterpart(route, text, numeric):
    df = frame(text, ["10", "12"], dtype="object", price=True)
    expected = collapse(frame(numeric, [10, 12], price=True), route)
    out = collapse(df, route)
    pd.testing.assert_frame_equal(out, expected, check_exact=True)
    assert out.attrs == expected.attrs


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("dtype,numeric_dtype", [("object", "float64"),
                                                ("string", "Int64")])
def test_nullable_text_preserves_real_missingness(route, dtype, numeric_dtype):
    text = frame(["2", None, "3"], ["10", pd.NA, "12"], dtype=dtype, price=True)
    numeric = frame([2, None, 3], [10, None, 12], dtype=numeric_dtype, price=True)
    before = copy.deepcopy(text)
    out = collapse(text, route)
    expected = collapse(numeric, route)
    pd.testing.assert_frame_equal(out, expected, check_exact=True)
    unchanged(text, before)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("dtype", ["object", "string", "Float64", "Int64"])
def test_all_real_missing_stays_missing_never_zero(route, dtype):
    df = frame([None, None], [None, None], dtype=dtype, price=True)
    before = copy.deepcopy(df)
    out = collapse(df, route)
    assert len(out) == 1
    assert out[["Quantity", "Expenditure", "Price"]].isna().all().all()
    unchanged(df, before)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("bad", ["", " ", "\t", "NaN", "not-a-number", "1,000"])
@pytest.mark.parametrize("column", ["Quantity", "Expenditure"])
def test_malformed_nonmissing_never_becomes_missing_or_partial_sum(route, bad, column):
    df = frame(["2", "3"], ["10", "12"], price=True)
    df[column] = [bad, "3"]
    before = copy.deepcopy(df)
    with pytest.raises(ValueError, match=column) as error:
        collapse(df, route)
    assert "nonmissing additive values must be numeric" in str(error.value)
    if bad.strip():
        assert bad not in str(error.value)
        rendered = "".join(traceback.TracebackException.from_exception(error.value).format())
        assert bad not in rendered
    unchanged(df, before)


@pytest.mark.parametrize("route", ROUTES)
def test_all_malformed_refuses_without_fabricating_a_zero_or_na(route):
    df = frame(["first-invalid", "second-invalid"])
    before = copy.deepcopy(df)
    with pytest.raises(ValueError, match="Quantity"):
        collapse(df, route)
    unchanged(df, before)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("bad", ["nan", "None", ""])
@pytest.mark.parametrize("column", ["Quantity", "Expenditure"])
def test_categorical_tokens_are_not_mistaken_for_actual_missing(route, bad, column):
    df = frame(["2", "3"], ["10", "12"])
    df[column] = pd.Categorical([bad, "3"])
    before = copy.deepcopy(df)
    with pytest.raises(ValueError, match=column):
        collapse(df, route)
    unchanged(df, before)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("dtype", ["int64", "float64", "Int64", "Float64", "object"])
def test_already_numeric_values_and_dtypes_are_preserved(route, dtype):
    df = frame([2, 3], [10, 12], dtype=dtype, price=True)
    before = copy.deepcopy(df)
    out = collapse(df, route)
    assert out["Quantity"].iloc[0] == 5
    assert out["Expenditure"].iloc[0] == 22
    assert out["Price"].iloc[0] == 22 / 5
    assert out["Quantity"].dtype == df["Quantity"].dtype
    assert out["Expenditure"].dtype == df["Expenditure"].dtype
    unchanged(df, before)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("values,expected", [
    ([2 ** 80, 3], 2 ** 80 + 3),
    ([Decimal("1.10"), Decimal("2.20")], Decimal("3.30")),
])
def test_object_numeric_carriers_are_not_float_cast(route, values, expected):
    df = frame(values, dtype="object")
    before = copy.deepcopy(df)
    out = collapse(df, route)
    assert out["Quantity"].dtype == object
    assert type(out["Quantity"].iloc[0]) is type(expected)
    assert out["Quantity"].iloc[0] == expected
    unchanged(df, before)


@pytest.mark.parametrize("route", ROUTES)
def test_nontext_arithmetic_errors_are_not_reclassified_as_parser_errors(route):
    class NumericFailure:
        def __add__(self, other):
            raise ArithmeticError("original numeric arithmetic failure")

    df = frame([NumericFailure(), NumericFailure()], dtype="object")
    with pytest.raises(ArithmeticError, match="original numeric arithmetic failure"):
        collapse(df, route)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("quantity,expected,price", [(["-2", "1"], -1, -22),
                                                    (["-2", "2"], 0, None)])
def test_signed_and_zero_numeric_arithmetic_unchanged(route, quantity, expected, price):
    out = collapse(frame(quantity, ["10", "12"], price=True), route)
    assert out["Quantity"].iloc[0] == expected
    if price is None:
        assert pd.isna(out["Price"].iloc[0])
    else:
        assert out["Price"].iloc[0] == price


@pytest.mark.parametrize("route", ROUTES)
def test_assets_value_only_is_numeric_age_remains_observed(route):
    df = frame(["4", "4"])
    df["Value"] = ["100", "200"]
    df["Age"] = [7.0, 9.0]
    before = copy.deepcopy(df)
    with pytest.warns(GrainCollapseWarning):
        out = collapse(df, route, "assets")
    assert out["Value"].iloc[0] == 300
    assert out["Quantity"].iloc[0] == "4"
    assert out["Age"].iloc[0] == 7
    unchanged(df, before)


@pytest.mark.parametrize("route", ROUTES)
def test_nonadditive_payload_and_price_do_not_gain_numeric_authority(route):
    df = frame(["2", "3"])
    df["Price"] = ["first-price", "second-price"]
    df["Code"] = ["0007", "0007"]
    before = copy.deepcopy(df)
    with pytest.warns(GrainCollapseWarning):
        out = collapse(df, route)
    assert out["Quantity"].iloc[0] == 5
    assert out["Price"].iloc[0] == "first-price"
    assert out["Code"].iloc[0] == "0007"
    assert out.index[0] == ("wave", "001", "002", "03")
    unchanged(df, before)


@pytest.mark.parametrize("route", ROUTES)
def test_discarded_price_is_not_parsed_and_cannot_vote_on_selection(route):
    df = frame(["2", "3"], ["10", "12"], price=True)
    df["Price"] = [pd.NA, "not-a-price"]
    df["Source"] = ["observed", pd.NA]
    df["Note"] = [pd.NA, "other"]
    with pytest.warns(GrainCollapseWarning):
        out = collapse(df, route)
    assert out["Price"].iloc[0] == 22 / 5
    assert out["Source"].iloc[0] == "observed"
    assert pd.isna(out["Note"].iloc[0])


@pytest.mark.parametrize("route", ROUTES)
def test_provenance_is_unioned_but_cannot_select_a_row(route):
    df = frame(["2", "3"], ["10", "12"])
    df["Derivation"] = [pd.NA, "Alpha::food::b+Alpha::food::a"]
    df["Source"] = ["first", "second"]
    with pytest.warns(GrainCollapseWarning):
        out = collapse(df, route)
    assert out["Source"].iloc[0] == "first"
    assert out["Derivation"].iloc[0] == "Alpha::food::a+Alpha::food::b"


@pytest.mark.parametrize("route", ROUTES)
def test_unique_and_nonadditive_paths_keep_their_original_strings(route):
    df = frame(["unparsed-first", "unparsed-second"])
    df.index = pd.MultiIndex.from_tuples(
        [("wave", "001", "002", "03"), ("wave", "002", "002", "03")],
        names=df.index.names)
    pd.testing.assert_frame_equal(collapse(df, route), df, check_exact=True)
    duplicate = frame(["unparsed-first", "unparsed-second"])
    with pytest.warns(GrainCollapseWarning):
        out = collapse(duplicate, route, "shocks")
    assert out["Quantity"].iloc[0] == "unparsed-first"


@pytest.mark.parametrize("route", ROUTES)
def test_full_key_partition_is_not_numericized_or_pooled(route):
    df = frame(["1", "2", "3", "4"], ["10", "20", "30", "40"])
    df.index = pd.MultiIndex.from_tuples(
        [("wave", "001", "002", "03")] * 2
        + [("wave", "001", "002", "003")] * 2, names=df.index.names)
    out = collapse(df, route)
    assert len(out) == 2
    assert out.loc[("wave", "001", "002", "03"), "Quantity"] == 3
    assert out.loc[("wave", "001", "002", "003"), "Quantity"] == 7


@pytest.mark.parametrize("route", ROUTES)
def test_real_projection_caller_collapses_text_after_surplus_level_removal(route):
    df = frame(["2", "3"], ["10", "12"], dtype="object")
    df.index = pd.MultiIndex.from_tuples(
        [("wave", "001", "002", "03", 1), ("wave", "001", "002", "03", 2)],
        names=["t", "i", "j", "u", "visit"])
    before = copy.deepcopy(df)
    levels = ["t", "i", "j", "u"]
    if route == "country":
        out = _normalize_dataframe_index(
            df, {"index": "(t, i, j, u)"}, "wave", "food_acquired", "Alpha")
    else:
        with pytest.warns(UserWarning, match="extra index"):
            out = _harmonize_country_frame(df, levels, "Alpha", "food_acquired")
    assert out.index.names == levels and len(out) == 1
    assert out["Quantity"].iloc[0] == 5 and out["Expenditure"].iloc[0] == 22
    unchanged(df, before)


@pytest.mark.parametrize("route", ROUTES)
def test_existing_null_key_deletion_remains_loud(route):
    df = frame(["1", "2", "3"])
    df.index = pd.MultiIndex.from_tuples(
        [("wave", "001", "002", "03")] * 2
        + [("wave", None, "002", "03")], names=df.index.names)
    with pytest.warns(GrainCollapseWarning):
        out = collapse(df, route)
    assert len(out) == 1 and out["Quantity"].iloc[0] == 3


@pytest.mark.parametrize("route", ROUTES)
def test_nonadditive_conflict_still_respects_strict_before_arithmetic(route, monkeypatch):
    monkeypatch.setenv("LSMS_GRAIN_STRICT", "1")
    df = frame(["2", "3"], ["10", "12"])
    df["Source"] = ["first", "second"]
    with pytest.raises(GrainCollapseError):
        collapse(df, route)


@pytest.mark.parametrize("route", ROUTES)
def test_empty_typed_frame_remains_unchanged(route):
    df = frame([], [], dtype="string")
    out = collapse(df, route)
    pd.testing.assert_frame_equal(out, df, check_exact=True)
    assert out.attrs == df.attrs


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("column", ["Quantity", "Expenditure"])
@pytest.mark.parametrize("native", [Decimal("0.1234567890123456789"), 2**70],
                         ids=["decimal", "large-integer"])
def test_mixed_native_object_and_text_preserves_numeric_counterpart(route, column, native):
    df = frame([1, 2], [1, 2], dtype="object", price=True)
    df[column] = pd.Series([native, "2"], index=df.index, dtype="object")
    expected_input = copy.deepcopy(df)
    expected_input[column] = pd.Series([native, 2], index=df.index, dtype="object")
    before = copy.deepcopy(df)
    expected = collapse(expected_input, route)
    out = collapse(df, route)
    pd.testing.assert_frame_equal(out, expected, check_exact=True)
    assert type(out[column].iloc[0]) is type(native)
    assert out[column].iloc[0] == native + 2
    unchanged(df, before)


@pytest.mark.parametrize("route", ROUTES)
def test_mixed_native_decimal_text_and_real_na_preserves_counterpart(route):
    native = Decimal("0.1234567890123456789")
    df = frame([native, pd.NA, "2"], [1, pd.NA, 2], dtype="object", price=True)
    before = copy.deepcopy(df)
    expected = collapse(frame([native, pd.NA, 2], [1, pd.NA, 2],
                              dtype="object", price=True), route)
    out = collapse(df, route)
    pd.testing.assert_frame_equal(out, expected, check_exact=True)
    assert type(out["Quantity"].iloc[0]) is Decimal
    unchanged(df, before)


@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("bad", ["", "not-a-number"])
def test_mixed_native_decimal_does_not_hide_invalid_text(route, bad):
    df = frame([Decimal("0.1234567890123456789"), bad], [1, 2], dtype="object")
    before = copy.deepcopy(df)
    with pytest.raises(ValueError, match="nonmissing additive values must be numeric"):
        collapse(df, route)
    unchanged(df, before)
