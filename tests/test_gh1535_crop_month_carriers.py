"""Lossless crop month carriers through real finalization and Feature assembly.

All records are invented. The Country metadata/finalizer, canonical type
declarations, Feature assembly and Arrow round-trip are production machinery.
Ledger: 1535-crop-month-carriers.md §§2–5.
"""
from io import BytesIO
import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.country import Country, _enforce_canonical_dtypes


@pytest.mark.parametrize("column", ["harvest_month", "planting_month"])
@pytest.mark.parametrize("dtype,values,expected", [
    ("Float64", [6., 12., pd.NA], ["6.0", "12.0", pd.NA]),
    ("Int64", [6, 12, pd.NA], ["6", "12", pd.NA]),
    ("string", ["01 06 12", "2015-06", pd.NA], ["01 06 12", "2015-06", pd.NA]),
    ("string", ["unknown", "", pd.NA], ["unknown", "", pd.NA]),
])
def test_canonical_month_conversion_preserves_every_answer(column, dtype, values, expected):
    frame = pd.DataFrame({column: pd.array(values, dtype=dtype),
                          "Quantity": pd.array([0., 2.5, pd.NA], dtype="Float64")})
    quantity = frame.Quantity.copy()
    _enforce_canonical_dtypes(frame, "crop_production")
    pd.testing.assert_series_equal(frame[column],
        pd.Series(expected, name=column, dtype="string"))
    pd.testing.assert_series_equal(frame.Quantity, quantity)


def _native(name, column):
    numeric = name == "Tanzania"
    values = [6., 12., pd.NA] if numeric else ["01 06 12", "2015-06", pd.NA]
    idx = pd.MultiIndex.from_tuples([
        (f"h{j}", "invented-wave", "c", "plot", "Maize", "Kg", "unknown_condition", "unknown_season", "reported:0")
        for j in [2, 0, 1]], names=["i", "t", "v", "plot_id", "j", "u", "condition", "season", "production_obs"])
    frame = pd.DataFrame({column: pd.array(values, dtype="Float64" if numeric else "string"),
                          "Quantity": pd.array([0., 2.5, pd.NA], dtype="Float64"),
                          "Value_sold": pd.array([0., pd.NA, 4.25], dtype="Float64")}, index=idx)
    frame.attrs = {"id_converted": True}
    return frame


@pytest.mark.parametrize("column", ["harvest_month", "planting_month"])
@pytest.mark.parametrize("names", [("Tanzania", "Nigeria"), ("Nigeria", "Tanzania")])
def test_real_finalize_and_feature_roundtrip_preserve_rows_and_payload(monkeypatch, tmp_path, column, names):
    # Intercept only the invented provider; finalize and Feature run unchanged.
    monkeypatch.setenv("LSMS_DATA_DIR", str(tmp_path / "private-data"))
    natives = {name: _native(name, column) for name in names}
    originals = {name: frame.copy(deep=True) for name, frame in natives.items()}
    owners = {name: Country(name) for name in names}
    finalized = {}

    class Provider:
        data_scheme = {}
        categorical_mapping = {}

        def __init__(self, name, **kwargs):
            self.name = name

        def crop_production(self):
            declared = {column: "float" if self.name == "Tanzania" else "str"}
            out = owners[self.name]._finalize_result(
                natives[self.name].copy(deep=True), declared, "crop_production")
            finalized[self.name] = out.copy(deep=True)
            return out

    monkeypatch.setattr(ll, "Country", Provider)
    with warnings.catch_warnings(record=True) as seen:
        warnings.simplefilter("always")
        pooled = ll.Feature("crop_production")(list(names), currency=None)
    assert not [str(w.message) for w in seen if "Failed to load" in str(w.message)]
    assert set(finalized) == set(names)
    assert len(pooled) == 6
    expected = pd.concat(finalized, names=["country"]).reorder_levels(
        ["country", "t", "v", "i", "plot_id", "j", "u", "condition", "season", "production_obs"])
    pd.testing.assert_frame_equal(pooled, expected)
    buffer = BytesIO()
    pooled.to_parquet(buffer)
    buffer.seek(0)
    restored = pd.read_parquet(buffer)
    pd.testing.assert_frame_equal(restored, pooled)
    assert restored.attrs == pooled.attrs
    for name in names:
        pd.testing.assert_frame_equal(natives[name], originals[name])
        before, after = natives[name], finalized[name]
        pd.testing.assert_index_equal(after.index, before.index)
        pd.testing.assert_frame_equal(after.drop(columns=column), before.drop(columns=column))
        pd.testing.assert_series_equal(after[column], before[column].astype("string"))
    assert isinstance(pooled[column].dtype, pd.StringDtype)


def test_absent_optional_month_columns_are_not_fabricated():
    frame = pd.DataFrame({"Quantity": pd.array([1., pd.NA], dtype="Float64")})
    expected = frame.copy(deep=True)
    _enforce_canonical_dtypes(frame, "crop_production")
    pd.testing.assert_frame_equal(frame, expected)


def test_other_table_month_columns_keep_their_native_types():
    frame = pd.DataFrame({"harvest_month": pd.array([1., pd.NA], dtype="Float64"),
                          "planting_month": pd.array([12, pd.NA], dtype="Int64")})
    expected = frame.copy(deep=True)
    _enforce_canonical_dtypes(frame, "housing")
    pd.testing.assert_frame_equal(frame, expected)
