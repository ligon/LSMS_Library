"""Runtime-derived food prices and quantities must have floating payloads.

Mixed values in kg-factor inputs once produced object arithmetic that was
serialized as strings. Check the canonical API payloads, not derived parquets:
normal food_prices/food_quantities calls do not persist those tables.
"""
import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.paths import data_root


def _uganda_food_cache_exists() -> bool:
    root = data_root("Uganda") / "var"
    return all((root / f"{table}.parquet").is_file()
               for table in ("food_acquired", "sample"))


def _assert_canonical_float(frame: pd.DataFrame, column: str) -> None:
    # Reuse the existing predicate on required canonical fields (ledger sections 2-3).
    assert not frame.empty, "Derived food table returned an empty DataFrame"
    assert column in frame.columns, f"Missing canonical {column} column"
    assert pd.api.types.is_float_dtype(frame[column]), (
        f"{column} is {frame[column].dtype}; expected a floating dtype"
    )


@pytest.mark.requires_s3
@pytest.mark.skipif(
    not _uganda_food_cache_exists(),
    reason="Requires cached Uganda food_acquired and sample source tables",
)
@pytest.mark.parametrize("table,column", [
    ("food_prices", "Price"),
    ("food_quantities", "Quantity"),
])
def test_uganda_food_api_dtype_float(table, column):
    country = ll.Country("Uganda", preload_panel_ids=False, verbose=False)
    _assert_canonical_float(getattr(country, table)(), column)


@pytest.mark.parametrize("column", ["Price", "Quantity"])
@pytest.mark.parametrize("fault", ["missing", "object", "string"])
def test_canonical_float_rejects_invalid_payload(column, fault):
    if fault == "missing":
        frame = pd.DataFrame({"unrelated": [1.0]})
    elif fault == "object":
        frame = pd.DataFrame({column: pd.Series([1.0], dtype=object)})
    else:
        frame = pd.DataFrame({column: pd.Series(["1.0"], dtype="string")})
    with pytest.raises(AssertionError):
        _assert_canonical_float(frame, column)


@pytest.mark.parametrize("column", ["Price", "Quantity"])
@pytest.mark.parametrize("dtype", ["float64", "Float64"])
def test_canonical_float_accepts_float_payload(column, dtype):
    frame = pd.DataFrame({column: pd.Series([1.0], dtype=dtype)})
    _assert_canonical_float(frame, column)
