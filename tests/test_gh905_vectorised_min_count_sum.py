"""GH #905: the dedupe tail of ``_finalize_canonical_food_acquired`` reduces
with cython ``groupby.sum(min_count=1)`` / ``mean`` rather than per-group
Python lambdas (~30x faster on 400k rows; the lambda form also deep-copies
``df.attrs`` once per group, the GH #871 cost).

The contract this pins: against the old lambda reducer the result has the
same index, column order, dtypes and NA pattern (an all-NaN group stays NaN,
a NaN KEY is kept because ``dropna=False``), and every value agrees to within
a couple of ULP -- cython sums pairwise where ``Series.sum`` accumulates in
order, exactly as GH #871 measured for the core collapse.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from lsms_library.build_transforms import _finalize_canonical_food_acquired

LEVELS = ["t", "i", "j", "u", "s"]


def _old_reducer(out: pd.DataFrame) -> pd.DataFrame:
    """The pre-#905 agg dict, verbatim."""
    qty_ok = out["Quantity"].notna() & (out["Quantity"] > 0)
    exp_ok = out["Expenditure"].notna() & (out["Expenditure"] > 0)
    out = out[qty_ok | exp_ok]
    aggs = {"Quantity": ("Quantity", lambda s: s.sum(min_count=1)),
            "Expenditure": ("Expenditure", lambda s: s.sum(min_count=1))}
    if "Price" in out.columns:
        aggs["Price"] = ("Price", "mean")
    if "Quantity_kg" in out.columns:
        aggs["Quantity_kg"] = ("Quantity_kg", lambda s: s.sum(min_count=1))
    return out.groupby(LEVELS, dropna=False).agg(**aggs).sort_index()


def _frame(seed: int, n: int, *, price: bool, kg: bool) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    q = rng.random(n) * 100
    q[rng.random(n) < 0.3] = np.nan
    e = rng.random(n) * 1e4
    e[rng.random(n) < 0.3] = np.nan
    df = pd.DataFrame({
        "t": rng.choice(["2014-15", "2018-19"], n),
        "i": rng.integers(0, 40, n).astype(str),
        "j": rng.choice(["Rice", "Millet", "Oil"], n),
        "u": rng.choice(["Kg", "Piece", None], n),   # NaN key -> dropna=False
        "s": rng.choice(["purchased", "produced"], n),
        "Quantity": q, "Expenditure": e})
    if price:
        df["Price"] = df["Expenditure"] / df["Quantity"]
    if kg:
        df["Quantity_kg"] = df["Quantity"] * 0.7
    return df


def _assert_equivalent(old: pd.DataFrame, new: pd.DataFrame) -> None:
    assert list(old.columns) == list(new.columns)
    assert old.index.equals(new.index)
    assert (old.dtypes == new.dtypes).all()
    for c in old.columns:
        a, b = old[c].to_numpy(dtype=float), new[c].to_numpy(dtype=float)
        assert np.array_equal(np.isnan(a), np.isnan(b)), f"NA pattern differs in {c}"
        m = ~np.isnan(a)
        ulp = np.abs(a[m].view(np.int64) - b[m].view(np.int64))
        assert ulp.max(initial=0) <= 4, f"{c}: {ulp.max()} ULP apart"


@pytest.mark.parametrize("price,kg", [(False, False), (True, False),
                                      (False, True), (True, True)])
def test_vectorised_tail_matches_the_lambda_reducer(price, kg):
    df = _frame(905, 4000, price=price, kg=kg)
    _assert_equivalent(_old_reducer(df), _finalize_canonical_food_acquired(df.copy()))


def test_all_nan_group_stays_nan_and_nan_key_is_kept():
    df = pd.DataFrame({
        "t": ["w"] * 4, "i": ["1"] * 4, "j": ["Rice"] * 4,
        "u": ["Kg", "Kg", None, None], "s": ["purchased"] * 4,
        "Quantity": [np.nan, np.nan, 2.0, 3.0],
        "Expenditure": [10.0, 5.0, np.nan, np.nan],
        "Quantity_kg": [np.nan, np.nan, 1.4, 2.1]})
    out = _finalize_canonical_food_acquired(df)
    assert len(out) == 2
    kg_row = out.xs("Kg", level="u").iloc[0]
    assert np.isnan(kg_row["Quantity"]) and kg_row["Expenditure"] == 15.0
    assert np.isnan(kg_row["Quantity_kg"])
    nan_key = out[out.index.get_level_values("u").isna()]
    assert len(nan_key) == 1, "dropna=False: a NaN key must survive"
    assert nan_key["Quantity"].iloc[0] == 5.0 and np.isnan(nan_key["Expenditure"].iloc[0])
    assert list(out.columns) == ["Quantity", "Expenditure", "Quantity_kg"]


def test_price_is_averaged_and_column_order_is_the_old_one():
    df = pd.DataFrame({
        "t": ["w"] * 2, "i": ["1"] * 2, "j": ["Rice"] * 2, "u": ["Kg"] * 2,
        "s": ["purchased"] * 2, "Quantity": [1.0, 3.0], "Expenditure": [2.0, 9.0],
        "Price": [2.0, 3.0], "Quantity_kg": [1.0, 3.0]})
    out = _finalize_canonical_food_acquired(df)
    assert list(out.columns) == ["Quantity", "Expenditure", "Price", "Quantity_kg"]
    assert out["Price"].iloc[0] == 2.5 and out["Quantity"].iloc[0] == 4.0


def test_dedupe_false_is_untouched():
    df = _frame(1, 50, price=True, kg=False)
    out = _finalize_canonical_food_acquired(df.copy(), dedupe=False)
    assert out.index.names == LEVELS and "Price" in out.columns
