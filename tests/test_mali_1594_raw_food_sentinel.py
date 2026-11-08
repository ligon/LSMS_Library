"""GH1594: exact 2014 purchased-value code, before passage aggregation.

Pure invented frames use the real wave mapper and existing additive kernel.
The user ruling concerns s13q03c only, not quantity, price or recall policy.
"""
from copy import deepcopy
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library.country import _normalize_dataframe_index


@pytest.fixture(scope="module")
def mapping():
    path = (Path(__file__).resolve().parents[1] / "lsms_library/countries/Mali"
            / "2014-15/_/mapping.py")
    spec = importlib.util.spec_from_file_location("_test_mali1594_mapping", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def raw(amounts, quantities=None, visits=None):
    n = len(amounts)
    frame = pd.DataFrame({
        "t": ["2014-15"] * n, "v": ["v-invented"] * n,
        "visit": visits if visits is not None else list(range(1, n + 1)),
        "i": ["EACI14_invented"] * n, "j": ["Rice"] * n,
        "QuantityPurchased": quantities if quantities is not None else [2.] * n,
        "UnitPurchased": ["Kg"] * n, "ExpenditurePurchased": amounts,
        "QuantityProduced": [0.] * n, "UnitProduced": ["Kg"] * n,
        "QuantityInkind": [0.] * n, "UnitInkind": ["Kg"] * n,
    }).set_index(["t", "v", "visit", "i", "j"])
    frame.attrs = {"invented": True, "native": {"passage": "retained input"}}
    return frame


def collapse(frame):
    return _normalize_dataframe_index(
        frame, {"index": "(t, v, i, j, u, s)"}, "2014-15",
        table_name="food_acquired", country="Mali")


@pytest.mark.parametrize("visit", [1, 2])
def test_exact_raw_amount_masked_in_either_passage(mapping, visit):
    out = mapping.food_acquired(raw([99_999_999.], [2.5], [visit]))
    assert len(out) == 1 and out.index.names == ["t", "v", "i", "j", "u", "s"]
    assert out.index.get_level_values("s").tolist() == ["purchased"]
    assert out.Quantity.iloc[0] == 2.5 and pd.isna(out.Expenditure.iloc[0])


def test_paired_legitimate_amount_survives_raw_mask_and_existing_sum(mapping):
    lines = mapping.food_acquired(raw([99_999_999., 16_325.], [1., 2.]))
    assert len(lines) == 2 and lines.Expenditure.isna().tolist() == [True, False]
    assert lines.Expenditure.iloc[1] == 16_325.
    out = collapse(lines.assign(Price=[99_999_999., 8_162.5]))
    assert len(out) == 1 and out.Quantity.iloc[0] == 3.
    assert out.Expenditure.iloc[0] == 16_325.
    # Existing aggregate-amount / aggregate-quantity rule, not a new policy.
    assert out.Price.iloc[0] == pytest.approx(16_325. / 3.)


@pytest.mark.parametrize("quantities", [[2., 3.], [np.nan, np.nan]])
def test_all_sentinel_components_remain_missing_not_zero(mapping, quantities):
    lines = mapping.food_acquired(raw([99_999_999., 99_999_999.], quantities))
    assert len(lines) == 2 and lines.Expenditure.isna().all()
    out = collapse(lines)
    assert len(out) == 1 and pd.isna(out.Expenditure.iloc[0])
    if all(pd.isna(x) for x in quantities):
        assert pd.isna(out.Quantity.iloc[0])
    else:
        assert out.Quantity.iloc[0] == 5.


@pytest.mark.parametrize("quantity", [np.nan, 0., -1.])
def test_original_positive_value_only_component_is_retained(mapping, quantity):
    out = mapping.food_acquired(raw([99_999_999.], [quantity]))
    assert len(out) == 1 and pd.isna(out.Expenditure.iloc[0])
    if pd.isna(quantity):
        assert pd.isna(out.Quantity.iloc[0])
    else:
        assert out.Quantity.iloc[0] == quantity


def test_only_exact_code_is_masked_healthy_neighbors_and_real_na_preserved(mapping):
    amounts = [99_999_998., 100_000_000., 9_999_999., 99_999_999.01, 0., np.nan]
    lines = mapping.food_acquired(raw(amounts))
    assert len(lines) == len(amounts)
    np.testing.assert_allclose(lines.Expenditure.to_numpy(), amounts, equal_nan=True)
    assert lines.Quantity.tolist() == [2.] * len(amounts)


@pytest.mark.parametrize("dtype", ["float64", "Float64", "object"])
def test_nullable_and_numeric_raw_representations_keep_missingness(mapping, dtype):
    frame = raw(pd.Series([99_999_999., 16_325., np.nan], dtype=dtype).array)
    lines = mapping.food_acquired(frame)
    assert lines.Expenditure.isna().tolist() == [True, False, True]
    assert lines.Expenditure.iloc[1] == 16_325.
    assert lines.Quantity.tolist() == [2., 2., 2.]


def test_other_acquisition_quantities_and_all_input_payload_are_unchanged(mapping):
    frame = raw([99_999_999., 16_325.], [99_999_999., 2.])
    frame["QuantityProduced"] = [99_999_999., 4.]
    frame["QuantityInkind"] = [5., 99_999_999.]
    before = deepcopy(frame)
    lines = mapping.food_acquired(frame)
    assert len(lines) == 6
    assert lines.xs("purchased", level="s").Quantity.tolist() == [99_999_999., 2.]
    assert lines.xs("produced", level="s").Quantity.tolist() == [99_999_999., 4.]
    assert lines.xs("inkind", level="s").Quantity.tolist() == [5., 99_999_999.]
    assert lines.xs("produced", level="s").Expenditure.isna().all()
    assert lines.xs("inkind", level="s").Expenditure.isna().all()
    pd.testing.assert_frame_equal(frame, before)
    assert frame.attrs == before.attrs


def test_existing_empty_component_selection_stays_unchanged(mapping):
    frame = raw([0., np.nan, -2.], [0., np.nan, -1.])
    assert mapping.food_acquired(frame).empty
