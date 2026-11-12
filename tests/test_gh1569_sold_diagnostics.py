"""Site Q sale diagnostics: invented Alpha/Beta operands and literal oracles.

No country constructor, provider, source, table build or cache is used. Ledger
1569 §§2–5 defines the reuse and native sold-denominator contract.
"""
from copy import deepcopy
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import quantity_audit as Q


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    monkeypatch.delenv("LSMS_QUANTITY_STRICT", raising=False)
    Q._clear_quantity_reports()
    yield
    Q._clear_quantity_reports()


def cell(*, n=40, quantity=2.0, price=10.0, extra=2000.0, **labels):
    axes = dict(country="Alpha", t="w1", v="cluster-a", i="", plot_id="plot-a",
                crop="crop-a", u="measure-a", source="source-a", season="season-a",
                production_obs="")
    axes.update(labels)
    amounts = [quantity] * n + ([] if extra is None else [extra])
    frame = pd.DataFrame({a: [v] * len(amounts) for a, v in axes.items()})
    frame["i"] = [f"{axes['country']}-{k}" for k in range(len(amounts))]
    frame["production_obs"] = [f"reported:Alpha:{k}" for k in range(len(amounts))]
    frame["Quantity"] = 100.0
    frame["Quantity_kg"] = 200.0
    frame["Quantity_sold"] = pd.array(amounts, dtype="Float64")
    frame["Value_sold"] = pd.array([price * q for q in amounts], dtype="Float64")
    frame["Unit_sold"] = labels.get("Unit_sold", "measure-a")
    frame["Condition_sold"] = labels.get("Condition_sold", "dry")
    # Unit_sold and Condition_sold are payload columns even when overridden.
    return frame.set_index([a for a in axes if a not in {"Unit_sold", "Condition_sold"}])


def audit(frame, column="Quantity_sold"):
    return Q.audit_quantities(frame, column, country="Alpha", table="crop_production")


def outliers(reports):
    return [r for r in reports if r["n_implausible"]]


def test_sale_only_extreme_is_screened_without_changing_normal_harvest():
    frame = cell()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert Q.check_quantities(frame, country="Alpha", table="crop_production") is frame
    assert len(caught) == 1 and caught[0].category is Q.QuantityImplausibleWarning
    reports = Q.quantity_reports("Alpha", "crop_production")
    assert [(r["column"], r["direction"], r["n_implausible"]) for r in reports] == [
        ("Quantity_sold", "high", 1)]
    row = reports[0]["offenders"][0]
    assert (row["i"], row["value"], row["reference"], row["ratio"]) == (
        "Alpha-40", 2000.0, 2.0, 1000.0)
    assert row["u"] == row["Unit_sold"] == "measure-a"
    assert row["source"] == "source-a" and row["Condition_sold"] == "dry"


def test_price_only_extreme_has_an_explicit_sold_unit_denominator():
    frame = cell(extra=2.0)
    frame.iloc[-1, frame.columns.get_loc("Value_sold")] = 2e6
    with pytest.warns(Q.QuantityImplausibleWarning, match="Sale_price.*Unit_sold"):
        Q.check_quantities(frame, country="Alpha", table="crop_production")
    reports = Q.quantity_reports("Alpha", "crop_production")
    assert [(r["column"], r["n_implausible"]) for r in reports] == [("Sale_price", 1)]
    assert reports[0]["definition"] == "Value_sold / Quantity_sold"
    assert reports[0]["unit_column"] == "Unit_sold"
    assert reports[0]["unit_semantics"] == "as-reported"
    row = reports[0]["offenders"][0]
    assert (row["value"], row["reference"], row["ratio"], row["Unit_sold"]) == (
        1e6, 10.0, 100000.0, "measure-a")
    assert row["quantity_sold"] == 2.0 and row["value_sold"] == 2e6
    assert "Sale_price" not in frame.columns


@pytest.mark.parametrize("column", ["Quantity_sold", "Sale_price"])
@pytest.mark.parametrize("boundary", [
    "country", "t", "season", "Unit_sold", "Condition_sold",
])
def test_each_sold_boundary_separates_scales_and_cannot_lend_thin_support(column, boundary):
    # Literal scale: 40 reports of 2, one of 2000; another cell's 1e8 must
    # neither drown that outlier nor lend its support to a thin first cell.
    a = cell(**{boundary: "Alpha"})
    b = cell(quantity=1e8, extra=None, **{boundary: "Beta"})
    if column == "Sale_price":
        a["Quantity_sold"] = 2.0
        a["Value_sold"] = pd.array([4.0] * 40 + [4000.0], dtype="Float64")
        b["Quantity_sold"] = 2.0
        b["Value_sold"] = 2e8
    reports = outliers(audit(pd.concat([a, b]), column))
    assert len(reports) == 1 and reports[0]["n_implausible"] == 1
    assert reports[0]["offenders"][0]["reference"] == 2.0
    assert reports[0]["offenders"][0]["ratio"] == 1000.0
    if boundary == "country":
        assert reports[0]["country"] == "Alpha" and reports[0]["rows"] == 41
    thin = pd.concat([a.iloc[:3], a.iloc[[-1]], b])
    assert outliers(audit(thin, column)) == []
    assert audit(thin, column) == []  # Existing no-reference contract.


def test_harvest_units_do_not_define_sold_cells_or_displace_explicit_labels():
    a = cell(u="harvest-only", Unit_sold="measure-a")
    b = cell(quantity=1e8, extra=None, u="harvest-only", Unit_sold="measure-b")
    reports = audit(pd.concat([a, b]))
    row = outliers(reports)[0]["offenders"][0]
    assert (row["reference"], row["u"], row["harvest_u"]) == (
        2.0, "measure-a", "harvest-only")
    assert len(reports) == 1 and reports[0]["unit_semantics"] == "as-reported"
    # The same sold unit remains comparable across different harvest units.
    a = cell(n=3, u="harvest-a", Unit_sold="measure-a")
    b = cell(extra=None, u="harvest-b", Unit_sold="measure-a")
    assert outliers(audit(pd.concat([a, b])))[0]["offenders"][0]["reference"] == 2.0


@pytest.mark.parametrize("column", ["Quantity_sold", "Sale_price"])
def test_crop_falls_to_same_sold_unit_state_cell_under_existing_ladder_convention(column):
    a = cell(n=3, crop="crop-a")
    b = cell(extra=None, crop="crop-b")
    if column == "Sale_price":
        a["Quantity_sold"] = 2.0
        a["Value_sold"] = pd.array([4.0] * 3 + [4000.0], dtype="Float64")
        b["Quantity_sold"] = 2.0
        b["Value_sold"] = 4.0
    row = outliers(audit(pd.concat([a, b]), column))[0]["offenders"][0]
    assert row["reference"] == 2.0 and row["j"] == "crop-a"
    assert row["rung"] == "country,t,season,Unit_sold,Condition_sold"


def test_cluster_and_source_are_row_identity_not_new_comparison_support_gates():
    a = cell(n=3, v="cluster-a", source="source-a")
    b = cell(extra=None, v="cluster-b", source="source-b")
    reports = audit(pd.concat([a, b]))
    assert len(reports) == 1 and reports[0]["rows"] == 44
    row = reports[0]["offenders"][0]
    assert (row["v"], row["source"], row["reference"], row["production_obs"]) == (
        "cluster-a", "source-a", 2.0, "reported:Alpha:3")
    assert ["source", "source-a"] in row["index"]


@pytest.mark.parametrize("unit", [None, pd.NA, "Unknown", "manquant", ""])
@pytest.mark.parametrize("absent_column", [False, True])
def test_missing_sold_denominators_remain_unassessed_without_harvest_or_factor_fallback(unit, absent_column):
    frame = cell()
    frame["Unit_sold"] = unit
    if absent_column:
        frame = frame.drop(columns="Unit_sold")
    frame["KgFactor_sold"] = 1.0
    frame["KgFactor"] = 1.0
    before = frame.copy(deep=True)
    for column in ("Quantity_sold", "Sale_price"):
        assert audit(frame, column) == []
    pd.testing.assert_frame_equal(frame, before, check_exact=True)


@pytest.mark.parametrize("field", ["Quantity_sold", "Value_sold"])
@pytest.mark.parametrize("bad", [0.0, -1.0, np.nan, np.inf, -np.inf, "junk"])
def test_unusable_price_amounts_do_not_set_scale_or_become_numeric_outliers(field, bad):
    frame = cell(extra=2.0)
    frame[field] = frame[field].astype(object)
    frame.iloc[-1, frame.columns.get_loc(field)] = bad
    before = frame.copy(deep=True)
    assert audit(frame, "Sale_price") == []
    pd.testing.assert_frame_equal(frame, before, check_exact=True)
    # Sale value is not a quantity-eligibility gate.
    if field == "Value_sold":
        assert audit(frame) == []


@pytest.mark.parametrize("quantity,value", [(1e-300, 1e300), (1e300, 1e-300)])
def test_nonfinite_or_underflowed_price_ratio_is_unassessed(quantity, value):
    frame = cell(extra=2.0)
    frame.iloc[-1, frame.columns.get_loc("Quantity_sold")] = quantity
    frame.iloc[-1, frame.columns.get_loc("Value_sold")] = value
    reports = audit(frame, "Sale_price")
    assert reports == []


@pytest.mark.parametrize("value", [0.0, -1.0, np.nan, np.inf])
def test_sold_quantity_is_screened_even_when_sale_value_is_unusable(value):
    frame = cell()
    frame.iloc[-1, frame.columns.get_loc("Value_sold")] = value
    reports = outliers(audit(frame))
    assert reports[0]["offenders"][0]["value"] == 2000.0
    assert reports[0]["offenders"][0]["reference"] == 2.0


@pytest.mark.parametrize("ordinary,expected", [(28, 0), (29, 1)])
def test_positive_support_boundary_counts_the_outlier_but_not_zero_rows(ordinary, expected):
    frame = cell(n=ordinary)
    zeros = cell(n=100, quantity=0.0, extra=None)
    reports = audit(pd.concat([frame, zeros]))
    assert sum(r["n_implausible"] for r in outliers(reports)) == expected
    if not expected:
        assert reports == []


@pytest.mark.parametrize("column", ["Quantity_sold", "Sale_price"])
@pytest.mark.parametrize("extreme,expected", [(200.0, 0), (200.2, 1)])
def test_existing_strictly_greater_than_100_tolerance_is_reused(column, extreme, expected):
    frame = cell(extra=extreme)
    if column == "Sale_price":
        frame["Quantity_sold"] = 2.0
        frame["Value_sold"] = pd.array([4.0] * 40 + [2 * extreme], dtype="Float64")
    assert sum(r["n_implausible"] for r in outliers(audit(frame, column))) == expected


@pytest.mark.parametrize("column", ["Quantity_sold", "Sale_price"])
def test_one_native_unit_floor_is_reused_and_no_lower_tail_is_added(column):
    frame = cell(quantity=0.01, extra=101.0, price=1.0)
    if column == "Sale_price":
        frame["Quantity_sold"] = 1.0
    row = outliers(audit(frame, column))[0]["offenders"][0]
    assert row["reference"] == 1.0 and row["ratio"] == 101.0
    frame = cell(extra=1e-20)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        Q.check_quantities(frame, country="Alpha", table="crop_production")
    assert not caught


def test_farm_level_no_harvest_and_duplicate_native_rows_keep_full_identity():
    frame = cell(plot_id="Farm-level").drop(columns=["Quantity", "Quantity_kg"])
    frame = frame.reset_index("u", drop=True)
    second = frame.iloc[[-1]].reset_index()
    second["production_obs"] = "reported:Beta:41"
    second = second.set_index(frame.index.names)
    frame = pd.concat([frame, second])
    rows = outliers(audit(frame))[0]["offenders"]
    assert len(rows) == 2 and {o["plot"] for o in rows} == {"Farm-level"}
    assert {o["production_obs"] for o in rows} == {"reported:Alpha:40", "reported:Beta:41"}
    assert {o["row_position"] for o in rows} == {40, 41}
    assert all(o["harvest_u"] is None and len(o["index"]) == frame.index.nlevels for o in rows)


def test_missing_sale_condition_never_borrows_the_harvest_condition():
    a = cell(n=3)
    a["Condition_sold"] = pd.NA
    a["condition"] = "fresh"
    b = cell(extra=None)
    b["condition"] = "fresh"
    reports = audit(pd.concat([a, b]))
    assert reports == []
    # If the entire sold-condition column is absent, harvest state still
    # cannot supply it or create a new support partition.
    a = cell(n=3).drop(columns="Condition_sold").assign(condition="fresh")
    b = cell(extra=None).drop(columns="Condition_sold").assign(condition="dry")
    row = outliers(audit(pd.concat([a, b])))[0]["offenders"][0]
    assert row["Condition_sold"] is None and row["reference"] == 2.0


@pytest.mark.parametrize("as_columns", [False, True])
@pytest.mark.parametrize("crop_name,plot_name", [("crop", "plot_id"), ("j", "plot")])
def test_native_aliases_columns_and_nullable_categorical_sold_labels(as_columns, crop_name, plot_name):
    frame = cell().rename_axis(index={"crop": crop_name, "plot_id": plot_name})
    frame["Unit_sold"] = pd.Categorical(frame["Unit_sold"], categories=["measure-a", "unused"])
    frame["Condition_sold"] = pd.Categorical([pd.NA] * len(frame), categories=["dry"])
    if as_columns:
        frame = frame.reset_index()
    row = outliers(audit(frame))[0]["offenders"][0]
    assert (row["j"], row["plot"], row["reference"], row["Unit_sold"]) == (
        "crop-a", "plot-a", 2.0, "measure-a")
    assert row["Condition_sold"] is None


def test_price_without_quantity_column_has_no_denominator_and_is_unassessed():
    assert audit(cell().drop(columns="Quantity_sold"), "Sale_price") == []
    assert audit(cell().drop(columns="Value_sold"), "Sale_price") == []


def test_no_price_or_kilogram_fallback_and_existing_virtual_name_is_untouched():
    frame = cell(extra=2.0)
    frame["Price"] = 1e20
    frame["Sale_price"] = 1e30
    frame["KgFactor_sold"] = 1e-30
    frame["KgFactor"] = 1e30
    frame["Quantity_kg"] = 1e30
    before = frame.copy(deep=True)
    assert audit(frame, "Sale_price") == []
    assert audit(frame.drop(columns="Value_sold"), "Sale_price") == []
    pd.testing.assert_frame_equal(frame, before, check_exact=True)


def test_absent_sale_fields_and_zero_or_missing_non_sales_are_silent():
    assert audit(cell().drop(columns="Quantity_sold")) == []
    for value in (0.0, pd.NA):
        frame = cell()
        frame["Quantity_sold"] = value
        frame["Value_sold"] = value
        frame["Unit_sold"] = pd.NA
        assert audit(frame) == audit(frame, "Sale_price") == []


def test_warn_strict_and_repeated_reads_preserve_values_axes_dtypes_and_nested_attrs(monkeypatch):
    frame = cell(u="harvest-only").iloc[::-1]
    frame["Unit_sold"] = pd.Categorical(frame["Unit_sold"], categories=["measure-a", "unused"])
    frame["Condition_sold"] = pd.Categorical(frame["Condition_sold"], categories=["dry", "unused"])
    frame.attrs = {"id_converted": True, "nested": {"Alpha": [1, None, {"Beta": "kept"}]}}
    before = frame.copy(deep=True)
    attrs = deepcopy(frame.attrs)
    with pytest.warns(Q.QuantityImplausibleWarning):
        assert Q.check_quantities(frame, country="Alpha", table="crop_production") is frame
    first = deepcopy(Q.quantity_reports("Alpha", "crop_production"))
    with pytest.warns(Q.QuantityImplausibleWarning):
        Q.check_quantities(frame, country="Alpha", table="crop_production")
    assert Q.quantity_reports("Alpha", "crop_production") == first
    monkeypatch.setenv("LSMS_QUANTITY_STRICT", "yes")
    with pytest.raises(Q.QuantityImplausibleError, match="Quantity_sold"):
        Q.check_quantities(frame, country="Alpha", table="crop_production")
    pd.testing.assert_frame_equal(frame, before, check_exact=True)
    assert frame.attrs == attrs


def test_unassessable_sales_remain_silent_under_existing_strict_mode(monkeypatch):
    monkeypatch.setenv("LSMS_QUANTITY_STRICT", "yes")
    for frame in (cell().drop(columns="Unit_sold"), cell(n=3)):
        before = frame.copy(deep=True)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            assert Q.check_quantities(frame, country="Alpha", table="crop_production") is frame
        assert caught == [] and Q.quantity_reports() == []
        pd.testing.assert_frame_equal(frame, before, check_exact=True)


def test_legacy_harvest_report_is_exact_and_sold_metadata_does_not_rekey_it():
    frame = cell(extra=None)
    frame.iloc[-1, frame.columns.get_loc("Quantity")] = 10001.0
    reports = Q.audit_quantities(frame, "Quantity", country="Alpha", table="crop_production")
    assert reports == [{
        "site": "built-table-quantity", "country": "Alpha", "table": "crop_production",
        "column": "Quantity", "direction": "high", "wave": "w1", "rows": 40,
        "n_implausible": 1, "max_ratio": 100.0, "k": 100.0,
        "reference_quantile": 0.9, "min_cell": 30, "reference_floor": 1.0,
        "offenders": [{"i": "Alpha-39", "plot": "plot-a", "j": "crop-a", "u": "measure-a",
                       "value": 10001.0, "reference": 100.0, "ratio": 100.0, "rung": "t,u,j"}]}]
    # No sold fields means the old two-column behavior, including lower tail.
    frame = frame.drop(columns=["Quantity_sold", "Value_sold", "Unit_sold", "Condition_sold"])
    frame.iloc[0, frame.columns.get_loc("Quantity")] = 0.01
    with pytest.warns(Q.QuantityImplausibleWarning):
        Q.check_quantities(frame, country="Alpha", table="crop_production")
    assert [(r["column"], r["direction"], r["n_implausible"]) for r in Q.quantity_reports()] == [
        ("Quantity", "high", 1), ("Quantity", "low", 1)]
