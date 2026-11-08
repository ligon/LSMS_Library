"""Native input amounts and livestock counts: warn/count, never correct.

Invented operands only.  Comparison boundaries follow ledger 1482-1485 §4;
the existing Site Q tests continue to pin its older tables' ladder.
"""
from pathlib import Path
import warnings

import pandas as pd
import pytest
import yaml

from lsms_library import quantity_audit as Q


@pytest.fixture(autouse=True)
def clean(monkeypatch):
    monkeypatch.delenv("LSMS_QUANTITY_STRICT", raising=False)
    Q._clear_quantity_reports()
    yield
    Q._clear_quantity_reports()


def cell(table="livestock", column="HeadCount", *, n=40, value=2.0,
         extreme=10000.0, **labels):
    axes = dict(t="w1", i=None, animal="Cattle") if table == "livestock" else dict(
        t="w1", i=None, input="Urea", crop="(not crop-specific)", u="kg")
    axes.update(labels)
    values = [value] * n + ([] if extreme is None else [extreme])
    data = {a: [v] * len(values) for a, v in axes.items()}
    data["i"] = [f"invented-{k}" for k in range(len(values))]
    out = pd.DataFrame(data).set_index(list(axes))
    out[column] = pd.array(values, dtype="Float64")
    return out


def audit(frame, table="livestock", column="HeadCount"):
    return Q.audit_quantities(frame, column, country="Invented", table=table)


def test_registry_covers_all_declared_head_counts_and_no_money_or_flags():
    """The census includes GhanaLSS's three extra flows, not just Malawi."""
    root = Path(__file__).resolve().parents[1] / "lsms_library" / "countries"
    schemes = []
    for path in sorted(root.glob("*/_/data_scheme.yml")):
        scheme = yaml.load(path.read_text(), Loader=yaml.BaseLoader).get(
            "Data Scheme", {}).get("livestock")
        if scheme is not None:
            schemes.append(scheme)
    heads = {col for scheme in schemes for col in scheme if col.startswith("Head")}
    assert set(Q._SCREENED_COLUMNS["livestock"]) == heads
    assert "RaisedOrOwned" not in heads
    assert Q._SCREENED_COLUMNS["plot_inputs"] == ("Quantity",)
    assert "livestock" not in Q._NEAR_ZERO_COLUMNS
    assert "plot_inputs" not in Q._NEAR_ZERO_COLUMNS


@pytest.mark.parametrize("column", [
    "HeadCount", "HeadAcquired", "HeadSold", "HeadCount12MonthsAgo",
    "HeadBornOrReceived", "HeadEaten", "HeadLostOrDied",
])
def test_each_head_column_warns_files_and_preserves_the_entire_operand(column):
    frame = cell(column=column)
    frame["HerdValue"] = pd.array([pd.NA] + [1e20] * (len(frame) - 1), dtype="Float64")
    frame["RaisedOrOwned"] = pd.array([True] * len(frame), dtype="boolean")
    frame.attrs = {"country": "Invented", "nested": {"source": ["invented"]}}
    before = frame.copy(deep=True)
    with pytest.warns(Q.QuantityImplausibleWarning, match="animal=Cattle"):
        returned = Q.check_quantities(frame, country="Invented", table="livestock")
    assert returned is frame
    pd.testing.assert_frame_equal(frame, before, check_exact=True)
    assert frame.attrs == before.attrs
    reports = Q.quantity_reports("Invented", "livestock")
    assert [(r["column"], r["direction"], r["n_implausible"]) for r in reports] == [
        (column, "high", 1)]
    offender = reports[0]["offenders"][0]
    assert offender["animal"] == offender["j"] == "Cattle"
    assert offender["reference"] == 2.0
    assert offender["rung"] == "t,animal"


def test_species_are_kept_separate_even_when_an_unrelated_j_column_exists():
    cows = cell(value=1.0, extreme=10000.0)
    hens = cell(animal="Chicken", value=1000.0, extreme=50000.0)
    frame = pd.concat([cows, hens])
    frame["j"] = "irrelevant crop"
    reports = audit(frame)
    assert reports[0]["n_implausible"] == 1
    assert reports[0]["offenders"][0]["animal"] == "Cattle"
    assert reports[0]["offenders"][0]["reference"] == 1.0


def test_a_thin_species_does_not_borrow_a_thick_species_reference():
    frame = pd.concat([cell(n=4), cell(animal="Chicken", n=80, extreme=None)])
    assert audit(frame) == []


def test_household_totals_without_animal_are_not_species_counts():
    frame = cell().reset_index("animal", drop=True)
    assert audit(frame) == []
    frame = frame.rename(columns={"HeadCount": "HerdValue"})
    with warnings.catch_warnings():
        warnings.simplefilter("error", Q.QuantityImplausibleWarning)
        assert Q.check_quantities(frame, country="Invented", table="livestock") is frame


@pytest.mark.parametrize("table,column", [("livestock", "HeadCount"),
                                          ("plot_inputs", "Quantity")])
@pytest.mark.parametrize("boundary", ["t", "u", "country", "period", "season", "visit"])
def test_new_screens_keep_each_supplied_boundary(table, column, boundary):
    low = cell(table, column, value=1.0, extreme=10000.0, **{boundary: "a"})
    high = cell(table, column, value=1e6, extreme=None, **{boundary: "b"})
    frame = pd.concat([low, high])
    reports = audit(frame, table, column)
    assert sum(r["n_implausible"] for r in reports) == 1
    report = reports[0]
    offender = report["offenders"][0]
    assert offender["reference"] == 1.0
    assert boundary in offender["rung"].split(",")
    if boundary == "country":
        assert report["country"] == "a" and report["rows"] == len(low)
    elif boundary == "t":
        assert report["wave"] == "a" and report["rows"] == len(low)
    # A thin partition stays unjudged rather than pooling to reach 30 positives.
    thin = pd.concat([low.iloc[:4], low.iloc[[-1]], high])
    assert audit(thin, table, column) == []


@pytest.mark.parametrize("table,column", [("livestock", "HeadCount"),
                                          ("plot_inputs", "Quantity")])
def test_country_reports_have_separate_denominators_and_ledger_keys(table, column):
    a = cell(table, column, country="A")
    b = cell(table, column, n=60, country="B")
    with pytest.warns(Q.QuantityImplausibleWarning) as caught:
        Q.check_quantities(pd.concat([a, b]), country="assembled", table=table)
    assert len(caught) == 2
    assert Q.quantity_reports("A", table)[0]["rows"] == len(a)
    assert Q.quantity_reports("B", table)[0]["rows"] == len(b)
    assert Q.quantity_reports("assembled", table) == []


@pytest.mark.parametrize("as_columns", [False, True])
def test_nullable_categorical_axes_and_missing_labels_are_not_dropped(as_columns):
    frame = cell(animal=pd.NA, u=pd.NA, period=pd.NA).reset_index()
    frame["animal"] = pd.Categorical(frame["animal"], categories=["unused"])
    frame["u"] = pd.Categorical(frame["u"], categories=["unused unit"])
    frame["period"] = pd.Categorical(frame["period"], categories=["unused period"])
    if as_columns:
        frame = frame.set_index(["t", "i"])
    else:
        frame = frame.set_index(["t", "i", "animal", "u", "period"])
    before = frame.copy(deep=True)
    reports = audit(frame)
    offender = reports[0]["offenders"][0]
    assert offender["animal"] is None and offender["u"] is None
    assert offender["period"] is None and offender["reference"] == 2.0
    pd.testing.assert_frame_equal(frame, before, check_exact=True)


def test_new_input_screen_uses_used_quantity_and_retains_native_units():
    frame = cell("plot_inputs", "Quantity", u="Tonne")
    frame["Quantity_purchased"] = 1e20
    frame["Unit_purchased"] = "Kilogramme"
    frame["Quantity_received_as_gift"] = 1e20
    frame["Unit_received_as_gift"] = "Litre"
    before = frame.copy(deep=True)
    with pytest.warns(Q.QuantityImplausibleWarning, match="'Quantity'"):
        returned = Q.check_quantities(frame, country="Invented", table="plot_inputs")
    assert returned is frame
    pd.testing.assert_frame_equal(frame, before, check_exact=True)
    reports = Q.quantity_reports("Invented", "plot_inputs")
    assert len(reports) == 1 and reports[0]["n_implausible"] == 1
    offender = reports[0]["offenders"][0]
    assert offender["u"] == "Tonne" and offender["input"] == "Urea"
    assert offender["reference"] == 2.0 and offender["value"] == 10000.0


def test_plot_inputs_try_crop_then_input_then_the_same_unit_only():
    table, column = "plot_inputs", "Quantity"
    a = cell(table, column, crop="A", n=4, value=1.0)
    b = cell(table, column, crop="B", n=40, value=1.0, extreme=None)
    report = audit(pd.concat([a, b]), table, column)[0]
    assert report["offenders"][0]["rung"] == "t,u,input"
    b = b.reset_index("input").assign(input="DAP").set_index("input", append=True)
    report = audit(pd.concat([a, b.reorder_levels(a.index.names)]), table, column)[0]
    assert report["offenders"][0]["rung"] == "t,u"
    # Even the coarsest input rung cannot borrow from a different native unit.
    b = b.reset_index("u").assign(u="Tonne").set_index("u", append=True)
    assert audit(pd.concat([a, b.reorder_levels(a.index.names)]), table, column) == []


@pytest.mark.parametrize("table,column", [("livestock", "HeadCount"),
                                          ("plot_inputs", "Quantity")])
def test_zeros_do_not_thicken_the_new_cells_and_lower_tail_is_not_added(table, column):
    frame = cell(table, column, n=80, value=0.0)
    assert audit(frame, table, column) == []
    frame = cell(table, column, value=2.0, extreme=1e-20)
    with warnings.catch_warnings():
        warnings.simplefilter("error", Q.QuantityImplausibleWarning)
        assert Q.check_quantities(frame, country="Invented", table=table) is frame


@pytest.mark.parametrize("table,column", [("livestock", "HeadCount"),
                                          ("plot_inputs", "Quantity")])
def test_quantity_strict_reuses_the_existing_lever_without_mutation(monkeypatch, table, column):
    frame = cell(table, column)
    before = frame.copy(deep=True)
    monkeypatch.setenv("LSMS_QUANTITY_STRICT", "yes")
    with pytest.raises(Q.QuantityImplausibleError):
        Q.check_quantities(frame, country="Invented", table=table)
    pd.testing.assert_frame_equal(frame, before, check_exact=True)
    assert Q.quantity_reports("Invented", table)[0]["n_implausible"] == 1


@pytest.mark.parametrize("table,column", [("livestock", "HeadCount"),
                                          ("plot_inputs", "Quantity")])
def test_real_finalizer_repeats_the_warning_after_an_invented_parquet_read(
        tmp_path, monkeypatch, table, column):
    from lsms_library.country import Country
    from lsms_library.local_tools import get_dataframe, to_parquet
    from lsms_library.paths import countries_root

    config = tmp_path / "countries" / "Invented" / "_"
    config.mkdir(parents=True)
    frame = cell(table, column)
    scheme = {"index": "(" + ", ".join(frame.index.names) + ")", column: "float"}
    (config / "data_scheme.yml").write_text(yaml.safe_dump({"Data Scheme": {table: scheme}}))
    monkeypatch.setenv("LSMS_COUNTRIES_ROOT", str(config.parent.parent))
    countries_root.cache_clear()
    try:
        country = Country("Invented")
        country._panel_ids_attempted = True
        country._updated_ids_cache = {}
        path = tmp_path / "invented.parquet"
        to_parquet(frame, path, absolute_path=True)
        payload = path.read_bytes()
        for _ in range(2):
            operand = get_dataframe(path)
            with pytest.warns(Q.QuantityImplausibleWarning, match="Invented/"):
                result = country._finalize_result(operand, scheme, table)
            assert result[column].max() == 10000.0
            assert len(result) == len(frame)
        assert path.read_bytes() == payload
        assert len(Q.quantity_reports("Invented", table)) == 1  # ledger de-duplicates.
    finally:
        countries_root.cache_clear()
