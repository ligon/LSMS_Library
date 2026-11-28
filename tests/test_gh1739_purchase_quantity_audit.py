"""#1739: native purchase amounts need their own quantity and reference cell."""
import warnings

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from lsms_library import quantity_audit as q


@pytest.fixture(autouse=True)
def clean_reports(monkeypatch):
    monkeypatch.delenv("LSMS_QUANTITY_STRICT", raising=False)
    q._clear_quantity_reports()
    yield
    q._clear_quantity_reports()


def purchases(n=40, *, u="Litre", unit="Bag", input_kind="Urea", crop="Maize",
              t="2021-22", country="Fixture", period="A", season="rainy", visit="1",
              qty=3.0, spike=None):
    df = pd.DataFrame(dict(country=country, t=t, i=range(n), plot_id="P", crop=crop,
                           input=input_kind, u=u, period=period, season=season, visit=visit,
                           Quantity=2.0, Unit_purchased=unit, Quantity_purchased=qty,
                           Quantity_received_as_gift=1e8, Unit_received_as_gift="Kg"))
    if spike is not None:
        df.loc[0, "Quantity_purchased"] = spike
    return df.set_index(["country", "t", "i", "plot_id", "crop", "input", "u"])


def audit(frame):
    return q.audit_quantities(frame, "Quantity_purchased", country="Fixture",
                              table="plot_inputs")


def test_warning_and_ledger_compare_reported_purchase_units_not_applied_units():
    left = purchases(spike=9000)
    right = purchases(u="Litre", unit="Kg", qty=1e6)
    frame = pd.concat([left, right])
    frame.attrs["proof"] = "unchanged"
    before = frame.copy(deep=True)
    with pytest.warns(q.QuantityImplausibleWarning, match=r"Unit_purchased=Bag") as caught:
        assert q.check_quantities(frame, country="Fixture", table="plot_inputs") is frame
    assert len(caught) == 1
    assert_frame_equal(frame, before, check_exact=True)
    assert frame.attrs == before.attrs
    reports = q.quantity_reports("Fixture", "plot_inputs")
    assert len(reports) == 1
    report = reports[0]
    assert report["column"] == "Quantity_purchased"
    assert report["unit_column"] == "Unit_purchased"
    assert report["n_implausible"] == 1
    offender, = report["offenders"]
    assert offender["Unit_purchased"] == offender["u"] == "Bag"
    assert offender["reference"] == 3.0
    assert offender["ratio"] == 3000.0
    assert "Unit_purchased" in offender["rung"].split(",")


@pytest.mark.parametrize("unit", [None, "", " ", "Unknown", "Manquant", "Value"])
def test_no_guess_for_unsupported_purchase_unit(unit):
    frame = purchases(unit=unit, spike=1e10)
    assert audit(frame) == []
    assert audit(frame.drop(columns="Unit_purchased")) == []


@pytest.mark.parametrize("different", [
    {"unit": "Kg"}, {"input_kind": "Seed"}, {"t": "2018-19"}, {"country": "Other"},
    {"period": "B"}, {"season": "dry"}, {"visit": "2"},
])
def test_thin_cell_cannot_borrow_a_reference_across_boundaries(different):
    left = purchases(n=1, spike=1e6)
    right = purchases(qty=1.0, **different)
    assert audit(pd.concat([left, right])) == []


def test_a_purchase_reference_can_pool_crops_but_not_applied_units():
    left = purchases(n=1, spike=1e6, crop="Maize")
    right = purchases(crop="Millet", u="Drum")
    report, = audit(pd.concat([left, right]))
    offender, = report["offenders"]
    assert offender["reference"] == 3.0
    assert offender["rung"].endswith("Unit_purchased,input")
    assert "j" not in offender["rung"].split(",")


def test_only_positive_amounts_set_the_scale_and_no_new_lower_tail():
    frame = pd.concat([purchases(qty=0.0), purchases(n=1, spike=1e6)])
    assert audit(frame) == []
    frame = purchases(spike=1e-20)
    with warnings.catch_warnings():
        warnings.simplefilter("error", q.QuantityImplausibleWarning)
        assert q.check_quantities(frame, country="Fixture", table="plot_inputs") is frame


def test_strict_mode_is_existing_optional_error(monkeypatch):
    frame = purchases(spike=1e6)
    before = frame.copy(deep=True)
    monkeypatch.setenv("LSMS_QUANTITY_STRICT", "1")
    with pytest.raises(q.QuantityImplausibleError, match=r"'Quantity_purchased'"):
        q.check_quantities(frame, country="Fixture", table="plot_inputs")
    assert_frame_equal(frame, before, check_exact=True)
