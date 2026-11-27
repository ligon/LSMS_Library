"""GH #828: the disclosure is counts, not a new expenditure model.

All acquisitions, sample rows, and requested variant inputs are invented.
Use the generated Country method, real post-processing, and Feature assembly.
"""
from __future__ import annotations

from collections import Counter
import importlib
import json
from unittest.mock import patch
import warnings

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import conversion as FX
from lsms_library import transformations as T
from lsms_library.errors import (
    FoodExpenditureCoverageWarning, LabelUnavailableError, PeriodUnavailableError,
)

R = importlib.import_module("lsms_library.recall")
C = ll.Country
F = ll.Feature
KEY = "food_expenditure_coverage"
OLD = "2018-19"
NEW = "2021-22"


def _row(wave, role, qty, value, *, item="fixture-grain", unit="Kg", hh="fixture-hh"):
    return (wave, "fixture-cluster", hh, item, unit, role, qty, value)


def _acquired(*rows):
    names = ["t", "v", "i", "j", "u", "s", "Quantity", "Expenditure"]
    return pd.DataFrame(rows, columns=names).astype(
        {"Quantity": "float64", "Expenditure": "float64"}).set_index(names[:6])


def _partition(wave=NEW):
    return [
        _row(wave, "purchased", 3, 60),
        _row(wave, "produced", 2, None),
        _row(wave, "inkind", 4, None, item="fixture-gift"),
    ]


@pytest.fixture
def reads(monkeypatch):
    frames, calls = {}, Counter()
    original_relabel = C._relabel_j

    def load(self, waves, table, **kw):
        calls[self.name, table] += 1
        data = frames[self.name]
        if isinstance(data, Exception):
            raise data
        flat = data.index.to_frame(index=False)
        if table == "sample":
            return flat[["t", "i", "v"]].drop_duplicates(["t", "i"]).set_index(
                ["i", "t"]).assign(Region="fixture-region")
        if table == "cluster_features":
            return flat[["t", "v"]].drop_duplicates().set_index(
                ["t", "v"]).assign(Region="fixture-region")
        assert table == "food_acquired", (self.name, table, waves, kw)
        if waves is not None:
            data = data.loc[data.index.get_level_values("t").isin(waves)]
        return data.copy()

    # Only block nonexistent derived-build fallback and replace source leaves.
    monkeypatch.setattr(C, "_aggregate_wave_data", load)
    monkeypatch.setattr(C, "_legacy_can_build", lambda *args: False)
    monkeypatch.setattr(C, "categorical_mapping", property(lambda self: {
        "food_items": pd.DataFrame({"Preferred Label": ["fixture-grain", "fixture-gift"],
                                    "Group Label": ["fixture-group", "fixture-group"]})
    }))
    monkeypatch.setattr(F, "countries", property(lambda self: list(frames)))
    return frames, calls, original_relabel


def _invoke(method, *args, **opts):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        result = method(*args, **opts)
    return result, caught


def _coverage_warnings(caught):
    return [w for w in caught if issubclass(w.category, FoodExpenditureCoverageWarning)]


def _assert_old_contract(method, *args, **opts):
    """Compare exact ordinary results and warnings with the observer inactive."""
    module = importlib.import_module("lsms_library._food_expenditure_coverage")
    got, caught = _invoke(method, *args, **opts)
    counts = got.attrs.pop(KEY, None)
    with patch.object(module, "_reviewed_sources", return_value={}):
        expected, old_warnings = _invoke(method, *args, **opts)
    pd.testing.assert_frame_equal(got, expected, check_exact=True)
    assert got.attrs == expected.attrs
    assert [(w.category, str(w.message)) for w in caught if w not in _coverage_warnings(caught)] == [
        (w.category, str(w.message)) for w in old_warnings]
    if counts is not None:
        got.attrs[KEY] = counts
    return got, caught


def _cell(df, country="Niger", wave=NEW):
    return df.attrs[KEY][country][wave]


def test_country_counts_acquisitions_not_aggregate_keys(reads):
    frames, _, _ = reads
    frames["Niger"] = _acquired(
        *_partition(),
        _row(NEW, "produced", 1, None, unit="Litre"),
        _row(NEW, "inkind", None, 19, unit="Value"),
        _row(NEW, "inkind", 2, 0, hh="fixture-zero"),
        _row(NEW, "purchased", 2, None, hh="fixture-missing-purchase"),
    )
    got, caught = _assert_old_contract(C("Niger").food_expenditures, basis="total")
    cell = _cell(got)
    assert cell["result"] == "served"
    assert cell["roles"]["produced"] == {
        "source": {
            "file": "2021-22/Data/s07b_me_ner2021.dta", "section": "7B",
            "quantity_question": "asked", "quantity_field": "s07bq04",
            "value_question": "not_asked", "producer_value": "unvalued",
            "producer": "2021-22/_/mapping.py::food_acquired",
        },
        "acquisition_rows": 2, "input_value_present": 0, "input_zero": 0,
        "observed_missing_input": 2, "imputed": 0,
        "imputed_from_input_zero": 0, "remaining_unvalued": 2,
    }
    gift = cell["roles"]["inkind"]
    assert gift["source"]["quantity_field"] == "s07bq05"
    assert (gift["acquisition_rows"], gift["input_value_present"], gift["input_zero"],
            gift["remaining_unvalued"]) == (3, 2, 1, 1)
    assert sorted(got["Expenditure"].tolist()) == [19, 60]
    warning, = _coverage_warnings(caught)
    assert "Niger 2021-22 produced: 2" in str(warning.message)
    serialized = json.dumps(got.attrs[KEY], allow_nan=False)
    assert "fixture-" not in serialized + str(warning.message)
    assert "19.0" not in serialized + str(warning.message)


@pytest.mark.parametrize("kwargs", [{}, {"basis": "purchased"}])
def test_default_and_explicit_purchased_do_not_disclose(reads, kwargs):
    frames, _, _ = reads
    frames["Niger"] = _acquired(*_partition())
    got, caught = _invoke(C("Niger").food_expenditures, **kwargs)
    assert got["Expenditure"].tolist() == [60]
    assert not _coverage_warnings(caught)
    assert KEY not in got.attrs


@pytest.mark.parametrize("rows", [
    [],  # absent role, but a purchased row establishes the wave
    [_row(NEW, "produced", 3, 0)],
    [_row(NEW, "produced", 0, None), _row(NEW, "inkind", None, None),
     _row(NEW, "produced", float("inf"), None), _row(NEW, "inkind", 2, None, unit="Value")],
    [_row(NEW, "inkind", None, 19, unit="Value")],
    [_row(NEW, "purchased", 3, None, hh="fixture-missing-purchased")],
])
def test_no_remaining_gap_controls(reads, rows):
    frames, _, _ = reads
    frames["Niger"] = _acquired(_partition()[0], *rows)
    got, caught = _invoke(C("Niger").food_expenditures, basis="total")
    assert not _coverage_warnings(caught)
    for role in _cell(got)["roles"].values():
        assert role["remaining_unvalued"] == 0


@pytest.mark.parametrize("axis", ["t", "s", "u", "Quantity"])
def test_missing_evidence_is_not_zero_coverage(reads, axis):
    frames, _, _ = reads
    frame = _acquired(*_partition())
    if axis == "Quantity":
        frame = frame.drop(columns=axis)
    else:
        frame = frame.droplevel(axis)
    frames["Niger"] = frame
    got, caught = _invoke(C("Niger").food_expenditures, basis="total")
    assert got["Expenditure"].tolist() == [60]
    assert not _coverage_warnings(caught)
    assert KEY not in got.attrs


def test_same_scope_selection_and_unsupported_bypasses(reads):
    frames, _, _ = reads
    frames["Burkina_Faso"] = _acquired(
        *_partition("2014"), *_partition(NEW),
        _row("2014", "inkind", None, 29, unit="Unknown"),
    )
    c = C("Burkina_Faso")
    older, caught = _invoke(c.food_expenditures, waves=["2014"], basis="total")
    assert older["Expenditure"].sum() == 89
    assert KEY not in older.attrs and not _coverage_warnings(caught)
    got, caught = _invoke(c.food_expenditures, basis="total")
    assert set(got.attrs[KEY]["Burkina_Faso"]) == {NEW}
    assert len(_coverage_warnings(caught)) == 1
    # Same shape/labels alone do not establish review on the bare transform
    # or on the unchecked-cache path.
    data = frames["Burkina_Faso"].assign()
    data.attrs["country"] = "Burkina_Faso"
    for method, args in (
        (T.food_expenditures_from_acquired, [data]),
        (C("Burkina_Faso", assume_cache_fresh=True).food_expenditures, []),
    ):
        out, caught = _invoke(method, *args, basis="total")
        assert KEY not in out.attrs and not _coverage_warnings(caught)


def test_reviewed_scope_matches_the_shipped_wave_bindings():
    from lsms_library._food_expenditure_coverage import _SOURCE_FILES

    assert set(_SOURCE_FILES) == {"Burkina_Faso", "Mali", "Niger"}
    for country, files in _SOURCE_FILES.items():
        assert set(files) == {OLD, NEW}
        for wave, file in files.items():
            binding = C(country)[wave].resources["food_acquired"]
            assert binding["file"] == file
            assert binding["myvars"]["Produced"] == "s07bq04"
            assert binding["myvars"]["GiftQuantity"] == "s07bq05"
            assert "ProducedValue" not in binding["myvars"]
            assert "GiftValue" not in binding["myvars"]


@pytest.mark.parametrize("valuation, gift_gap", [
    ("own_price", 1),
    (("own_price", "median_price"), 0),
])
def test_opt_in_valuation_and_explicit_input_zero(reads, valuation, gift_gap):
    frames, _, _ = reads
    frames["Niger"] = _acquired(
        *_partition(),
        _row(NEW, "inkind", 0.5, 0),
        _row(NEW, "purchased", 1, 30, item="fixture-gift", hh="fixture-neighbor"),
    )
    got, caught = _assert_old_contract(
        C("Niger").food_expenditures, basis="total", valuation=valuation)
    roles = _cell(got)["roles"]
    assert roles["produced"]["observed_missing_input"] == roles["produced"]["imputed"] == 1
    assert roles["produced"]["remaining_unvalued"] == 0
    assert roles["inkind"]["input_zero"] == roles["inkind"]["imputed_from_input_zero"] == 1
    assert roles["inkind"]["observed_missing_input"] == 1
    assert roles["inkind"]["remaining_unvalued"] == gift_gap
    assert roles["inkind"]["imputed"] == 2 - gift_gap
    assert len(_coverage_warnings(caught)) == gift_gap
    assert any(isinstance(w.message, T.ValuationLadderWarning) for w in caught)
    assert got.attrs["valuation_sources"]["own_price"] == 2
    assert 10 in got["Expenditure"].tolist()


def test_unpriced_input_zero_is_not_a_gap_after_opt_in(reads):
    frames, _, _ = reads
    frames["Niger"] = _acquired(
        _partition()[0], _row(NEW, "produced", 2, 0, item="fixture-no-donor"),
    )
    got, caught = _assert_old_contract(F("food_expenditures"), ["Niger"],
                                       basis="total", valuation="own_price")
    role = _cell(got)["roles"]["produced"]
    assert role["input_zero"] == 1
    assert role["imputed"] == role["observed_missing_input"] == role["remaining_unvalued"] == 0
    assert got.attrs["valuation_sources"]["none"] == 1
    assert got["Expenditure"].tolist() == [60]
    assert not _coverage_warnings(caught)


def test_country_empty_and_feature_single_mixed_and_all_empty(reads):
    frames, _, _ = reads
    frames["Niger"] = _acquired(_partition()[1])
    frames["Mali"] = _acquired(*_partition())
    country, w = _assert_old_contract(C("Niger").food_expenditures, basis="total")
    assert country.empty and _cell(country)["result"] == "no_expenditure_rows"
    assert len(_coverage_warnings(w)) == 1

    feature = F("food_expenditures")
    single, w = _assert_old_contract(feature, ["Niger"], basis="total")
    assert single.empty and _cell(single)["result"] == "no_expenditure_rows"
    assert single.index.names[0] == "country"
    assert len(_coverage_warnings(w)) == 1
    assert sum("No data" in str(x.message) for x in w) == 1

    mixed, w = _assert_old_contract(feature, ["Mali", "Niger"], basis="total")
    assert not mixed.empty and _cell(mixed)["result"] == "no_expenditure_rows"
    assert _cell(mixed, "Mali")["result"] == "served"
    assert len(_coverage_warnings(w)) == 1
    assert mixed.index.get_level_values("country").unique().tolist() == ["Mali"]

    frames["Mali"] = _acquired(_partition()[2])
    empty, w = _assert_old_contract(feature, ["Mali", "Niger"], basis="total")
    assert empty.empty and set(empty.attrs[KEY]) == {"Niger", "Mali"}
    assert all(_cell(empty, c)["result"] == "no_expenditure_rows" for c in frames)
    assert len(_coverage_warnings(w)) == 1
    assert sum("No data" in str(x.message) for x in w) == 2


@pytest.mark.parametrize("rejection", ["build", "label", "shape", "required"])
def test_feature_only_discloses_kept_or_valid_empty_scopes(reads, monkeypatch, rejection):
    frames, _, relabel = reads
    for c in ["Niger", "Mali", "Burkina_Faso"]:
        frames[c] = _acquired(*_partition())
    if rejection == "build":
        frames["Niger"] = FileNotFoundError("invented source failure")
    else:
        def finish(self, df, *args, **kwargs):
            if self.name != "Niger":
                return relabel(self, df, *args, **kwargs)
            if rejection == "label":
                raise LabelUnavailableError("fixture label unavailable")
            return df.droplevel({"shape": "s", "required": "j"}[rejection])
        monkeypatch.setattr(C, "_relabel_j", finish)
    out, w = _assert_old_contract(
        F("food_expenditures"), list(frames), currency=None, basis="total")
    assert set(out.attrs[KEY]) == {"Mali", "Burkina_Faso"}
    assert len(_coverage_warnings(w)) == 1
    assert "Niger" not in str(_coverage_warnings(w)[0].message)
    assert "Niger" not in out.index.get_level_values("country")
    assert any("Niger" in str(x.message) for x in w)
    if rejection == "label":
        assert out.attrs["labels_unavailable"] == ["Niger"]
    if rejection == "required":
        assert set(out.attrs["index_unavailable"]) == {"Niger"}


@pytest.mark.parametrize("mode", ["rejected_gap", "kept_gap", "empty_gap"])
def test_period_continuation_prunes_only_rejected_waves(reads, monkeypatch, mode):
    frames, calls, _ = reads
    good = "2014" if mode == "empty_gap" else NEW
    rejected = NEW if mode == "empty_gap" else OLD
    nonempty = [_row(good, "purchased", 1, 70), _row(rejected, "purchased", 1, 140)]
    gap = {"rejected_gap": rejected, "kept_gap": good, "empty_gap": OLD}[mode]
    frames["Burkina_Faso"] = _acquired(*nonempty, _row(gap, "produced", 2, None))
    monkeypatch.setattr(R, "recall_records", lambda country: {
        good: R.RecallRecord(country=country, wave=good, basis="questionnaire",
                             ask_count_basis="declared", recall_days=7, n_asks=1)
    })
    c = C("Burkina_Faso")
    with warnings.catch_warnings(record=True) as strict_warnings:
        warnings.simplefilter("always")
        with pytest.raises(PeriodUnavailableError):
            c.food_expenditures(basis="total", period="day")
    assert not _coverage_warnings(strict_warnings)
    assert calls["Burkina_Faso", "food_acquired"] == 1

    calls.clear()
    out, w = _invoke(F("food_expenditures"), ["Burkina_Faso"], basis="total", period="day")
    assert calls["Burkina_Faso", "food_acquired"] == 1
    assert out["Expenditure"].tolist() == [10]
    assert out.attrs["period_unavailable"] == {"Burkina_Faso": [rejected]}
    assert set(out.attrs["period"]["Burkina_Faso"]["waves"]) == {good}
    assert rejected not in out.attrs[KEY]["Burkina_Faso"]
    if mode == "empty_gap":
        assert _cell(out, "Burkina_Faso", OLD)["result"] == "no_expenditure_rows"
    assert len(_coverage_warnings(w)) == (mode != "rejected_gap")


def test_output_empty_does_not_extend_period_validation(reads, monkeypatch):
    frames, _, _ = reads
    frames["Niger"] = _acquired(_partition()[1])
    monkeypatch.setattr(R, "recall_records", lambda country: {})
    c = C("Niger")
    got, w = _assert_old_contract(c.food_expenditures, basis="total", period="day")
    assert got.empty and got.attrs["period"]["Niger"]["waves"] == {}
    assert _cell(got)["result"] == "no_expenditure_rows"
    assert len(_coverage_warnings(w)) == 1
    got, w = _assert_old_contract(F("food_expenditures"), ["Niger"], basis="total", period="day")
    assert got.empty and "period" not in got.attrs and "period_unavailable" not in got.attrs
    assert _cell(got)["result"] == "no_expenditure_rows"
    assert len(_coverage_warnings(w)) == 1


def test_no_period_survivor_does_not_publish_unfinished_coverage(reads, monkeypatch):
    frames, calls, _ = reads
    frames["Niger"] = _acquired(*_partition(), _row(OLD, "inkind", 3, None))
    monkeypatch.setattr(R, "recall_records", lambda country: {})
    got, caught = _invoke(F("food_expenditures"), ["Niger"], basis="total", period="day")
    assert calls["Niger", "food_acquired"] == 1
    assert got.empty and got.attrs["period_unavailable"] == {"Niger": [NEW]}
    assert KEY not in got.attrs and not _coverage_warnings(caught)


def test_warnings_as_errors_are_not_caught_as_build_failures(reads):
    frames, _, _ = reads
    frames["Niger"] = _acquired(*_partition())
    for method, args in ((C("Niger").food_expenditures, ()),
                         (F("food_expenditures"), (["Niger"],))):
        with warnings.catch_warnings():
            warnings.simplefilter("error", FoodExpenditureCoverageWarning)
            with pytest.raises(FoodExpenditureCoverageWarning):
                method(*args, basis="total")
    out, caught = _invoke(C("Niger").food_expenditures, basis="total")
    assert _cell(out)["result"] == "served"
    assert len(_coverage_warnings(caught)) == 1


@pytest.mark.parametrize("kwargs", [
    {"labels": "Group", "market": "Region", "currency": "column"},
    {"labels": "Group", "market": "Region", "numeraire": "PPP-2017", "period": "week"},
])
def test_real_currency_market_period_relabel_poststeps_are_unchanged(reads, monkeypatch, kwargs):
    frames, _, _ = reads
    frames["Niger"] = _acquired(
        *_partition(), _row(NEW, "purchased", 1, 17, item="fixture-gift"),
        _row(NEW, "purchased", 1, 5, item="fixture-unlabelled"),
    )
    monkeypatch.setattr(FX, "_interview_dates", lambda country: {})
    monkeypatch.setattr(FX, "_load_factors", lambda: pd.DataFrame({
        "Country": ["Niger"], "_date": [pd.Timestamp("2018-01-01")],
        "PPP-2017": [2.0],
    }))
    got, w = _assert_old_contract(C("Niger").food_expenditures, **kwargs, basis="total")
    assert set(got.index.get_level_values("j")) == {"fixture-group", "fixture-unlabelled"}
    assert set(got.index.get_level_values("m")) == {"fixture-region"}
    assert got.attrs["labels_unmatched"]
    assert len(_coverage_warnings(w)) == 1
    assert _cell(got)["roles"]["inkind"]["remaining_unvalued"] == 1
    if "numeraire" in kwargs:
        assert got["Expenditure"].sum() == 41.0
        assert got.attrs["conversion"]["to"] == "PPP-2017"
