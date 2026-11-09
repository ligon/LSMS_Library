"""GH #851 Phase II: ``period=`` on ``food_expenditures`` / ``food_quantities``.

Ruling (@ligon, 2026-10-18): ship ``period=`` under Contract B -- a typed raise
from ``Country()``, a degrade plus warning from ``Feature()`` -- and refuse it
on ``food_prices``.  Design and coverage: ``docs/guide/recall.md``.

One test (at least) per branch:

* the vocabulary (``PERIOD_DAYS``) and its refusals;
* :func:`recall.unavailable_reason` -- one reason per way a window can fail
  to be established, and ``config-comment`` DOES convert;
* :func:`recall.to_period` -- the arithmetic, the strict raise, the
  ``attrs['period']`` record (JSON-serialisable), commutation with a re-sum;
* the generated ``Country`` method -- conversion, the typed raise naming the
  country, ``waves=`` as the escape hatch, refusal on ``food_prices`` /
  ``food_acquired`` / a non-food table, no legacy fall-through under
  ``period=``, and ``period=None`` unchanged (``_aggregate_wave_data``
  monkeypatched; no data read);
* ``Feature`` -- refusal up front, the wave-grain degrade with ONE warning and
  ``attrs['period_unavailable']``, and the ``period`` carrier surviving the
  concat (fake ``Country``; no data read);
* coverage -- every recorded cell is classified, and the count the guide
  states is the count the records give;
* All fixtures below are invented; the historical real-source tiers are not ported.
"""
from __future__ import annotations

import json
import re
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import lsms_library.feature  # noqa: F401  (bind the submodule, not ll.recall)
import lsms_library.recall  # noqa: F401
import lsms_library.sampling  # noqa: F401
from lsms_library.errors import PeriodUnavailableError

F = sys.modules["lsms_library.feature"]
R = sys.modules["lsms_library.recall"]
S = sys.modules["lsms_library.sampling"]

REPO = Path(__file__).resolve().parent.parent


def rec(wave, **kw):
    return R.RecallRecord(country="Someland", wave=wave, **kw)


#: One record per branch of `unavailable_reason`.
RECORDS = {
    "w7": rec("w7", recall_days=7, n_asks=1, basis="questionnaire",
              ask_count_basis="declared"),
    "w30": rec("w30", recall_days=5, n_asks=6, basis="config-comment",
               ask_count_basis="measured"),
    "wnr": rec("wnr"),                                   # not-recorded
    "wvar": rec("wvar", basis="questionnaire", within_wave_variation="stratum"),
    "wfile": rec("wfile", basis="filename", recall_days=7, n_asks=1),
    "wblank": rec("wblank", basis="questionnaire", recall_days=7),
}


def frame(waves, col="Expenditure", values=None):
    idx = pd.MultiIndex.from_tuples(
        [(w, "C1", f"h{k}", "maize", "purchased") for k, w in enumerate(waves)],
        names=["t", "v", "i", "j", "s"])
    values = values if values is not None else [70.0 * (k + 1)
                                                for k in range(len(waves))]
    return pd.DataFrame({col: values}, index=idx)


# ---------------------------------------------------------------------------
# vocabulary
# ---------------------------------------------------------------------------

class TestVocabulary:

    def test_the_accepted_values_and_their_lengths(self):
        assert R.PERIOD_DAYS == {"day": 1.0, "week": 7.0,
                                 "month": 365.25 / 12, "year": 365.25}

    @pytest.mark.parametrize("bad", ["annual", "Day", "fortnight", 7, None])
    def test_anything_else_is_a_ValueError_listing_the_vocabulary(self, bad):
        with pytest.raises(ValueError, match="'day', 'week', 'month', 'year'"):
            R.period_days(bad)

    def test_only_the_two_flow_tables_take_it(self):
        assert R.PERIOD_TABLES == {"food_expenditures": ("Expenditure",),
                                   "food_quantities": ("Quantity",)}

    def test_food_prices_refusal_says_why(self):
        msg = R.period_refusal("food_prices")
        assert "unexpected keyword argument 'period'" in msg
        assert "not a flow" in msg

    def test_food_acquired_refusal_says_why(self):
        assert "visit" in R.period_refusal("food_acquired")


# ---------------------------------------------------------------------------
# which records establish a window
# ---------------------------------------------------------------------------

class TestUnavailableReason:

    @pytest.mark.parametrize("wave", ["w7", "w30"])
    def test_an_established_window_converts(self, wave):
        assert R.unavailable_reason(RECORDS[wave]) is None

    def test_config_comment_converts(self):
        """The ruling's count included the config-comment cells; the rung
        rides on attrs['period'] instead of blocking the conversion."""
        assert RECORDS["w30"].basis == "config-comment"
        assert R.unavailable_reason(RECORDS["w30"]) is None

    @pytest.mark.parametrize("wave,needle", [
        (None, "no recall record"),
        ("wnr", "not-recorded"),
        ("wvar", "varies within the wave"),
        ("wfile", "filename"),
        ("wblank", "n_asks not established"),
    ])
    def test_every_failure_has_its_own_reason(self, wave, needle):
        why = R.unavailable_reason(RECORDS.get(wave))
        assert why is not None and needle in why


# ---------------------------------------------------------------------------
# the conversion
# ---------------------------------------------------------------------------

class TestToPeriod:

    @pytest.mark.parametrize("period", list(R.PERIOD_DAYS))
    def test_value_times_period_over_exposure(self, period):
        df = frame(["w7", "w30"], values=[70.0, 300.0])
        out = R.to_period(df, "Someland", period, table="food_expenditures",
                          records=RECORDS)
        days = R.PERIOD_DAYS[period]
        np.testing.assert_allclose(out["Expenditure"].to_numpy(),
                                   [70.0 * days / 7, 300.0 * days / 30],
                                   rtol=1e-12)
        assert out.index.equals(df.index)

    def test_week_on_a_seven_day_wave_is_the_identity(self):
        df = frame(["w7"], values=[123.456])
        out = R.to_period(df, "Someland", "week", table="food_expenditures",
                          records=RECORDS)
        assert out["Expenditure"].iloc[0] == 123.456

    def test_food_quantities_scales_Quantity(self):
        df = frame(["w30"], col="Quantity", values=[60.0])
        out = R.to_period(df, "Someland", "day", table="food_quantities",
                          records=RECORDS)
        assert out["Quantity"].iloc[0] == pytest.approx(2.0)

    def test_only_the_flow_column_is_scaled(self):
        df = frame(["w30"], values=[60.0]).assign(currency="GHS", n=3)
        out = R.to_period(df, "Someland", "day", table="food_expenditures",
                          records=RECORDS)
        assert out["Expenditure"].iloc[0] == pytest.approx(2.0)
        assert out["currency"].iloc[0] == "GHS" and out["n"].iloc[0] == 3

    def test_the_input_is_not_mutated(self):
        df = frame(["w30"], values=[60.0])
        df.attrs["keep"] = 1
        R.to_period(df, "Someland", "day", table="food_expenditures",
                    records=RECORDS)
        assert df["Expenditure"].iloc[0] == 60.0
        assert df.attrs == {"keep": 1}

    def test_the_record_rides_on_attrs_and_is_json(self):
        """`df.to_parquet` serialises attrs with a bare json.dumps."""
        df = frame(["w7", "w30"])
        df.attrs["keep"] = 1
        out = R.to_period(df, "Someland", "year", table="food_expenditures",
                          records=RECORDS)
        assert out.attrs["keep"] == 1
        record = out.attrs["period"]["Someland"]
        assert record["period"] == "year" and record["period_days"] == 365.25
        assert record["columns"] == ["Expenditure"]
        assert record["waves"]["w30"] == {
            "exposure_days": 30.0, "factor": 365.25 / 30, "recall_days": 5.0,
            "n_asks": 6, "n_asks_source": "n_asks", "basis": "config-comment",
            "ask_count_basis": "measured", "shortfall": None}
        assert record["shortfall_source"] == "not-measured"
        json.dumps(out.attrs)

    def test_a_per_column_ask_count_sets_that_columns_exposure(self):
        """GH #851 r1, R1: Burkina Faso 2014 asks spending at four 7-day
        passages and quantity at one.  One per-wave exposure divided its
        quantity by 28 days instead of 7 -- served 4x too low."""
        recs = {"w": rec("w", recall_days=7, n_asks=4, basis="config-comment",
                         evidence="CONTENTS.org:844",
                         n_asks_by_column={"Quantity": 1})}
        q = R.to_period(frame(["w"], col="Quantity", values=[70.0]),
                        "Someland", "week", table="food_quantities",
                        records=recs)
        e = R.to_period(frame(["w"], values=[280.0]), "Someland", "week",
                        table="food_expenditures", records=recs)
        assert q["Quantity"].iloc[0] == 70.0           # 7 days -> a week: x1
        assert e["Expenditure"].iloc[0] == 70.0        # 28 days -> a week
        wq = q.attrs["period"]["Someland"]["waves"]["w"]
        we = e.attrs["period"]["Someland"]["waves"]["w"]
        assert (wq["exposure_days"], wq["n_asks"], wq["n_asks_source"]) == (
            7.0, 1, "n_asks_by_column")
        assert (we["exposure_days"], we["n_asks"], we["n_asks_source"]) == (
            28.0, 4, "n_asks")
        json.dumps(q.attrs)

    def test_the_ask_shortfall_is_disclosed_not_corrected(self):
        """Option A (@ligon 2026-10-20): the exposure stays one declared
        constant per wave; `attrs['period']` says how many households the
        served food_acquired's `visit` level shows asked fewer times, and
        the understatement that implies.  h1 carries all four passages
        (10 each), h2 only passage 1 (40): declared = 80/4 = 20, delivered
        = 40/4 + 40/1 = 50, understatement = -60%.  A zero is 'not
        acquired', not an ask delivered."""
        recs = {"w": rec("w", recall_days=7, n_asks=4, basis="config-comment",
                         evidence="x")}
        idx = pd.MultiIndex.from_tuples(
            [("w", "h1", "maize", "purchased", v) for v in (1, 2, 3, 4)]
            + [("w", "h2", "maize", "purchased", 1),
               ("w", "h2", "maize", "purchased", 2)],
            names=["t", "i", "j", "s", "visit"])
        fa = pd.DataFrame({"Expenditure": [10.0, 10.0, 10.0, 10.0, 40.0, 0.0]},
                          index=idx)
        per_wave, source = R.ask_shortfall(fa, "Expenditure", recs)
        assert source == "visit"
        assert per_wave == {"w": {
            "households": 2, "households_short": 1, "share_short": 0.5,
            "understatement": pytest.approx(20 / 50 - 1)}}
        out = R.to_period(frame(["w"], values=[80.0]), "Someland", "week",
                          table="food_expenditures", records=recs, visits=fa)
        assert out["Expenditure"].iloc[0] == 20.0          # still / 28 x 7
        record = out.attrs["period"]["Someland"]
        assert record["shortfall_source"] == "visit"
        assert record["waves"]["w"]["shortfall"]["households_short"] == 1
        json.dumps(out.attrs)
        # no visit level: nothing to measure, and it says so
        _, source = R.ask_shortfall(fa.droplevel("visit"), "Expenditure", recs)
        assert source == "no-visit-level"
        out = R.to_period(frame(["w"], values=[80.0]), "Someland", "week",
                          table="food_expenditures", records=recs,
                          visits=fa.droplevel("visit"))
        assert out.attrs["period"]["Someland"]["shortfall_source"] == (
            "no-visit-level")
        assert out.attrs["period"]["Someland"]["waves"]["w"]["shortfall"] is None

    def test_it_commutes_with_a_resum_over_items(self):
        """Linear and per-t: scaling then re-summing (labels= reaggregate)
        equals re-summing then scaling -- why Country applies it first."""
        df = pd.concat([frame(["w7", "w30"]),
                        frame(["w7", "w30"]).rename(index={"maize": "rice"})])
        a = R.to_period(df, "Someland", "month", table="food_expenditures",
                        records=RECORDS).groupby(["t", "s"]).sum()
        b = R.to_period(df.groupby(["t", "s"]).sum(), "Someland", "month",
                        table="food_expenditures", records=RECORDS)
        pd.testing.assert_frame_equal(a, b, check_exact=False, rtol=1e-12)

    @pytest.mark.parametrize("bad", ["wnr", "wvar", "wfile", "wblank", "nope"])
    def test_one_unknown_wave_converts_nothing_and_names_everything(self, bad):
        df = frame(["w7", bad, "w30"])
        with pytest.raises(PeriodUnavailableError) as info:
            R.to_period(df, "Someland", "day", table="food_expenditures",
                        records=RECORDS)
        e = info.value
        assert isinstance(e, ValueError)
        assert (e.country, e.period, e.table) == ("Someland", "day",
                                                  "food_expenditures")
        assert list(e.unavailable) == [bad]
        assert e.convertible == ["w30", "w7"]
        assert "waves=['w30', 'w7']" in str(e)
        assert str(e).startswith("Someland: food_expenditures(period='day')")

    def test_no_wave_converts_says_so(self):
        with pytest.raises(PeriodUnavailableError) as info:
            R.to_period(frame(["wnr"]), "Someland", "day",
                        table="food_expenditures", records=RECORDS)
        assert info.value.convertible == []
        assert "No wave of this frame converts" in str(info.value)

    def test_a_null_t_is_unavailable_not_silently_dropped(self):
        df = frame(["w7", "w7"])
        df.index = df.index.set_levels(
            df.index.levels[0], level="t").set_codes([0, -1], level="t")
        with pytest.raises(PeriodUnavailableError) as info:
            R.to_period(df, "Someland", "day", table="food_expenditures",
                        records=RECORDS)
        assert "<NA>" in info.value.unavailable

    def test_a_categorical_t_looks_up_like_a_string(self):
        df = frame(["w7", "w30"])
        df.index = df.index.set_levels(
            pd.CategoricalIndex(df.index.levels[0]), level="t")
        out = R.to_period(df, "Someland", "day", table="food_expenditures",
                          records=RECORDS)
        np.testing.assert_allclose(out["Expenditure"], [10.0, 140.0 / 30])

    def test_a_refusing_table_is_a_TypeError(self):
        with pytest.raises(TypeError, match="not a flow"):
            R.to_period(frame(["w7"], col="Price"), "Someland", "day",
                        table="food_prices", records=RECORDS)

    def test_no_flow_column_is_a_ValueError(self):
        with pytest.raises(ValueError, match="flow columns"):
            R.to_period(frame(["w7"], col="Price"), "Someland", "day",
                        table="food_expenditures", records=RECORDS)

    def test_no_t_level_is_a_ValueError(self):
        df = frame(["w7"]).droplevel("t")
        with pytest.raises(ValueError, match="`t` index level"):
            R.to_period(df, "Someland", "day", table="food_expenditures",
                        records=RECORDS)

    def test_merge_period_attrs_unions_by_country(self):
        a = {"period": {"A": {"period": "day"}}}
        b = pd.DataFrame({"x": [1]})
        b.attrs = {"period": {"B": {"period": "day"}}}
        assert set(R.merge_period_attrs([a, b, {}])) == {"A", "B"}


# ---------------------------------------------------------------------------
# the generated Country method (no data read)
# ---------------------------------------------------------------------------
#
# Invented records use familiar logical wave labels: two known seven-day
# windows and one by-source-varying window. No real records or constructor are
# read. Actual generated Country dispatch/transform functions receive finite
# payload and metadata leaves; the finalizer's cluster lookup is an invented
# constant, so these tests do not independently validate finalization.

def _synthetic_food_acquired(waves):
    rows = [(t, "C1", f"{t}-h{k}", "Maize", "kg", "purchased")
            for t in waves for k in range(2)]
    idx = pd.MultiIndex.from_tuples(rows,
                                    names=["t", "v", "i", "j", "u", "s"])
    n = len(rows)
    return pd.DataFrame({"Quantity": np.arange(1.0, n + 1),
                         "Expenditure": 7.0 * np.arange(1.0, n + 1)},
                        index=idx)


_LEGACY_SENTINEL = pd.DataFrame(
    {"Expenditure": [1.0]},
    index=pd.MultiIndex.from_tuples([("2010Q3", "legacy")], names=["t", "i"]))


@pytest.fixture
def nigeria(monkeypatch):
    """``make(waves_present, drop=())`` -> a ``Country('Nigeria')`` whose
    food_acquired holds *waves_present* (minus columns *drop*)."""
    from lsms_library.country import Country
    from types import SimpleNamespace
    C = sys.modules['lsms_library.country']

    class SyntheticCountry(Country):
        @property
        def waves(self):
            return list(self._fixture_waves)

        @property
        def data_scheme(self):
            return {"food_acquired": {}, "household_roster": {}}

        def __getitem__(self, wave):
            if wave not in self.waves:
                raise KeyError(wave)
            return SimpleNamespace(data_scheme={"food_acquired": {}})

    records = {
        "2010Q3": R.RecallRecord(country="Nigeria", wave="2010Q3",
            recall_days=7, n_asks=1, basis="config-comment"),
        "2011Q1": R.RecallRecord(country="Nigeria", wave="2011Q1",
            recall_days=7, n_asks=1, basis="config-comment"),
        "2018Q3": R.RecallRecord(country="Nigeria", wave="2018Q3",
            basis="questionnaire", within_wave_variation="by-s"),
    }
    monkeypatch.setattr(R, "recall_records", lambda name: records)
    monkeypatch.setattr(C, "recall_records", lambda name: records)
    def finalize(self, df, *args, **kwargs):
        # A finite invented cluster lookup: retain current required-v behavior
        # without a sample build or a real Country constructor/finalizer.
        out = df.copy(deep=True)
        if "v" not in out.index.names:
            names = list(out.index.names)
            out = out.assign(v="C1").set_index("v", append=True)
            out = out.reorder_levels(["t", "v", *[n for n in names if n != "t"]])
        return out

    monkeypatch.setattr(SyntheticCountry, "_finalize_result", finalize)
    monkeypatch.setattr(SyntheticCountry, "_materialization_entry",
        lambda self, name: {})
    monkeypatch.setattr(SyntheticCountry, "_relabel_j",
        lambda self, df, *a, **kw: df)
    monkeypatch.setattr(SyntheticCountry, "_no_kg_factor_cells", lambda self: {})
    monkeypatch.setattr(SyntheticCountry, "_legacy_can_build", lambda *a: True)
    monkeypatch.setattr(SyntheticCountry, "_proven_absent", lambda *a: False)

    calls = []

    def make(waves_present, drop=()):
        fa = _synthetic_food_acquired(waves_present).drop(columns=list(drop))

        def fake_aggregate(self, waves, table, *args, **kwargs):
            calls.append(table)
            if table == "food_acquired":
                out = fa.copy()
                if waves is not None:
                    out = out[out.index.get_level_values("t").isin(waves)]
                return out
            if table in ("food_expenditures", "food_quantities"):
                return _LEGACY_SENTINEL.copy()
            return pd.DataFrame()

        monkeypatch.setattr(Country, "_aggregate_wave_data", fake_aggregate)
        monkeypatch.setattr(Country, "_unit_kg_factors", lambda self: {})
        c = object.__new__(SyntheticCountry)
        c.name = "Nigeria"
        c._fixture_waves = list(waves_present)
        assert "food_acquired" in c.data_scheme
        return c

    make.calls = calls
    return make


class TestCountry:

    def test_converts_each_wave_by_its_own_record(self, nigeria):
        c = nigeria(["2010Q3", "2011Q1"])
        base = c.food_expenditures()
        out = c.food_expenditures(period="day")
        assert out.index.equals(base.index)
        np.testing.assert_allclose(out["Expenditure"].to_numpy(),
                                   base["Expenditure"].to_numpy() / 7,
                                   rtol=1e-12)
        record = out.attrs["period"]["Nigeria"]
        assert set(record["waves"]) == {"2010Q3", "2011Q1"}
        assert record["waves"]["2010Q3"]["basis"] == "config-comment"

    def test_food_quantities_converts_too(self, nigeria):
        c = nigeria(["2010Q3"])
        base = c.food_quantities(units="units")
        out = c.food_quantities(units="units", period="year")
        np.testing.assert_allclose(out["Quantity"].to_numpy(),
                                   base["Quantity"].to_numpy() * 365.25 / 7,
                                   rtol=1e-12)

    def test_period_None_is_the_unconverted_table(self, nigeria):
        c = nigeria(["2010Q3", "2018Q3"])
        pd.testing.assert_frame_equal(c.food_expenditures(period=None),
                                      c.food_expenditures())
        assert "period" not in c.food_expenditures().attrs

    def test_an_unknown_wave_is_a_typed_raise_naming_the_country(self, nigeria):
        c = nigeria(["2010Q3", "2018Q3"])
        with pytest.raises(PeriodUnavailableError) as info:
            c.food_expenditures(period="week")
        e = info.value
        assert e.country == "Nigeria"
        assert list(e.unavailable) == ["2018Q3"]
        assert "by-s" in e.unavailable["2018Q3"]
        assert e.convertible == ["2010Q3"]

    def test_waves_is_the_escape_hatch_the_error_names(self, nigeria):
        c = nigeria(["2010Q3", "2018Q3"])
        with pytest.raises(PeriodUnavailableError) as info:
            c.food_expenditures(period="week")
        out = c.food_expenditures(period="week", waves=info.value.convertible)
        assert set(out.index.get_level_values("t")) == {"2010Q3"}

    @pytest.mark.parametrize("table,needle", [
        ("food_prices", "not a flow"),
        ("food_acquired", "visit"),
        ("household_roster", "only ['food_expenditures', 'food_quantities']"),
    ])
    def test_every_other_table_refuses_before_reading_anything(
            self, nigeria, table, needle):
        c = nigeria(["2010Q3"])
        nigeria.calls.clear()
        with pytest.raises(TypeError, match=re.escape(needle)):
            getattr(c, table)(period="day")
        assert nigeria.calls == [], "refused only after reading data"

    def test_a_bad_value_is_refused_before_reading_anything(self, nigeria):
        c = nigeria(["2010Q3"])
        nigeria.calls.clear()
        with pytest.raises(ValueError, match="period= must be one of"):
            c.food_expenditures(period="annual")
        assert nigeria.calls == []

    def test_no_legacy_fall_through_under_period(self, nigeria):
        """A bare call whose derivation fails falls through to the legacy
        aggregation (pre-#828 behaviour, kept).  Under period= it must not:
        the legacy path cannot honour it, and an unscaled frame would come
        back looking converted."""
        c = nigeria(["2010Q3"], drop=["Expenditure"])
        pd.testing.assert_frame_equal(c.food_expenditures(), _LEGACY_SENTINEL)
        with pytest.raises(ValueError, match="Expenditure"):
            c.food_expenditures(period="day")


class TestCurrentPort:
    """Current dispatch invariants over invented frames, with no constructor IO."""

    def test_unavailable_declared_wave_refuses_before_any_frame_read(self, nigeria):
        c = nigeria(["2010Q3", "2018Q3"])
        with pytest.raises(PeriodUnavailableError) as caught:
            c.food_expenditures(period="day")
        assert caught.value.convertible == ["2010Q3"]
        assert set(caught.value.unavailable) == {"2018Q3"}
        assert nigeria.calls == []

    def test_none_adds_no_metadata_or_provider_calls(self, nigeria, monkeypatch):
        c = nigeria(["2010Q3"])
        C = sys.modules["lsms_library.country"]

        def forbidden(*args, **kwargs):
            raise AssertionError("period=None must not preflight recall")

        monkeypatch.setattr(C, "recall_records", forbidden)
        baseline = c.food_expenditures()
        actual = c.food_expenditures(period=None)
        pd.testing.assert_frame_equal(actual, baseline, check_exact=True)
        assert actual.attrs == baseline.attrs
        assert nigeria.calls == ["food_acquired", "food_acquired"]
        assert "period" not in actual.attrs

    def test_actual_country_feature_degrade_builds_one_full_request(
            self, nigeria, monkeypatch):
        import lsms_library as ll
        c = nigeria(["2010Q3", "2018Q3"])
        monkeypatch.setattr(ll, "Country", lambda *args, **kwargs: c)
        feature = F.Feature("food_expenditures")
        object.__setattr__(feature, "_countries", ["Nigeria"])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            actual = feature(["Nigeria"], period="day", currency=None)
        assert set(actual.index.get_level_values("t")) == {"2010Q3"}
        assert nigeria.calls == ["food_acquired"]
        assert actual.attrs["period_unavailable"] == {"Nigeria": ["2018Q3"]}
        assert len([w for w in caught if "period=" in str(w.message)]) == 1

    def test_proven_absent_subset_stays_empty(self, nigeria, monkeypatch):
        c = nigeria(["2010Q3", "2018Q3"])
        monkeypatch.setattr(c, "_proven_absent", lambda *args: True)
        calls = []

        def empty(waves, table, **kwargs):
            calls.append((waves, table))
            return _synthetic_food_acquired([])

        monkeypatch.setattr(c, "_aggregate_wave_data", empty)
        actual = c.food_quantities(waves=["2018Q3"], units="kgs", period="day")
        assert actual.empty
        assert calls == [(["2018Q3"], "food_acquired")]
        assert actual.index.names == ["t", "v", "i", "j", "u", "s"]

    def test_named_build_failure_keeps_identity_under_period(
            self, nigeria, monkeypatch):
        class NamedFailure(ValueError):
            pass

        c = nigeria(["2010Q3"])
        failure = NamedFailure("source-specific contract")

        def broken(*args, **kwargs):
            raise failure

        monkeypatch.setattr(c, "_aggregate_wave_data", broken)
        with pytest.raises(NamedFailure) as caught:
            c.food_expenditures(period="day")
        assert caught.value is failure

    def test_uncertain_wave_membership_defers_to_represented_t(
            self, nigeria, monkeypatch):
        from types import SimpleNamespace

        c = nigeria(["2018Q3"])
        monkeypatch.setattr(type(c), "__getitem__",
                            lambda self, wave: SimpleNamespace(data_scheme={}))
        with pytest.raises(PeriodUnavailableError) as caught:
            c.food_expenditures(period="day")
        assert set(caught.value.unavailable) == {"2018Q3"}
        assert nigeria.calls == ["food_acquired"]

    def test_kg_subset_retains_the_current_all_wave_factor_read(
            self, nigeria, monkeypatch):
        c = nigeria(["2010Q3", "2011Q1"])
        calls = []
        original = c._aggregate_wave_data

        def observe(waves, table, **kwargs):
            calls.append((waves, table))
            return original(waves, table, **kwargs)

        monkeypatch.setattr(c, "_aggregate_wave_data", observe)
        all_waves = c.food_quantities(units="kgs")
        actual = c.food_quantities(waves=["2010Q3"], units="kgs", period="day")
        expected = all_waves.loc[
            all_waves.index.get_level_values("t") == "2010Q3"].copy()
        expected["Quantity"] = expected["Quantity"] / 7
        pd.testing.assert_frame_equal(actual, expected, check_exact=True)
        assert calls == [(None, "food_acquired"), (None, "food_acquired")]

    def test_collapsed_mali_uses_declared_exposure_without_visit_invention(self):
        original = frame(["2014-15"], values=[140.0])
        original.attrs["keep"] = {"recorded": True}
        actual = R.to_period(original, "Mali", "week", table="food_expenditures",
                             visits=original)
        pd.testing.assert_index_equal(actual.index, original.index, exact=True)
        assert "visit" not in actual.index.names
        assert actual["Expenditure"].tolist() == [70.0]
        assert original["Expenditure"].tolist() == [140.0]
        record = actual.attrs["period"]["Mali"]
        assert record["shortfall_source"] == "no-visit-level"
        assert record["waves"]["2014-15"]["shortfall"] is None
        assert record["waves"]["2014-15"]["ask_count_basis"] == "declared"
        assert record["waves"]["2014-15"]["n_asks"] == 2
        assert actual.attrs["keep"] == original.attrs["keep"]


# ---------------------------------------------------------------------------
# Feature (fake Country; no data read)
# ---------------------------------------------------------------------------

class _PeriodCountry:
    """A Country whose food_expenditures goes through the REAL to_period over
    hand-built records, and honours ``waves=`` -- so the Feature plumbing
    (catch, second read, slice, warning, marker, carrier) is exercised end
    to end.

    ``POOLED`` makes every value depend on the wave set REQUESTED, the way
    ``food_quantities(units='kgs')`` does through its pooled kg inference
    (GH #850) -- the property that made a ``waves=`` re-call move
    non-period numbers (red team r1, R3)."""

    WAVES = {"Alpha": ["w7", "w30"], "Beta": ["w7", "wvar"], "Gamma": ["wnr"]}
    POOLED = False
    calls: list = []

    def __init__(self, name):
        self.name = name

    @property
    def waves(self):                 # what Feature hands sampling.attach
        return list(self.WAVES[self.name])

    def food_expenditures(self, waves=None, currency=None, period=None):
        type(self).calls.append((self.name, waves, period))
        present = [w for w in self.WAVES[self.name]
                   if waves is None or w in waves]
        value = 210.0 * (len(present) if self.POOLED else 1)
        df = frame(present, values=[value] * len(present))
        # as Country._finalize_result does (GH #793); a no-op unless a test
        # monkeypatches sampling_records for this fake country
        S.attach(df, self.name, known_waves=self.waves)
        if period is not None:
            df = R.to_period(df, self.name, period, table="food_expenditures",
                             records=RECORDS)
        return df

    food_quantities = food_expenditures

    def food_prices(self, waves=None, currency=None, period=None):
        raise AssertionError("Feature must refuse before building anything")


@pytest.fixture
def feature(monkeypatch):
    import lsms_library as ll
    _PeriodCountry.calls = []
    monkeypatch.setattr(_PeriodCountry, "POOLED", False)
    monkeypatch.setattr(ll, "Country", lambda name, **kw: _PeriodCountry(name))
    # Feature converts the degraded slice itself, through the real
    # to_period, which looks records up by country name at call time.
    monkeypatch.setattr(R, "recall_records", lambda country: RECORDS)

    def make(table="food_expenditures"):
        f = F.Feature(table)
        object.__setattr__(f, "_countries", ["Alpha", "Beta", "Gamma"])
        return f
    return make


def _call(f, *args, **kwargs):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = f(*args, currency=None, **kwargs)
    return out, [str(w.message) for w in caught]


class TestFeature:

    def test_degrades_at_the_wave_grain_with_one_warning(self, feature):
        out, msgs = _call(feature(), ["Alpha", "Beta", "Gamma"], period="day")
        cells = set(zip(out.index.get_level_values("country"),
                        out.index.get_level_values("t")))
        # Beta KEPT, restricted to its convertible wave; Gamma dropped whole.
        assert cells == {("Alpha", "w7"), ("Alpha", "w30"), ("Beta", "w7")}
        assert out.attrs["period_unavailable"] == {"Beta": ["wvar"],
                                                   "Gamma": ["wnr"]}
        period_msgs = [m for m in msgs if "period=" in m]
        assert len(period_msgs) == 1, msgs
        assert "Beta" in period_msgs[0] and "Gamma" in period_msgs[0]
        assert not [m for m in msgs if m.startswith("Failed to load")], \
            "a period= refusal must not be reported as a build failure"

    def test_the_kept_rows_are_converted(self, feature):
        out, _ = _call(feature(), ["Alpha", "Beta"], period="day")
        got = out["Expenditure"].groupby(["country", "t"]).sum().to_dict()
        assert got == {("Alpha", "w30"): pytest.approx(7.0),
                       ("Alpha", "w7"): pytest.approx(30.0),
                       ("Beta", "w7"): pytest.approx(30.0)}

    def test_the_second_read_is_the_unconverted_table_not_a_wave_subset(
            self, feature):
        """Red team r1, R3: re-calling with ``waves=convertible`` moved
        non-period numbers wherever a value depends on the requested wave
        set.  The degrade now reads the table as an unconverted Feature
        would -- same waves, no period -- and slices."""
        _call(feature(), ["Beta"], period="week")
        assert _PeriodCountry.calls == [("Beta", None, "week"),
                                        ("Beta", None, None)]

    def test_kept_rows_are_the_unconverted_rows_times_the_factor(
            self, feature, monkeypatch):
        """Under a POOLED estimator (the kg-inference case) the old re-call
        served Beta's w7 at 210 x 1 / 7 = 30 per day; the unconverted
        Feature row is 210 x 2 = 420, so the right answer is 60."""
        monkeypatch.setattr(_PeriodCountry, "POOLED", True)
        base, _ = _call(feature(), ["Alpha", "Beta"])
        out, _ = _call(feature(), ["Alpha", "Beta"], period="day")
        kept = base.loc[out.index]
        factor = np.where(out.index.get_level_values("t") == "w7", 1 / 7, 1 / 30)
        np.testing.assert_array_equal(out["Expenditure"].to_numpy(),
                                      kept["Expenditure"].to_numpy() * factor)
        got = out["Expenditure"].groupby(["country", "t"]).sum()
        assert got[("Beta", "w7")] == 420.0 / 7

    def test_the_warning_says_which_countries_were_dropped_whole(self, feature):
        _, msgs = _call(feature(), ["Alpha", "Beta", "Gamma"], period="day")
        (msg,) = [m for m in msgs if "period=" in m]
        assert "['Beta'] kept their other waves" in msg
        assert "['Gamma'] had no convertible wave and were dropped whole" in msg

    def test_the_slice_carries_records_for_the_rows_served(self, feature):
        out, _ = _call(feature(), ["Beta"], period="day")
        assert set(out.attrs["recall"]["Beta"]) == {"w7"}

    def test_the_slice_carries_the_sampling_record_for_the_rows_served(
            self, feature, monkeypatch):
        """GH #793's per-wave carrier landed after the slice degrade was
        written (red team r2 §4, the semantic half of the merge): like
        population and recall it must be re-attached for the SLICE, or a
        degraded country keeps describing every wave it read."""
        monkeypatch.setattr(S, "sampling_records", lambda country: {
            w: S.SamplingRecord.unrecorded(country, w)
            for w in _PeriodCountry.WAVES.get(country, [])})
        base, _ = _call(feature(), ["Beta"])
        assert set(base.attrs[S.ATTRS_KEY]["Beta"]) == {"w7", "wvar"}
        out, _ = _call(feature(), ["Beta"], period="day")
        assert set(out.attrs[S.ATTRS_KEY]["Beta"]) == {"w7"}, \
            "the sampling record must describe the rows served"
        assert out.attrs[S.ATTRS_RESOLUTION_KEY]["Beta"] == "exact"

    def test_the_period_record_survives_the_concat(self, feature):
        out, _ = _call(feature(), ["Alpha", "Beta"], period="month")
        record = out.attrs["period"]
        assert set(record) == {"Alpha", "Beta"}
        assert set(record["Beta"]["waves"]) == {"w7"}, \
            "the record must describe the rows served, not the rows refused"
        json.dumps(out.attrs)

    def test_every_country_dropped_returns_empty_with_the_marker(self, feature):
        out, _ = _call(feature(), ["Gamma"], period="day")
        assert out.empty
        assert out.attrs["period_unavailable"] == {"Gamma": ["wnr"]}
        assert list(out.index.names) == [
            "country", *F._canonical_index_levels("food_expenditures")]
        assert out.groupby("country").size().empty

    def test_no_period_no_marker_no_record(self, feature):
        out, msgs = _call(feature(), ["Alpha", "Beta", "Gamma"])
        assert "period_unavailable" not in out.attrs
        assert "period" not in out.attrs
        assert not [m for m in msgs if "period=" in m]

    def test_explicit_none_matches_the_default_frame_and_attrs(self, feature):
        f = feature()
        base, base_messages = _call(f, ["Alpha", "Beta"])
        actual, actual_messages = _call(f, ["Alpha", "Beta"], period=None)
        pd.testing.assert_frame_equal(actual, base, check_exact=True)
        assert actual.attrs == base.attrs
        assert actual_messages == base_messages

    def test_food_prices_is_refused_up_front(self, feature):
        with pytest.raises(TypeError, match="not a flow"):
            feature("food_prices")(["Alpha"], period="day")
        assert _PeriodCountry.calls == []

    def test_a_bad_value_is_refused_up_front(self, feature):
        with pytest.raises(ValueError, match="period= must be one of"):
            feature()(["Alpha"], period="annual")
        assert _PeriodCountry.calls == []


# ---------------------------------------------------------------------------
# coverage: what converts today, from the live records
# ---------------------------------------------------------------------------

def _all_cells():
    from lsms_library.paths import countries_root
    for path in sorted(Path(countries_root()).glob("*/_/recall.yml")):
        country = path.parent.parent.name
        for wave, record in sorted(R.recall_records(country).items()):
            yield country, wave, record


class TestCoverage:

    def test_every_recorded_cell_is_classified(self):
        cells = list(_all_cells())
        assert cells
        for c, w, r in cells:
            why = R.unavailable_reason(r)
            assert (why is None) == (r.is_strong
                                     and r.total_exposure_days is not None
                                     and not r.within_wave_variation), (c, w)

    def test_the_within_wave_cells_are_refused_for_that_reason(self):
        varying = {(c, w) for c, w, r in _all_cells()
                   if r.within_wave_variation}
        assert {("GhanaLSS", "1991-92"), ("Nigeria", "2018Q3"),
                ("Nigeria", "2019Q1")} <= varying
        for c, w, r in _all_cells():
            if (c, w) in varying:
                assert "varies within the wave" in R.unavailable_reason(r)

    def test_no_not_recorded_cell_converts(self):
        for c, w, r in _all_cells():
            if r.basis == "not-recorded":
                assert R.unavailable_reason(r) is not None, (c, w)

    def test_the_guide_states_the_live_count(self):
        """`docs/guide/recall.md` states how many cells convert.  A promoted
        (or demoted) record must update it in the same change -- the count
        drifting from 28 (the issue) to 32 (2026-10-19) unnoticed is why."""
        guide_path = REPO / "docs" / "guide" / "recall.md"
        if not guide_path.exists():
            pytest.skip("docs/ is source-checkout only")
        cells = list(_all_cells())
        n = sum(R.unavailable_reason(r) is None for _, _, r in cells)
        guide = guide_path.read_text()
        assert f"**{n} of {len(cells)}**" in guide, (
            f"docs/guide/recall.md does not state the live coverage "
            f"'**{n} of {len(cells)}**' -- update its coverage section")
