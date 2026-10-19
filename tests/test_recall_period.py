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
* ``TestDelivered`` -- the same on real built tables; skipped when cold.
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
from lsms_library.errors import PeriodUnavailableError

F = sys.modules["lsms_library.feature"]
R = sys.modules["lsms_library.recall"]

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
        [(w, f"h{k}", "maize", "purchased") for k, w in enumerate(waves)],
        names=["t", "i", "j", "s"])
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
            "ask_count_basis": "measured"}
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
# Nigeria's REAL records are used on purpose: 2010Q3/2011Q1 are 7-day
# config-comment cells (convert), 2018Q3 varies by acquisition source
# (`within_wave_variation='by-s'`, refused).  _aggregate_wave_data is
# monkeypatched to serve a synthetic food_acquired, filtered by `waves=`
# the way the real one is.

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
    import lsms_library as ll
    from lsms_library.country import Country

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
        c = ll.Country("Nigeria")
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

    def food_expenditures(self, waves=None, currency=None, period=None):
        type(self).calls.append((self.name, waves, period))
        present = [w for w in self.WAVES[self.name]
                   if waves is None or w in waves]
        value = 210.0 * (len(present) if self.POOLED else 1)
        df = frame(present, values=[value] * len(present))
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

    def test_no_period_no_marker_no_record(self, feature):
        out, msgs = _call(feature(), ["Alpha", "Beta", "Gamma"])
        assert "period_unavailable" not in out.attrs
        assert "period" not in out.attrs
        assert not [m for m in msgs if "period=" in m]

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


# ---------------------------------------------------------------------------
# on real built tables
# ---------------------------------------------------------------------------

@pytest.mark.requires_s3
@pytest.mark.slow
class TestDelivered:
    """End-to-end on the warm corpus.  Skipped when the cache is cold."""

    def _get(self, country, table, **kw):
        import lsms_library as ll
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                return getattr(ll.Country(country), table)(**kw)
        except PeriodUnavailableError:
            raise
        except Exception as e:                    # pragma: no cover
            pytest.skip(f"{country} {table} not buildable here: {e}")

    def test_ethiopia_day_is_the_recorded_week_over_seven(self):
        base = self._get("Ethiopia", "food_expenditures")
        out = self._get("Ethiopia", "food_expenditures", period="day")
        assert out.index.equals(base.index)
        np.testing.assert_allclose(out["Expenditure"].to_numpy(),
                                   base["Expenditure"].to_numpy() / 7,
                                   rtol=1e-12)
        assert set(out.attrs["period"]["Ethiopia"]["waves"]) == set(
            base.index.get_level_values("t").astype(str))

    def test_ghanalss_glss7_month_is_thirty_days_rescaled(self):
        kw = dict(waves=["2016-17"], units="units")
        base = self._get("GhanaLSS", "food_quantities", **kw)
        out = self._get("GhanaLSS", "food_quantities", period="month", **kw)
        np.testing.assert_allclose(out["Quantity"].to_numpy(),
                                   base["Quantity"].to_numpy()
                                   * (365.25 / 12) / 30, rtol=1e-12)

    def test_ghanalss_glss3_is_refused_and_the_rest_named(self):
        with pytest.raises(PeriodUnavailableError) as info:
            self._get("GhanaLSS", "food_expenditures", period="year",
                      waves=["1991-92", "2016-17"])
        assert list(info.value.unavailable) == ["1991-92"]
        assert info.value.convertible == ["2016-17"]

    def test_burkina_2014_quantity_is_the_passage_1_week(self):
        """Red team r1, R1.  Only passage 1 of four fields the quantity grid
        (Burkina_Faso/_/CONTENTS.org), so the served quantity IS one 7-day
        ask: a week of it is the passage-1 sum of food_acquired, x 7/7.  It
        was served x 7/28 = 0.25."""
        fa = self._get("Burkina_Faso", "food_acquired", waves=["2014"])
        out = self._get("Burkina_Faso", "food_quantities", waves=["2014"],
                        units="units", period="week")
        p1 = fa[fa.index.get_level_values("visit") == 1]["Quantity"]
        want = p1.groupby(level="i").sum(min_count=1).dropna() * (7 / 7)
        got = out["Quantity"].groupby(level="i").sum(min_count=1).dropna()
        assert got.index.sort_values().equals(want.index.sort_values())
        np.testing.assert_allclose(got.sort_index().to_numpy(),
                                   want.sort_index().to_numpy(), rtol=1e-12)
        rec = out.attrs["period"]["Burkina_Faso"]["waves"]["2014"]
        assert (rec["exposure_days"], rec["n_asks"], rec["n_asks_source"]) == (
            7.0, 1, "n_asks_by_column")

    def test_burkina_2014_expenditure_is_still_28_days(self):
        """...while spending is asked at all four passages: unchanged from
        Phase II's first cut, x 7/28."""
        base = self._get("Burkina_Faso", "food_expenditures", waves=["2014"])
        out = self._get("Burkina_Faso", "food_expenditures", waves=["2014"],
                        period="week")
        assert out.index.equals(base.index)
        np.testing.assert_array_equal(out["Expenditure"].to_numpy(),
                                      base["Expenditure"].to_numpy() * 0.25)
        assert out.attrs["period"]["Burkina_Faso"]["waves"]["2014"][
            "exposure_days"] == 28.0

    @pytest.mark.parametrize("table,kw,col", [
        ("food_expenditures", {}, "Expenditure"),
        ("food_quantities", {"units": "units"}, "Quantity"),
    ])
    def test_mali_2014_15_week_is_half_the_served_two_passage_total(
            self, table, kw, col):
        """Red team r1, R2.  The served 2014-15 row is passage 1 + passage 2
        of one panel, so its exposure is recall_days x n_asks = 7 x 2 = 14
        days, and period='week' multiplies by 7 / 14 = 0.5.  It was served
        x 7 / (7 x 1) = 1."""
        base = self._get("Mali", table, waves=["2014-15"], **kw)
        out = self._get("Mali", table, waves=["2014-15"], period="week", **kw)
        assert out.index.equals(base.index)
        np.testing.assert_array_equal(out[col].to_numpy(),
                                      base[col].to_numpy() * (7 / (7 * 2)))
        rec = out.attrs["period"]["Mali"]["waves"]["2014-15"]
        assert (rec["recall_days"], rec["n_asks"], rec["exposure_days"],
                rec["factor"], rec["ask_count_basis"]) == (
            7.0, 2, 14.0, 0.5, "measured")


@pytest.mark.requires_s3
@pytest.mark.slow
class TestDeliveredFeature:
    """Red team r1, R3, on the warm corpus: for a DEGRADED country, the rows
    Feature keeps under period= are the unconverted Feature rows for those
    waves times each wave's factor -- EXACTLY (bit-for-bit), including under
    food_quantities' default units='kgs', whose kg inference pools over the
    waves read (the reason a waves= re-call moved them: Mali +2.90% kg).

    Exact because both sides are the same unconverted read multiplied by the
    same float; no argument tried here breaks linearity (labels= re-sums
    within t, market= re-keys keeping t, the kg inference runs before)."""

    @pytest.mark.parametrize("country,table,kw", [
        ("Mali", "food_quantities", {}),
        ("Uganda", "food_quantities",
         {"units": "kgs", "labels": "Aggregate", "market": "Region"}),
    ])
    def test_kept_rows_are_the_unconverted_rows_times_the_factor(
            self, country, table, kw):
        import lsms_library as ll
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                base = ll.Feature(table)([country], **kw)
                out = ll.Feature(table)([country], period="week", **kw)
        except Exception as e:                    # pragma: no cover
            pytest.skip(f"{country} {table} not buildable here: {e}")
        if base.empty:                            # pragma: no cover
            pytest.skip(f"{country} {table} empty here")
        unavailable = out.attrs["period_unavailable"][country]
        assert unavailable, "this test needs a degraded country"
        waves = out.attrs["period"][country]["waves"]
        t = base.index.get_level_values("t").astype(str)
        kept = base[t.isin(list(waves))]
        assert out.index.equals(kept.index)
        factor = (out.index.get_level_values("t").astype(str)
                  .map(lambda w: waves[w]["factor"]).to_numpy(dtype=float))
        col = R.PERIOD_TABLES[table][0]
        np.testing.assert_array_equal(out[col].to_numpy(),
                                      kept[col].to_numpy() * factor)
        for other in [c for c in out.columns if c != col]:
            pd.testing.assert_series_equal(out[other], kept[other])
        assert not set(unavailable) & set(out.index.get_level_values("t")
                                          .astype(str))


# ---------------------------------------------------------------------------
# delivered exposure: does each convertible record count the asks its sources
# actually deliver into the served column?  (red team r1, verdict §5.4)
# ---------------------------------------------------------------------------
#
# period= divides by the record's exposure, so a record that miscounts the
# asks feeding a column serves a wrong number with full confidence.  Both r1
# refutations were exactly that: Burkina Faso 2014's quantity grid is in
# passage 1 of 4 (R1), and Mali 2014-15 sums two passages of one panel (R2).
# Neither is visible in the record alone; both are visible in the sources.
#
# Every convertible cell is put in ONE delivery class, and each class has a
# check against data:
#
# * `visit-level` -- the country declares `visit` in food_acquired's index,
#   so the delivered table shows the asks: per FLOW column, the number of
#   visits carrying a value must equal `n_asks_for(column)`.  GENERIC.
# * `declared-visit` -- a YAML wave whose food_acquired names a `visit`
#   idxvar or lists several source files, but whose served table has no
#   `visit` level (a hook dropped it).  The asks are counted in the SOURCES:
#   the most distinct visits any household has.  GENERIC for the YAML path;
#   config tier: several files must mean that many asks.
# * `one-ask-by-inspection` -- a script-path wave with no visit level.  No
#   generic signal exists here (the script, not config, decides what it
#   reads), so these are TABLE-DRIVEN: listed by hand, each with the source
#   it reads, and pinned to n_asks == 1.
#
# A cell in no class fails, so a record promoted from `not-recorded` cannot
# convert until someone has said how its asks are delivered.

#: Script-path cells whose food_acquired reads ONE food-consumption source
#: per `t`, asked once.  Swept by red team r1 (2026-10-19) and re-read for
#: this table; the source named is what the wave script reads.
ONE_ASK_BY_INSPECTION = {
    **{("Ethiopia", w): f"Ethiopia/{w}/_/food_acquired.py reads one section "
                        f"5A/6A file"
       for w in ("2011-12", "2013-14", "2015-16", "2018-19", "2021-22")},
    ("Guatemala", "2000"): "2000/_/food_acquired.py reads ECV13G12.DTA, "
                           "the 15-day variables only",
    **{("Nigeria", w): "post-planting and post-harvest are separate `t`, "
                       "each read from its own round file "
                       "(sect7b_planting / sect10b_harvest)"
       for w in ("2010Q3", "2011Q1", "2012Q3", "2013Q1", "2015Q3", "2016Q1")},
    ("Panama", "1997"): "1997/_/food_acquired.py reads GAST-A.DTA, 15-day vars",
    ("Panama", "2003"): "2003/_/food_acquired.py reads E03GA10B.DTA, 15-day vars",
    ("Panama", "2008"): "2008/_/food_acquired.py reads 05alimentos.dta, 15-day vars",
    ("Serbia", "2007"): "2007/_/food_acquired.py reads m5_1_diary.dta",
    **{("Tanzania", w): "2008-15/_/food_acquired.py reads upd4_hh_j1.dta; "
                        "its `round` column is the wave `t` "
                        "(wave_folder_map), one ask per round"
       for w in ("2008-09", "2010-11", "2012-13", "2014-15")},
    ("Tanzania", "2019-20"): "2019-20/_/food_acquired.py reads HH_SEC_J1.dta",
    ("Tanzania", "2020-21"): "2020-21/_/food_acquired.py reads hh_sec_j1.dta",
    ("Uganda", "2019-20"): "2019-20/_/food_acquired.py reads HH/gsec15b.dta",
}


def _convertible_cells():
    return [(c, w, r) for c, w, r in _all_cells()
            if R.unavailable_reason(r) is None]


def _declares_visit_level(country):
    from lsms_library.paths import countries_root
    from lsms_library.yaml_utils import load_yaml
    scheme = load_yaml(Path(countries_root()) / country / "_"
                       / "data_scheme.yml") or {}
    entry = (scheme.get("Data Scheme") or {}).get("food_acquired") or {}
    index = entry.get("index", "") if isinstance(entry, dict) else ""
    return "visit" in re.findall(r"\w+", str(index))


def _yaml_food_acquired(country, wave):
    """``(files, i_columns, visit_column)`` of a YAML-path food_acquired, or
    None where the wave builds it with a script."""
    from lsms_library.paths import countries_root
    from lsms_library.yaml_utils import load_yaml
    path = Path(countries_root()) / country / wave / "_" / "data_info.yml"
    if not path.exists():
        return None
    block = (load_yaml(path) or {}).get("food_acquired")
    if not isinstance(block, dict) or "file" not in block:
        return None
    raw = block["file"]
    files = []
    for f in (raw if isinstance(raw, list) else [raw]):
        files.extend(f if isinstance(f, dict) else [f])
    idx = block.get("idxvars") or {}
    i_cols = idx.get("i")
    i_cols = [x for x in (i_cols if isinstance(i_cols, list) else [i_cols])
              if isinstance(x, str)]
    visit = idx.get("visit")
    visit = visit if isinstance(visit, str) else None
    return [str(f) for f in files], i_cols, visit


def _delivery_class(country, wave):
    if _declares_visit_level(country):
        return "visit-level"
    if (country, wave) in ONE_ASK_BY_INSPECTION:
        return "one-ask-by-inspection"
    yaml = _yaml_food_acquired(country, wave)
    if yaml is not None and (yaml[2] is not None or len(yaml[0]) > 1):
        return "declared-visit"
    return None


class TestDeliveredExposureConfig:
    """Config tier -- runs in CI, no data."""

    def test_every_convertible_cell_has_a_delivery_class(self):
        missing = [(c, w) for c, w, _ in _convertible_cells()
                   if _delivery_class(c, w) is None]
        assert not missing, (
            f"{missing} convert under period= but nothing checks how their "
            f"asks are delivered.  Add each to ONE_ASK_BY_INSPECTION with the "
            f"source its script reads (if it is asked once), or give the "
            f"country a `visit` level -- see this section's comment.")

    def test_the_hand_list_is_live_and_says_one_ask(self):
        live = {(c, w): r for c, w, r in _convertible_cells()}
        stale = sorted(set(ONE_ASK_BY_INSPECTION) - set(live))
        assert not stale, f"ONE_ASK_BY_INSPECTION lists non-convertible {stale}"
        for cell in ONE_ASK_BY_INSPECTION:
            r = live[cell]
            assert not _declares_visit_level(cell[0]), cell
            assert r.n_asks == 1 and not r.n_asks_by_column, (
                f"{cell} is listed as one ask by inspection but its record "
                f"says n_asks={r.n_asks}, by column {r.n_asks_by_column}")

    def test_several_source_files_mean_that_many_asks(self):
        """R2's shape, caught without data: Mali 2014-15 lists EACIALI_p1 and
        EACIALI_p2, so the record must count two asks."""
        checked = []
        for c, w, r in _convertible_cells():
            if _delivery_class(c, w) != "declared-visit":
                continue
            files, _, _ = _yaml_food_acquired(c, w)
            if len(files) > 1:
                checked.append((c, w))
                assert {r.n_asks_for(col) for col in R.FLOW_COLUMNS} == {
                    len(files)}, (c, w, files, r.n_asks, r.n_asks_by_column)
        assert ("Mali", "2014-15") in checked

    def test_the_classes_are_the_ones_measured(self):
        """Pin today's partition so a change to it is a reviewed diff."""
        got = {}
        for c, w, _ in _convertible_cells():
            got.setdefault(_delivery_class(c, w), set()).add((c, w))
        assert got["declared-visit"] == {("Mali", "2014-15"), ("Togo", "2018")}
        assert {c for c, _ in got["visit-level"]} == {"Burkina_Faso", "GhanaLSS"}
        assert len(got["one-ask-by-inspection"]) == 23


def _source_visit_counts(country, wave, monkeypatch):
    """Per household: how many distinct visits the declared sources carry
    for it.  A file without a `visit` column counts as its own visit."""
    from lsms_library.local_tools import get_dataframe
    from lsms_library.paths import countries_root
    files, i_cols, visit = _yaml_food_acquired(country, wave)
    monkeypatch.chdir(Path(countries_root()) / country / wave / "_")
    parts = []
    for k, f in enumerate(files):
        df = get_dataframe(str(Path("..") / "Data" / f))
        cols = {c: df[c].astype(str) for c in i_cols}
        cols["_visit"] = (df[visit].astype(str) if visit and visit in df
                          else pd.Series(str(k), index=df.index))
        parts.append(pd.DataFrame(cols).drop_duplicates())
    both = pd.concat(parts, ignore_index=True).drop_duplicates()
    return both.groupby(i_cols)["_visit"].nunique()


@pytest.mark.requires_s3
@pytest.mark.slow
class TestDeliveredExposure:
    """Data tier: the record's per-column ask count against the delivered
    table (visit-level) or the declared sources (declared-visit).  These
    are the two tests that fail on the r1 records: Burkina Faso 2014
    Quantity (record 4, delivered 1) and Mali 2014-15 (record 1, sources 2).
    Skipped when the cache is cold."""

    @pytest.mark.parametrize("country,wave", sorted(
        (c, w) for c, w, _ in _convertible_cells()
        if _delivery_class(c, w) == "visit-level"))
    def test_visits_carrying_each_column_equal_its_ask_count(self, country, wave):
        import lsms_library as ll
        try:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                fa = ll.Country(country).food_acquired(waves=[wave])
        except Exception as e:                    # pragma: no cover
            pytest.skip(f"{country} food_acquired not buildable here: {e}")
        rec = R.recall_records(country)[wave]
        visits = fa.index.get_level_values("visit")
        for col in R.FLOW_COLUMNS:
            if col not in fa.columns:
                continue
            v = pd.to_numeric(fa[col], errors="coerce")
            carrying = visits[(v.notna() & (v != 0)).to_numpy()].unique()
            assert len(carrying) == rec.n_asks_for(col), (
                f"{country} {wave} {col}: delivered by visits "
                f"{sorted(carrying)}, but the record counts "
                f"{rec.n_asks_for(col)} ask(s)")

    @pytest.mark.parametrize("country,wave", sorted(
        (c, w) for c, w, _ in _convertible_cells()
        if _delivery_class(c, w) == "declared-visit"))
    def test_the_sources_ask_each_household_n_asks_times(
            self, country, wave, monkeypatch):
        rec = R.recall_records(country)[wave]
        assert not rec.n_asks_by_column, (
            "a per-column count on a declared-visit cell cannot be checked "
            "from the sources generically -- extend this test")
        try:
            per_household = _source_visit_counts(country, wave, monkeypatch)
        except Exception as e:                    # pragma: no cover
            pytest.skip(f"{country} {wave} sources not readable here: {e}")
        assert per_household.max() == rec.n_asks, (
            f"{country} {wave}: households are asked up to "
            f"{per_household.max()} time(s) in the sources "
            f"({per_household.value_counts().to_dict()}), but the record "
            f"says n_asks={rec.n_asks}")
