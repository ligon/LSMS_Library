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
    (catch, re-call, warning, marker, carrier) is exercised end to end."""

    WAVES = {"Alpha": ["w7", "w30"], "Beta": ["w7", "wvar"], "Gamma": ["wnr"]}
    calls: list = []

    def __init__(self, name):
        self.name = name

    def food_expenditures(self, waves=None, currency=None, period=None):
        type(self).calls.append((self.name, waves, period))
        present = [w for w in self.WAVES[self.name]
                   if waves is None or w in waves]
        df = frame(present, values=[210.0] * len(present))
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
    monkeypatch.setattr(ll, "Country", lambda name, **kw: _PeriodCountry(name))

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

    def test_the_re_call_asks_for_exactly_the_convertible_waves(self, feature):
        _call(feature(), ["Beta"], period="week")
        assert _PeriodCountry.calls == [("Beta", None, "week"),
                                        ("Beta", ["w7"], "week")]

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
