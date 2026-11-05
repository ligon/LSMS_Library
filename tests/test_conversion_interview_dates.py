"""The real ``conversion._interview_dates``, with no microdata (GH #1585).

``tests/test_conversion.py`` patches the lookup to ``{}`` for every test, so no
test ran it.  That is how ``getattr(r, 'Interview start')`` on ``itertuples()``,
which raises ``AttributeError`` because ``itertuples`` renames a column that is
not an identifier, kept seven countries on nominal dates behind a warning saying
the table was unavailable.

Mutant these tests must kill: restore
``pd.Timestamp(getattr(r, datecol)) for r in flat.itertuples()`` and every test
of an ``Interview start`` frame fails.  ``test_int_t_frame`` still passes under
that mutant, because ``Int_t`` is an identifier; it pins the other 20 countries.

A second mutant: drop the sort on ``visit``, so the first row served wins.
``test_visit_is_chosen_by_rule_not_row_order`` fails under it.
"""
import warnings

import pandas as pd
import pytest

import lsms_library
from lsms_library.conversion import _interview_dates, _load_factors, convert


@pytest.fixture(autouse=True)
def _fresh_lookup():
    """The lookup is lru_cached on the country name; a stale ``{}`` would leak."""
    _interview_dates.cache_clear()
    yield
    _interview_dates.cache_clear()


def _serve(monkeypatch, frame):
    """Make ``lsms_library.Country`` serve *frame* as its ``interview_date``."""
    class FakeCountry:
        def __init__(self, *args, **kwargs):
            self.data_scheme = ["interview_date"]

        def interview_date(self):
            return frame.copy()

    monkeypatch.setattr(lsms_library, "Country", FakeCountry)


def _ehcvm_frame(rows):
    """``(i, t, v, visit)`` with start and end, as ``melt_visit_intervals`` serves it."""
    frame = pd.DataFrame(rows, columns=["i", "t", "v", "visit", "Interview start"])
    frame["Interview start"] = pd.to_datetime(frame["Interview start"]).astype("datetime64[us]")
    frame["Interview end"] = frame["Interview start"] + pd.Timedelta(hours=1)
    return frame.set_index(["i", "t", "v", "visit"])


def _factor(country, year, target):
    factors = _load_factors()
    return float(factors.loc[(factors["Country"] == country)
                             & (factors["_date"] == pd.Timestamp(year, 1, 1)), target].iloc[0])


def test_interview_start_frame(monkeypatch):
    """One date per (i, t), the first visit's, with no 'unavailable' warning."""
    _serve(monkeypatch, _ehcvm_frame([
        ("h1", "2018-19", "c1", 1, "2018-11-05 08:40:01"),
        ("h1", "2018-19", "c1", 2, "2018-11-06 09:00:00"),
        ("h1", "2018-19", "c1", 3, "2018-11-07 09:00:00"),
        ("h2", "2018-19", "c1", 1, "2019-05-02 10:15:00"),
        ("h2", "2018-19", "c1", 2, "2019-05-03 10:15:00"),
        ("h1", "2021-22", "c1", 1, "2022-06-01 07:00:00"),
    ]))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        got = _interview_dates("Fake")
    assert got == {("h1", "2018-19"): pd.Timestamp("2018-11-05 08:40:01"),
                   ("h2", "2018-19"): pd.Timestamp("2019-05-02 10:15:00"),
                   ("h1", "2021-22"): pd.Timestamp("2022-06-01 07:00:00")}
    assert all(isinstance(d, pd.Timestamp) for d in got.values())


def test_a_visit_with_no_start_is_not_the_date(monkeypatch):
    """Visit 1 has an end but no start, so the household takes visit 2's start."""
    frame = _ehcvm_frame([("h1", "2018", "c1", 1, "2018-11-05 08:00:00"),
                          ("h1", "2018", "c1", 2, "2018-11-06 09:00:00")])
    frame.iloc[0, frame.columns.get_loc("Interview start")] = pd.NaT
    _serve(monkeypatch, frame)
    assert _interview_dates("Fake") == {("h1", "2018"): pd.Timestamp("2018-11-06 09:00:00")}


def test_visit_is_chosen_by_rule_not_row_order(monkeypatch):
    """Rows served out of visit order still give the lowest dated visit.

    The rows are permuted deterministically so a later visit always comes
    first.  h2's visit 1 has no start, so it takes visit 2, not visit 3.
    """
    frame = _ehcvm_frame([
        ("h1", "2021-22", "c1", 3, "2022-01-12 09:00:00"),
        ("h2", "2021-22", "c1", 3, "2022-07-04 08:00:00"),
        ("h1", "2021-22", "c1", 2, "2021-12-30 09:00:00"),
        ("h2", "2021-22", "c1", 1, "2022-07-01 08:00:00"),
        ("h1", "2021-22", "c1", 1, "2021-12-29 09:00:00"),
        ("h2", "2021-22", "c1", 2, "2022-07-02 08:00:00"),
    ])
    frame = frame.reset_index()
    frame.loc[(frame["i"] == "h2") & (frame["visit"] == 1), "Interview start"] = pd.NaT
    frame["visit"] = frame["visit"].astype("Int64")
    _serve(monkeypatch, frame.set_index(["i", "t", "v", "visit"]))
    assert _interview_dates("Fake") == {
        ("h1", "2021-22"): pd.Timestamp("2021-12-29 09:00:00"),
        ("h2", "2021-22"): pd.Timestamp("2022-07-02 08:00:00")}


def test_int_t_frame(monkeypatch):
    """The other 20 countries' shape: an integer id is keyed as a string."""
    frame = pd.DataFrame({"i": [101, 102], "t": ["2019-20", "2019-20"],
                          "Int_t": pd.to_datetime(["2019-08-15", "2020-02-01"])})
    _serve(monkeypatch, frame.set_index(["i", "t"]))
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        got = _interview_dates("Fake")
    assert got == {("101", "2019-20"): pd.Timestamp("2019-08-15"),
                   ("102", "2019-20"): pd.Timestamp("2020-02-01")}


def test_convert_as_of_with_time_of_day(monkeypatch):
    """Time of day is harmless: a factor year starts at midnight on 1 January.

    The third household has no interview date, so one frame holds served
    microsecond Timestamps beside the nominal-date fallback.
    """
    _serve(monkeypatch, _ehcvm_frame([
        ("dec31", "2018-19", "c1", 1, "2018-12-31 23:59:59"),
        ("jan1", "2018-19", "c1", 1, "2019-01-01 00:00:01"),
    ]))
    index = pd.MultiIndex.from_tuples(
        [("dec31", "2018-19", "rice"), ("jan1", "2018-19", "rice"),
         ("no_date", "2018-19", "rice")], names=["i", "t", "j"])
    frame = pd.DataFrame({"Expenditure": [1000.0, 1000.0, 1000.0]}, index=index)

    out = convert(frame, to="FX", country="Niger")

    fx_2018, fx_2019 = _factor("Niger", 2018, "FX"), _factor("Niger", 2019, "FX")
    assert fx_2018 != fx_2019
    assert out["Expenditure"].tolist() == pytest.approx(
        [1000 / fx_2018, 1000 / fx_2019, 1000 / fx_2018])
