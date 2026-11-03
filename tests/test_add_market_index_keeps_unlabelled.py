"""``_add_market_index`` keeps a household it cannot label, and says so.

``Country._add_market_index`` (every ``market=`` call) labels each row from
``sample()[column]`` at household level, then from ``cluster_features`` on
``(t, v)``.  A row neither source could label used to be deleted by
``flat.dropna(subset=['m'])``, with no warning.  Measured at ``3a33f2869``:
Albania 2008 and 2012 ``sample(market='Region')`` lost every household
(3,599 and 6,671), Tanzania 2008-09 / 2010-11 ``market='District'`` likewise,
and Burkina Faso 2021-22 7,176 of 10,403 (until #1276 wired its
cross-section).  The row is now KEPT with ``m`` NA and reported through Site I's
family (``NullIndexKeyWarning``, ``LSMS_READ_STRICT``, filed in
``null_index_reports`` with ``site='market-index'``).

Synthetic: no data, no cache.  The fixture borrows the two real methods onto
a stub that answers ``sample()`` and ``_market_lookup()`` itself.
"""
import warnings

import pandas as pd
import pytest

from lsms_library.country import Country
from lsms_library.null_index_audit import (NullIndexKeyError,
                                           NullIndexKeyWarning,
                                           _clear_null_index_reports,
                                           null_index_reports)


class _MarketStub:
    """What ``_add_market_index`` reads, and nothing else."""
    name = "Fixtureland"
    data_scheme = {"sample": {}, "cluster_features": {}}
    _add_market_index = Country._add_market_index
    _report_unlabelled_markets = Country._report_unlabelled_markets

    def __init__(self, sample, cluster):
        self._sample = sample
        self._cluster = cluster

    def sample(self):
        return self._sample

    def _market_lookup(self, column="Region"):
        return self._cluster


def _sample():
    # h1: own Region.  h2: no own Region, cluster c2 labelled.  h3: no own
    # Region, cluster c3 absent from cluster_features -> unlabelled.
    # h4 (wave 2001): own Region NA, cluster c4 present but Region null there.
    idx = pd.MultiIndex.from_tuples(
        [("h1", "2000"), ("h2", "2000"), ("h3", "2000"), ("h4", "2001")],
        names=["i", "t"])
    return pd.DataFrame({"v": ["c1", "c2", "c3", "c4"],
                         "Region": ["North", pd.NA, pd.NA, pd.NA]}, index=idx)


def _cluster():
    # _market_lookup's contract: columns t, v, m; null labels already removed.
    return pd.DataFrame({"t": ["2000", "2000"], "v": ["c1", "c2"],
                         "m": ["North", "South"]})


def _table():
    """A tall household table, (t, v, i, j): h3 has three rows."""
    rows = [("2000", "c1", "h1", "maize"), ("2000", "c2", "h2", "maize"),
            ("2000", "c3", "h3", "maize"), ("2000", "c3", "h3", "beans"),
            ("2000", "c3", "h3", "rice"), ("2001", "c4", "h4", "maize")]
    idx = pd.MultiIndex.from_tuples(rows, names=["t", "v", "i", "j"])
    return pd.DataFrame({"Expenditure": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]},
                        index=idx)


@pytest.fixture(autouse=True)
def _clean_ledger(monkeypatch):
    monkeypatch.delenv("LSMS_READ_STRICT", raising=False)
    _clear_null_index_reports()
    yield
    _clear_null_index_reports()


def _run(table="food_expenditures"):
    stub = _MarketStub(_sample(), _cluster())
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = stub._add_market_index(_table(), column="Region", table=table)
    return out, [w for w in caught if issubclass(w.category, NullIndexKeyWarning)]


def test_unlabelled_rows_survive_with_na_market():
    out, _ = _run()
    df = _table()
    assert len(out) == len(df), "no row may be dropped by market="
    assert list(out.index.names) == ["t", "m", "i", "j"]
    assert out.index.is_unique
    m = out.reset_index().set_index(["i", "j"])["m"]
    assert m.loc[("h1", "maize")] == "North"         # household level
    assert m.loc[("h2", "maize")] == "South"         # cluster fallback
    # A real missing value -- not the string '<NA>' / 'nan' the str path in
    # _market_lookup could have produced.
    assert m.loc["h3"].isna().all()
    assert pd.isna(m.loc[("h4", "maize")])
    assert out["Expenditure"].sum() == df["Expenditure"].sum()


def test_one_warning_with_rows_and_households_per_wave():
    _, warned = _run()
    assert len(warned) == 1, [str(w.message) for w in warned]
    msg = str(warned[0].message)
    assert "Fixtureland/food_expenditures(market='Region')" in msg
    assert "4 of 6 row(s)" in msg
    assert "2000: 3 row(s) / 1 household(s)" in msg
    assert "2001: 1 row(s) / 1 household(s)" in msg
    assert "dropna=True" in msg and "LSMS_READ_STRICT" in msg
    # Attributed to the caller (this file), not to a frame inside the library.
    assert warned[0].filename == __file__


def test_report_is_filed_in_the_site_i_ledger():
    _run()
    reports = null_index_reports(country="Fixtureland", table="food_expenditures")
    assert len(reports) == 1
    rep = reports[0]
    assert rep["site"] == "market-index"
    assert rep["column"] == "Region"
    assert rep["rows"] == 6
    (finding,) = rep["findings"]
    assert finding["level"] == "m"
    assert finding["n_nan"] == 4
    assert finding["waves"] == {"2000": 3, "2001": 1}
    assert finding["households"] == {"2000": 1, "2001": 1}


def test_strict_lever_is_read_strict(monkeypatch):
    monkeypatch.setenv("LSMS_READ_STRICT", "1")
    stub = _MarketStub(_sample(), _cluster())
    with pytest.raises(NullIndexKeyError, match="no 'Region' label"):
        stub._add_market_index(_table(), column="Region", table="food_expenditures")
    # Filed before raising, as Site I does.
    assert null_index_reports(country="Fixtureland")


def test_other_levers_do_not_ratchet_it(monkeypatch):
    for lever in ("LSMS_GRAIN_STRICT", "LSMS_QUANTITY_STRICT", "LSMS_CLUSTER_STRICT"):
        monkeypatch.setenv(lever, "1")
    out, warned = _run()
    assert len(out) == 6 and len(warned) == 1


def test_fully_labelled_frame_is_silent():
    sample = _sample()
    sample["Region"] = ["North", "South", "East", "West"]
    stub = _MarketStub(sample, _cluster())
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = stub._add_market_index(_table(), column="Region", table="t1")
    assert not [w for w in caught if issubclass(w.category, NullIndexKeyWarning)]
    assert out.index.get_level_values("m").notna().all()
    assert not null_index_reports(country="Fixtureland")


def test_a_default_groupby_still_drops_them():
    """The caveat the warning states: keeping the rows does not stop a
    consumer's ``groupby('m')`` (pandas' ``dropna=True``) from dropping them."""
    out, _ = _run()
    by_default = out.groupby(level="m")["Expenditure"].sum()
    kept = out.groupby(level="m", dropna=False)["Expenditure"].sum()
    assert by_default.sum() == 3.0
    assert kept.sum() == out["Expenditure"].sum() == 21.0
