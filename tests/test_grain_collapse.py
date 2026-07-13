"""GH #323 -- the grain-collapse audit.

``country._normalize_dataframe_index`` reduces a non-unique DECLARED index with
``groupby(...).first()``.  Where the duplicate rows DISAGREE, the rows it drops are
real data and used to vanish with no signal.

Two properties are load-bearing and each has a test that FAILS on pre-fix code:

1.  A DESTRUCTIVE collapse is LOUD (and fatal under ``LSMS_GRAIN_STRICT``).
2.  The signal SURVIVES THE CACHE.  This is the one that matters.  The old #323
    warning was gated on ``not df.index.is_unique``, and the L2-country parquet is
    written POST-collapse -- so on a warm read the index is already unique and the
    warning was *structurally unable to fire*.  Practically every read is warm.
    The bug hid behind the cache that the bug poisoned.

And one that keeps the signal READABLE:

3.  A collapse of duplicate rows that are IDENTICAL is a lossless de-dup and stays
    SILENT.  Across the 40 countries ~6.4M of the ~7.5M duplicate rows are of this
    kind; warning on all of them would bury the ~540k real losses in noise, and a
    warning nobody reads is how this bug survived its first fix.
"""
from __future__ import annotations

import json
import warnings

import pandas as pd
import pytest

from lsms_library import local_tools as lt
from lsms_library.country import (
    GrainCollapseError,
    GrainCollapseWarning,
    _audit_index_collapse,
    _GRAIN_LEDGER,
    _normalize_dataframe_index,
    _replay_grain_audit,
    grain_reports,
)

SCHEME = {"index": "(t, i)"}


@pytest.fixture(autouse=True)
def _clean_ledger(monkeypatch):
    monkeypatch.delenv("LSMS_GRAIN_STRICT", raising=False)
    _GRAIN_LEDGER.clear()
    yield
    _GRAIN_LEDGER.clear()


def _destructive() -> pd.DataFrame:
    """Two DIFFERENT people behind one (t, i) key -- the Mali shape in miniature."""
    return pd.DataFrame(
        {"Age": [40, 9], "Sex": ["F", "M"]},
        index=pd.MultiIndex.from_tuples([("2020", "h1"), ("2020", "h1")],
                                        names=["t", "i"]),
    )


def _redundant() -> pd.DataFrame:
    """Two IDENTICAL rows -- collapsing them destroys nothing (the cluster shape)."""
    return pd.DataFrame(
        {"Region": ["North", "North"]},
        index=pd.MultiIndex.from_tuples([("2020", "v1"), ("2020", "v1")],
                                        names=["t", "i"]),
    )


# --------------------------------------------------------------------------
# 1. destructive -> LOUD
# --------------------------------------------------------------------------

def test_destructive_collapse_warns_and_counts_the_rows_it_destroyed():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = _normalize_dataframe_index(_destructive(), SCHEME, None,
                                         "household_roster", country="Testland")

    assert len(out) == 1, "the collapse still happens (this fix reports, it does not aggregate)"
    grain = [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert len(grain) == 1, "a destructive collapse must warn"

    msg = str(grain[0].message)
    assert "Testland/household_roster" in msg, "the report must NAME the cell"
    assert "DESTROYED 1" in msg
    assert "GH #323" in msg

    (report,) = grain_reports(country="Testland", table="household_roster")
    assert report["destroyed"] == 1
    assert report["conflicting_groups"] == 1


def test_strict_mode_raises_so_ci_can_ratchet():
    """Warning by default (raising breaks ~30 countries at once and gets reverted);
    fatal on demand, so tests/CI can drive the census to zero WITHOUT a known-bad
    allowlist -- an allowlist is the same disease with a registry."""
    import os
    os.environ["LSMS_GRAIN_STRICT"] = "1"
    try:
        with pytest.raises(GrainCollapseError, match="NOT UNIQUE"):
            _normalize_dataframe_index(_destructive(), SCHEME, None,
                                       "household_roster", country="Testland")
    finally:
        del os.environ["LSMS_GRAIN_STRICT"]


def test_nan_in_a_declared_index_level_is_deleted_outright_and_reported():
    """A SEPARATE loss riding in the same operation: groupby defaults to
    dropna=True, so a row with NaN in a declared index level is DELETED, not
    merely merged.  14 cells / 485,231 rows across the corpus; worst is
    Burkina_Faso/food_acquired/2014 at 460,438 of 557,822 rows (82.5%)."""
    df = pd.DataFrame(
        {"Age": [40, 9, 7]},
        index=pd.MultiIndex.from_tuples(
            [("2020", "h1"), ("2020", "h1"), ("2020", None)], names=["t", "i"]),
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = _normalize_dataframe_index(df, SCHEME, None, "household_roster",
                                         country="Testland")

    assert len(out) == 1, "the NaN-keyed row is dropped by groupby(dropna=True)"
    msg = str([w for w in caught
               if issubclass(w.category, GrainCollapseWarning)][0].message)
    assert "DELETED OUTRIGHT" in msg
    assert grain_reports()[0]["nan_key_rows"] == 1


# --------------------------------------------------------------------------
# 2. THE ONE THAT MATTERS: the signal survives the cache
# --------------------------------------------------------------------------

def test_grain_audit_round_trips_through_the_parquet(tmp_path):
    """The audit must be embedded in the parquet, because the frame that PROVED
    the loss is destroyed one line after it is measured."""
    fn = tmp_path / "t.parquet"
    audit = [{"levels": ["t", "i"], "rows": 2, "dropped": 1, "destroyed": 1,
              "conflicting_groups": 1, "nan_key_rows": 0}]
    lt.to_parquet(_redundant(), fn, absolute_path=True, grain_audit=audit)

    assert lt.read_parquet_grain_audit(fn) == audit
    # and a parquet written without one is not falsely accused
    fn2 = tmp_path / "clean.parquet"
    lt.to_parquet(_redundant(), fn2, absolute_path=True)
    assert lt.read_parquet_grain_audit(fn2) is None


def test_warm_read_replays_the_loss_recorded_by_the_cold_build():
    """THE regression test for the mechanism that hid #323 twice.

    A warm read gets an already-collapsed frame: its index IS unique, so no
    detector standing at the collapse site can ever fire again.  The only way the
    loss can still be reported is by replaying what the cold build stamped into
    the parquet.  Pre-fix there is no stamp and no replay, and this is silent.
    """
    stamped = [{"levels": ["t", "i", "pid"], "rows": 37175, "dropped": 32026,
                "destroyed": 32026, "conflicting_groups": 3852, "nan_key_rows": 0}]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _replay_grain_audit(stamped, "Mali", "household_roster")

    grain = [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert len(grain) == 1, "a warm read of a poisoned cache must still speak up"
    msg = str(grain[0].message)
    assert "Mali/household_roster" in msg
    assert "DESTROYED 32,026" in msg


# --------------------------------------------------------------------------
# 3. precision: a lossless collapse stays silent
# --------------------------------------------------------------------------

def test_lossless_dedup_is_silent():
    """Identical duplicate rows -> first() loses nothing -> no warning.

    This is what keeps the signal readable: 6.4M of the 7.5M duplicate rows in the
    corpus are of exactly this kind (a cluster attribute repeated once per
    household in the cluster)."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = _normalize_dataframe_index(_redundant(), SCHEME, None,
                                         "cluster_features", country="Testland")

    assert len(out) == 1
    assert [w for w in caught if issubclass(w.category, GrainCollapseWarning)] == []
    assert grain_reports() == []


def test_a_declared_level_that_is_absent_is_not_dropped_silently():
    """The chained silent failure: a declared index level that is neither an index
    level nor a column was silently dropped, NARROWING the index behind the
    caller's back -- which MANUFACTURES the duplicates the collapse then destroys.

    Loud even when the narrowed index happens to stay unique: a silently narrowed
    index is a defect regardless.  (Measured occurrences in the corpus today: zero
    -- so this is a door being closed, not a fire being fought.)
    """
    df = pd.DataFrame(
        {"Age": [40, 9]},
        index=pd.MultiIndex.from_tuples([("2020", "h1"), ("2020", "h2")],
                                        names=["t", "i"]),
    )
    scheme = {"index": "(t, i, pid)"}   # 'pid' exists nowhere in the frame
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = _normalize_dataframe_index(df, scheme, None, "household_roster",
                                         country="Testland")

    assert list(out.index.names) == ["t", "i"], "index was narrowed"
    grain = [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert len(grain) == 1, "a silently narrowed index must not stay silent"
    msg = str(grain[0].message)
    assert "SILENTLY NARROWED" in msg
    assert "'pid'" in msg or "pid" in msg


def test_unique_index_is_never_touched():
    df = pd.DataFrame(
        {"Age": [40, 9]},
        index=pd.MultiIndex.from_tuples([("2020", "h1"), ("2020", "h2")],
                                        names=["t", "i"]),
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = _normalize_dataframe_index(df, SCHEME, None, "household_roster",
                                         country="Testland")
    assert len(out) == 2
    assert [w for w in caught if issubclass(w.category, GrainCollapseWarning)] == []


def test_additive_sum_is_lossless_and_stays_silent():
    """food_acquired's duplicates are a genuine multi-transaction grain and are
    SUMMED (`_ADDITIVE_MEASURE_COLUMNS`).  Nothing is destroyed, so nothing is
    reported -- the one real reduction policy we have stays exactly as it was."""
    df = pd.DataFrame(
        {"Quantity": [2.0, 3.0], "Expenditure": [10.0, 15.0]},
        index=pd.MultiIndex.from_tuples([("2020", "h1"), ("2020", "h1")],
                                        names=["t", "i"]),
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = _normalize_dataframe_index(df, SCHEME, None, "food_acquired",
                                         country="Testland")

    assert out["Quantity"].iloc[0] == 5.0, "summed, not first()-ed"
    assert [w for w in caught if issubclass(w.category, GrainCollapseWarning)] == []


# --------------------------------------------------------------------------
# the audit function itself
# --------------------------------------------------------------------------

def test_audit_treats_a_missing_value_as_a_value():
    """Two rows that differ only in WHETHER a field is recorded are different rows.

    Deliberately conservative: it over-reports rather than under-reports.  This is
    what catches Burkina_Faso/shocks, where first() keeps an all-<NA> row and throws
    away the row that has the real answers -- silently WRONG, the worst class.
    """
    df = pd.DataFrame(
        {"AffectedIncome": [pd.NA, True]},
        index=pd.MultiIndex.from_tuples([("2014", "h1"), ("2014", "h1")],
                                        names=["t", "i"]),
    )
    report = _audit_index_collapse(df, ["t", "i"])
    assert report is not None and report["destroyed"] == 1


def test_audit_returns_none_when_provably_lossless():
    assert _audit_index_collapse(_redundant(), ["t", "i"]) is None


def test_an_unauditable_collapse_is_reported_not_assumed_safe(monkeypatch):
    """``None`` from the audit means PROVABLY LOSSLESS.  So a failure to audit must
    never come back as ``None`` -- an instrument that fails silently and reports
    "clean" is the exact disease this change exists to cure.  ("A negative result
    from an instrument you have not validated is a result about the instrument.")

    The audit is hard to break in practice (``astype(str)`` swallows even unhashable
    cells), so force the failure rather than pretend to have found a natural one.
    """
    def boom(self, *a, **k):
        raise TypeError("unhashable type: 'list'")
    monkeypatch.setattr(pd.DataFrame, "drop_duplicates", boom)

    report = _audit_index_collapse(_destructive(), ["t", "i"])
    assert report is not None, "a failed audit must NOT masquerade as 'lossless'"
    assert report["unauditable"]
    assert report["dropped"] == 1

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _normalize_dataframe_index(_destructive(), SCHEME, None, "shocks",
                                   country="Testland")
    grain = [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert len(grain) == 1
    assert "COULD NOT BE AUDITED" in str(grain[0].message)


def test_unauditable_is_not_silenced_on_the_additive_path(monkeypatch):
    """food_acquired's additive SUM is lossless, so its reports are normally
    suppressed.  But "we could not check" must never be downgraded to "it is fine"."""
    def boom(self, *a, **k):
        raise TypeError("forced")
    monkeypatch.setattr(pd.DataFrame, "drop_duplicates", boom)

    df = pd.DataFrame(
        {"Quantity": [2.0, 3.0], "Expenditure": [10.0, 15.0]},
        index=pd.MultiIndex.from_tuples([("2020", "h1"), ("2020", "h1")],
                                        names=["t", "i"]),
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _normalize_dataframe_index(df, SCHEME, None, "food_acquired",
                                   country="Testland")
    grain = [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert len(grain) == 1, "an unauditable additive collapse must still speak up"
    assert "COULD NOT BE AUDITED" in str(grain[0].message)


def test_aggregation_key_is_not_honoured_as_a_core_reduction_policy():
    """You cannot DECLARE your way out of a broken index (GH #323).

    Duplicates on a declared index mean the identifier is broken or a level is
    missing.  Mali declares `(t, i, pid)` but `pid` is a HOUSEHOLD id stamped on
    every member, so no reducer is correct: `first` keeps one person per household,
    `sum` is meaningless on `Sex`.  A declared `aggregation:` would only put a
    signature on the corpse -- a silently-wrong result WITH paperwork.

    The core does not aggregate (SkunkWorks/grain_aggregation_policy.org: NO
    AGGREGATION IN CORE).  An `aggregation:` block must NOT suppress the report.
    """
    scheme = {"index": "(t, i)", "aggregation": {"i": "first"}}
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _normalize_dataframe_index(_destructive(), scheme, None,
                                   "household_roster", country="Testland")
    grain = [w for w in caught if issubclass(w.category, GrainCollapseWarning)]
    assert len(grain) == 1, "a declared 'aggregation:' must not buy silence"
    assert "Do NOT declare a reducer" in str(grain[0].message)
