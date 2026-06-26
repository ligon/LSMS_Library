"""Tests for the country × feature × wave coverage/readiness matrix.

Two layers:

- **Unit** — drive ``coverage.grade_feature`` with fake Country/Wave/env objects
  so every tier in the ladder is exercised deterministically, no builds.
- **Integration** — the cheap, auth-free coverage layer
  (``build_matrix(readiness=False)``) and the ``ll.coverage()`` reader / snapshot
  round-trip.

The unit layer uses the *real* ``Check`` / ``SanityReport`` dataclasses so the
builds↔sane boundary (``SanityReport.ok``) is the genuine one.
"""
from __future__ import annotations

import os

import pandas as pd
import pytest

import lsms_library as ll
from lsms_library import coverage_matrix as cov
from lsms_library.diagnostics import Check, SanityReport


# ---------------------------------------------------------------------------
# Fakes for the unit layer
# ---------------------------------------------------------------------------
class _FakeWave:
    def __init__(self, tables):
        self.data_scheme = list(tables)


class _FakeCountry:
    """``co[wave]`` -> a fake Wave with a chosen ``data_scheme``."""
    def __init__(self, wave_tables):
        self._wt = wave_tables

    def __getitem__(self, w):
        return _FakeWave(self._wt.get(w, []))


def _env(load_result, report=None, derived=None):
    """Build the env dict ``grade_feature`` consumes.

    ``load_result`` is the DataFrame returned by ``load_feature``; pass an
    ``Exception`` instance to simulate a build failure.
    """
    def load_feature(_co, _feature):
        if isinstance(load_result, Exception):
            raise load_result
        return load_result

    def is_sane(_df, _c, _f):
        return report

    return {
        "DERIVED_SOURCE": derived or {},
        "load_feature": load_feature,
        "is_this_feature_sane": is_sane,
    }


def _df_with_t(waves_rows):
    """A frame indexed by (t, i) with the given {wave: nrows}."""
    tuples = []
    for w, n in waves_rows.items():
        tuples += [(w, f"hh{j}") for j in range(n)]
    idx = pd.MultiIndex.from_tuples(tuples, names=["t", "i"])
    return pd.DataFrame({"x": range(len(tuples))}, index=idx)


def _ok_report():
    return SanityReport("C", "foo", [Check("not_empty", "pass")])


def _fail_report():
    return SanityReport("C", "foo",
                        [Check("not_empty", "pass"), Check("dup_index", "fail")])


def _tiers(cells):
    return {(c["feature"], c["wave"]): c["tier"] for c in cells}


# ---------------------------------------------------------------------------
# Unit: the tier ladder
# ---------------------------------------------------------------------------
def test_coverage_only_declared_vs_absent():
    co = _FakeCountry({"w1": ["foo"], "w2": []})
    cells = cov.grade_feature("C", "foo", ["w1", "w2"], co, _env(None),
                              readiness=False)
    t = _tiers(cells)
    assert t[("foo", "w1")] == "declared"
    assert t[("foo", "w2")] == "absent"


def test_readiness_sane_dropped_absent():
    # w1 declared+built(sane); w2 declared but missing from t-index -> dropped;
    # w3 not declared -> absent.
    co = _FakeCountry({"w1": ["foo"], "w2": ["foo"], "w3": []})
    df = _df_with_t({"w1": 3})
    cells = cov.grade_feature("C", "foo", ["w1", "w2", "w3"], co,
                              _env(df, _ok_report()), readiness=True)
    t = _tiers(cells)
    assert t[("foo", "w1")] == "sane"
    assert t[("foo", "w2")] == "dropped"
    assert t[("foo", "w3")] == "absent"


def test_readiness_builds_when_sanity_fails():
    co = _FakeCountry({"w1": ["foo"]})
    df = _df_with_t({"w1": 2})
    cells = cov.grade_feature("C", "foo", ["w1"], co,
                              _env(df, _fail_report()), readiness=True)
    assert _tiers(cells)[("foo", "w1")] == "builds"


def test_readiness_dropped_on_zero_row_slice():
    # wave present in t-index but with 0 rows is a drop, not a sane cell.
    co = _FakeCountry({"w1": ["foo"], "w2": ["foo"]})
    df = _df_with_t({"w1": 2, "w2": 0})  # w2 contributes no tuples
    cells = cov.grade_feature("C", "foo", ["w1", "w2"], co,
                              _env(df, _ok_report()), readiness=True)
    assert _tiers(cells)[("foo", "w2")] == "dropped"


def test_readiness_broken_when_build_raises():
    co = _FakeCountry({"w1": ["foo"], "w2": []})
    cells = cov.grade_feature("C", "foo", ["w1", "w2"], co,
                              _env(RuntimeError("boom"), _ok_report()),
                              readiness=True)
    t = _tiers(cells)
    assert t[("foo", "w1")] == "broken"   # declared -> broken
    assert t[("foo", "w2")] == "absent"   # not declared -> absent (not broken)


def test_readiness_broken_when_empty_frame():
    co = _FakeCountry({"w1": ["foo"]})
    empty = _df_with_t({})  # 0 rows
    cells = cov.grade_feature("C", "foo", ["w1"], co,
                              _env(empty, _ok_report()), readiness=True)
    assert _tiers(cells)[("foo", "w1")] == "broken"


def test_blessed_promotes_sane():
    co = _FakeCountry({"w1": ["foo"]})
    df = _df_with_t({"w1": 2})
    blessed = {("C", "foo", "w1")}
    cells = cov.grade_feature("C", "foo", ["w1"], co,
                              _env(df, _ok_report()), readiness=True,
                              blessed=blessed)
    assert _tiers(cells)[("foo", "w1")] == "blessed"


def test_no_t_axis_grades_country_level_na_per_wave():
    co = _FakeCountry({"w1": ["foo"], "w2": ["foo"]})
    # frame without a 't' index level
    df = pd.DataFrame({"x": [1, 2]},
                      index=pd.Index(["a", "b"], name="i"))
    cells = cov.grade_feature("C", "foo", ["w1", "w2"], co,
                              _env(df, _ok_report()), readiness=True)
    t = _tiers(cells)
    assert t[("foo", "w1")] == "n/a"
    assert t[("foo", "w2")] == "n/a"
    # plus a wave=None country-level summary row carrying the real grade
    summary = [c for c in cells if c["wave"] == ""]
    assert len(summary) == 1 and summary[0]["tier"] == "sane"


def test_derived_lift_makes_derived_feature_covered():
    # 'food_expenditures' is covered at a wave iff its source 'food_acquired' is.
    co = _FakeCountry({"w1": ["food_acquired"], "w2": ["household_roster"]})
    derived = {"food_expenditures": "food_acquired"}
    cells = cov.grade_feature("C", "food_expenditures", ["w1", "w2"], co,
                              _env(None, derived=derived), readiness=False)
    t = _tiers(cells)
    assert t[("food_expenditures", "w1")] == "declared"
    assert t[("food_expenditures", "w2")] == "absent"


def test_tier_from_report_boundary():
    assert cov.tier_from_report(_ok_report()) == "sane"
    assert cov.tier_from_report(_fail_report()) == "builds"


# ---------------------------------------------------------------------------
# Unit: blessing + snapshot round-trip
# ---------------------------------------------------------------------------
def test_load_blessed_roundtrip(tmp_path):
    p = tmp_path / "blessed.csv"
    p.write_text("country,feature,wave\nUganda,food_prices,2013-14\nMali,assets,\n")
    b = cov.load_blessed(p)
    assert ("Uganda", "food_prices", "2013-14") in b
    assert ("Mali", "assets", "") in b


def test_load_blessed_missing_file_is_empty(tmp_path):
    assert cov.load_blessed(tmp_path / "nope.csv") == set()


def test_save_and_read_snapshot(tmp_path):
    df = pd.DataFrame(
        [{"country": "C", "feature": "foo", "wave": "w1", "tier": "sane",
          "coverage": "declared", "n_rows": 5, "detail": "ok"}],
        columns=cov.COLUMNS,
    )
    df["tier"] = pd.Categorical(df["tier"], categories=cov.TIER_ORDER, ordered=True)
    snap = tmp_path / "latest.csv"
    cov.save_snapshot(df, snap)
    back = ll.coverage(snapshot=snap)
    assert list(back.columns) == cov.COLUMNS
    assert back.iloc[0]["tier"] == "sane"
    assert str(back.iloc[0]["wave"]) == "w1"


def test_coverage_reader_missing_snapshot_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        ll.coverage(snapshot=tmp_path / "absent.csv")


def test_coverage_reader_rejects_unknown_refresh():
    # A typo'd refresh mode must raise, not silently read a stale snapshot.
    with pytest.raises(ValueError):
        ll.coverage(refresh="readyness")


# ---------------------------------------------------------------------------
# Integration: the auth-free coverage layer over real config
# ---------------------------------------------------------------------------
def test_coverage_layer_is_auth_free_and_nonempty(monkeypatch):
    """The coverage layer must work with no data access (config only)."""
    monkeypatch.setenv("LSMS_SKIP_AUTH", "1")
    df = ll.coverage(refresh="coverage", countries=["Uganda"])
    assert len(df) > 0
    assert set(df.columns) == set(cov.COLUMNS)
    tiers = set(df["tier"].astype(str))
    # coverage-only run yields only declared / absent / n/a
    assert tiers <= {"declared", "absent", "n/a"}
    assert "declared" in tiers
    # every cell is a real (Uganda) wave
    uga_waves = set(ll.Country("Uganda").waves)
    wave_cells = df[df["wave"] != ""]
    assert set(wave_cells["wave"]).issubset(uga_waves)


def test_build_matrix_covers_declared_features(monkeypatch):
    monkeypatch.setenv("LSMS_SKIP_AUTH", "1")
    df = cov.build_matrix(["Uganda"], readiness=False)
    feats = set(df["feature"])
    # household_roster is declared for Uganda; food_expenditures is derived-surfaced
    assert "household_roster" in feats
    assert "food_expenditures" in feats
