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

    def is_sane(_df, _c, _f, **_kw):
        # **_kw absorbs ``extra_optional=`` (the populated-column set the
        # wave-slice grader passes; see coverage_matrix.grade_feature).
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


# ---------------------------------------------------------------------------
# Wave-slice grading must not apply the COUNTRY-level all-null-column check
# to a single wave (the 2026-07-12 regrade; 128 of 138 `builds` cells).
# ---------------------------------------------------------------------------
def test_wave_slice_exempts_columns_populated_elsewhere_in_the_country():
    """A column not fielded in ONE wave must not fail that wave.

    ``_check_no_all_null_columns`` is a country-level check.  Grading a wave by
    slicing the country frame on ``t`` made any question that a given wave did
    not ask look like a defect, because it is legitimately all-null *within
    that slice*.  ``grade_feature`` must exempt columns that are populated
    somewhere in the parent frame.
    """
    from lsms_library.diagnostics import is_this_feature_sane

    # Wave A fields `Latitude`; wave B does not.  Country-wide it IS populated.
    idx = pd.MultiIndex.from_tuples(
        [("A", "hh1"), ("A", "hh2"), ("B", "hh3"), ("B", "hh4")],
        names=["t", "i"],
    )
    df = pd.DataFrame({"Latitude": [1.5, 2.5, None, None]}, index=idx)

    sl_b = df[df.index.get_level_values("t") == "B"]        # the all-null slice
    populated = {c for c in df.columns if df[c].notna().any()}
    assert populated == {"Latitude"}

    # Without the exemption the slice fails; with it, it passes.
    naive = is_this_feature_sane(sl_b, "Uganda", "household_roster")
    fixed = is_this_feature_sane(sl_b, "Uganda", "household_roster",
                                 extra_optional=populated)
    naive_null = [c for c in naive.checks if c.name == "no_all_null_columns"]
    fixed_null = [c for c in fixed.checks if c.name == "no_all_null_columns"]
    assert naive_null and naive_null[0].status == "fail"   # the old, wrong grade
    assert fixed_null and fixed_null[0].status == "pass"   # the corrected grade


def test_column_all_null_country_wide_still_fails():
    """We relaxed the SLICE, not the country.  A truly dead column still fails."""
    from lsms_library.diagnostics import is_this_feature_sane

    idx = pd.MultiIndex.from_tuples(
        [("A", "hh1"), ("A", "hh2"), ("B", "hh3")], names=["t", "i"]
    )
    df = pd.DataFrame({"Latitude": [None, None, None]}, index=idx)   # never populated
    populated = {c for c in df.columns if df[c].notna().any()}
    assert populated == set()                                # nothing to exempt

    rep = is_this_feature_sane(df, "Uganda", "household_roster",
                               extra_optional=populated)
    null_check = [c for c in rep.checks if c.name == "no_all_null_columns"]
    assert null_check and null_check[0].status == "fail"


def test_extra_optional_default_preserves_historical_behaviour():
    """Omitting ``extra_optional`` must grade exactly as before (back-compat)."""
    from lsms_library.diagnostics import is_this_feature_sane

    idx = pd.MultiIndex.from_tuples([("A", "hh1"), ("A", "hh2")], names=["t", "i"])
    df = pd.DataFrame({"Latitude": [None, None]}, index=idx)

    a = is_this_feature_sane(df, "Uganda", "household_roster")
    b = is_this_feature_sane(df, "Uganda", "household_roster", extra_optional=None)
    pick = lambda r: [(c.name, c.status) for c in r.checks]
    assert pick(a) == pick(b)


# ---------------------------------------------------------------------------
# Absent-cell verdicts (GH #593) -- the four-way split of the `absent` tier.
# ---------------------------------------------------------------------------
def _verdicts_file(tmp_path, rows):
    p = tmp_path / "absent_verdicts.csv"
    hdr = "country,feature,wave,verdict,checks_run,evidence,adjudicated_by,date\n"
    p.write_text(hdr + "".join(rows))
    return p


def test_not_asked_closes_the_cell(tmp_path):
    p = _verdicts_file(tmp_path, [
        "C,foo,w2,not-asked,C1;C2;C4,no module in questionnaire s3,sue,2026-07-12\n"])
    v = cov.load_verdicts(p)
    co = _FakeCountry({"w1": ["foo"], "w2": []})
    cells = cov.grade_feature("C", "foo", ["w1", "w2"], co,
                              _env(_df_with_t({"w1": 3}), _ok_report()),
                              readiness=True, verdicts=v)
    assert _tiers(cells)[("foo", "w2")] == "not-asked"


def test_asked_not_distributed_is_its_own_tier(tmp_path):
    """The state the pilot found: instrument asked, extract does not carry it.

    Neither `todo` (nothing to configure) nor `not-asked` (it WAS asked) --
    it is an ACQUISITION problem and routes to a different queue.
    """
    p = _verdicts_file(tmp_path, [
        "C,foo,w2,asked-not-distributed,C1;C2;C4,"
        "questionnaire MODULE 2 lists it; vars absent from shipped dta,sue,2026-07-12\n"])
    v = cov.load_verdicts(p)
    co = _FakeCountry({"w1": ["foo"], "w2": []})
    cells = cov.grade_feature("C", "foo", ["w1", "w2"], co,
                              _env(_df_with_t({"w1": 3}), _ok_report()),
                              readiness=True, verdicts=v)
    assert _tiers(cells)[("foo", "w2")] == "asked-not-distributed"


@pytest.mark.parametrize("verdict", ["todo", "unsure"])
def test_todo_and_unsure_stay_in_the_queue(tmp_path, verdict):
    """`todo` and `unsure` are OPEN work -- they must NOT close the cell."""
    p = _verdicts_file(tmp_path, [
        f"C,foo,w2,{verdict},C1,found s08q01 in EACIACT_p1.dta,sue,2026-07-12\n"])
    v = cov.load_verdicts(p)
    co = _FakeCountry({"w1": ["foo"], "w2": []})
    cells = cov.grade_feature("C", "foo", ["w1", "w2"], co,
                              _env(_df_with_t({"w1": 3}), _ok_report()),
                              readiness=True, verdicts=v)
    cell = [c for c in cells if c["wave"] == "w2"][0]
    assert cell["tier"] == "absent"              # still in the queue
    assert verdict in cell["detail"]             # but the evidence is carried


def test_closing_verdict_without_evidence_is_REFUSED(tmp_path):
    """The load-bearing rule.

    A closing verdict is a permanent, unsupervised write.  An unevidenced
    negative is unfalsifiable and therefore permanent whether or not it is
    true -- exactly the failure already sitting in Albania's data_scheme.yml
    ("earlier waves have no shocks module"; Albania 2005 in fact carries
    m6e_q00 = 'Type of Shock Code').  So we refuse it.
    """
    p = _verdicts_file(tmp_path, ["C,foo,w2,not-asked,C1,,sue,2026-07-12\n"])
    with pytest.warns(UserWarning, match="unevidenced negative|REQUIRES"):
        v = cov.load_verdicts(p)
    assert v == {}                               # the row is dropped, not honoured

    co = _FakeCountry({"w1": ["foo"], "w2": []})
    cells = cov.grade_feature("C", "foo", ["w1", "w2"], co,
                              _env(_df_with_t({"w1": 3}), _ok_report()),
                              readiness=True, verdicts=v)
    assert _tiers(cells)[("foo", "w2")] == "absent"   # stays red. good.


def test_todo_without_evidence_is_allowed(tmp_path):
    """Only CLOSING verdicts need evidence; `todo` keeps the cell open anyway."""
    p = _verdicts_file(tmp_path, ["C,foo,w2,todo,C1,,sue,2026-07-12\n"])
    v = cov.load_verdicts(p)
    assert ("C", "foo", "w2") in v


def test_unknown_verdict_is_refused(tmp_path):
    p = _verdicts_file(tmp_path, ["C,foo,w2,definitely-not,C1,x,sue,2026-07-12\n"])
    with pytest.warns(UserWarning, match="unknown verdict"):
        assert cov.load_verdicts(p) == {}


def test_verdicts_missing_file_is_empty(tmp_path):
    assert cov.load_verdicts(tmp_path / "nope.csv") == {}


def test_absent_stays_absent_with_no_verdicts():
    """Back-compat: no verdicts file -> the historical grading, unchanged."""
    co = _FakeCountry({"w1": ["foo"], "w2": []})
    cells = cov.grade_feature("C", "foo", ["w1", "w2"], co,
                              _env(_df_with_t({"w1": 3}), _ok_report()),
                              readiness=True)
    assert _tiers(cells)[("foo", "w2")] == "absent"


# ---------------------------------------------------------------------------
# `unconfigured` + known-no-microdata (Defects 4 and 6)
# ---------------------------------------------------------------------------
def test_unconfigured_countries_finds_data_without_config(tmp_path):
    """A country dir with microdata but no _/data_scheme.yml must be REPORTED.

    `catalog.countries()` requires a data_scheme.yml, so these were invisible to
    the whole library -- 10 in-remit countries and ~35 waves of already-
    downloaded data that the matrix simply never mentioned.  A denominator that
    omits the work you have not started is not a denominator.
    """
    root = tmp_path / "countries"
    # configured country -> NOT unconfigured
    (root / "Uganda" / "_").mkdir(parents=True)
    (root / "Uganda" / "_" / "data_scheme.yml").write_text("Country: Uganda\n")
    (root / "Uganda" / "2019-20" / "Data").mkdir(parents=True)
    # data, no config -> unconfigured
    (root / "Peru" / "1994" / "Data").mkdir(parents=True)
    (root / "Peru" / "1991" / "Data").mkdir(parents=True)
    # no Data/ at all -> not a country dir; must be ignored
    (root / "stray").mkdir()

    got = cov.unconfigured_countries(lambda: root)
    assert got == {"Peru": 2}


def test_known_no_microdata_countries_are_na_not_broken():
    """Armenia/Nepal have NO source data.  That is a fact, not a defect.

    Before this, ALL 8 `broken` cells in the cube were these two countries --
    so the most alarming tier in the ladder contained nothing actionable, and a
    genuine build failure would have been lost among them.
    """
    known = cov.countries_without_microdata()
    assert set(known) >= {"Armenia", "Nepal"}

    co = _FakeCountry({"1996": ["household_roster"]})
    env = _env(FileNotFoundError("PathMissingError: INDSECA.dta"), _ok_report())

    cells = cov.grade_feature("Armenia", "household_roster", ["1996"], co, env,
                              readiness=True)
    cell = cells[0]
    assert cell["tier"] == "n/a"                      # not `broken`
    assert "no microdata in repo" in cell["detail"]

    # ...but a country that SHOULD have data still reports `broken`.
    cells = cov.grade_feature("Uganda", "household_roster", ["1996"], co, env,
                              readiness=True)
    assert cells[0]["tier"] == "broken"


def test_new_tiers_are_registered_in_the_ladder():
    for t in ("not-asked", "asked-not-distributed", "unconfigured"):
        assert t in cov.TIER_ORDER, t
        assert t in cov.ROLLUP_PRIORITY, t
    # every tier the model can emit must be rankable by the rollup
    assert set(cov.TIER_ORDER) == set(cov.ROLLUP_PRIORITY)


def test_scoped_snapshot_MERGES_and_does_not_destroy_other_cells(tmp_path):
    """A partial measurement must never erase a complete one.

    `make matrix C="Uganda"` grades only Uganda.  Writing that wholesale wiped
    every other country -- and docs/guide/coverage.md documented exactly that as
    the "spot refresh" procedure, followed by `git add latest.csv && commit`.
    Following the guide verbatim replaced the authoritative 1849-cell snapshot
    with a 67-cell one.  (Observed 2026-07-12.)
    """
    snap = tmp_path / "latest.csv"
    full = pd.DataFrame(
        [{"country": "Uganda", "feature": "housing", "wave": "2019-20",
          "tier": "builds", "coverage": "declared", "n_rows": "10", "detail": "old"},
         {"country": "Malawi", "feature": "housing", "wave": "2016-17",
          "tier": "sane", "coverage": "declared", "n_rows": "99", "detail": "keep me"}],
        columns=cov.COLUMNS)
    cov.save_snapshot(full, snap, merge=False)

    # a SCOPED re-run: only Uganda was graded
    scoped = pd.DataFrame(
        [{"country": "Uganda", "feature": "housing", "wave": "2019-20",
          "tier": "sane", "coverage": "declared", "n_rows": "10", "detail": "new"}],
        columns=cov.COLUMNS)
    cov.save_snapshot(scoped, snap)

    got = pd.read_csv(snap, dtype=str, keep_default_na=False)
    assert len(got) == 2, "the un-graded country was destroyed"
    uga = got[got.country == "Uganda"].iloc[0]
    mwi = got[got.country == "Malawi"].iloc[0]
    assert uga["tier"] == "sane" and uga["detail"] == "new"    # upserted
    assert mwi["tier"] == "sane" and mwi["detail"] == "keep me" # untouched


def test_save_snapshot_merge_false_still_replaces(tmp_path):
    snap = tmp_path / "latest.csv"
    cov.save_snapshot(pd.DataFrame(
        [{"country": "Malawi", "feature": "housing", "wave": "w", "tier": "sane",
          "coverage": "declared", "n_rows": "1", "detail": ""}],
        columns=cov.COLUMNS), snap, merge=False)
    cov.save_snapshot(pd.DataFrame(
        [{"country": "Uganda", "feature": "housing", "wave": "w", "tier": "sane",
          "coverage": "declared", "n_rows": "1", "detail": ""}],
        columns=cov.COLUMNS), snap, merge=False)
    got = pd.read_csv(snap, dtype=str)
    assert list(got.country) == ["Uganda"]
