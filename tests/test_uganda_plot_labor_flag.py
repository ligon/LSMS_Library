"""GH #1250: a MISSING labor use-flag is not "answered No".

``uganda._labor_persondays`` zeroes a source's person-days where the survey's
use-flag says "No" (family gate ``a3aq38 == 0`` -- a WORKER COUNT, so 0 means
nobody worked -- in 2009-10 / 2010-11; hired gate ``q41 / q34 == 2``
everywhere).  It mirrors the World Bank's
``replace hired_man_days = 0 if A3aq41==2`` (``UGA_UNPS1.do:484``), and in
Stata a missing flag is not ``== 2``, so that row is left alone.

Before the fix the flag went through ``_to_int_code`` (nullable ``Int64``),
``flag != code`` was ``<NA>`` for a missing flag, and ``Series.where`` read
``<NA>`` as False: every missing-flag row was served ``PersonDays = 0``.  That
fabricated ~5,200 zero rows (mostly 2009-10 plot-less stubs) and also zeroed
~100 rows whose day cells WERE reported.

Two families:

* synthetic -- no data; always run.  Fail on development, pass after the fix.
* ``test_every_served_zero_is_justified_by_the_source`` -- data-gated (S3
  credentials); re-derives every served zero of 2009-10 and 2010-11 (the two
  waves with both gates) from the raw AGSEC3A/3B and requires a "No" flag or
  reported zero days behind it.  Reads raw files directly; no Country() cache.

Follow-up (ruling @ligon 2026-10-23): a HIRED row with a reported Wage and no
reported days is served on its wage with ``PersonDays`` NaN, following Malawi
(``malawi.py:3766-3767``).  ``test_wage_only_hired_rows_are_served``
(synthetic) and ``test_every_reported_hired_wage_is_served`` (data-gated, every
wave) pin it; both fail on 90fc1e18d, which served only rows with days.
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lsms_library.paths import countries_root


def _aws_creds_available() -> bool:
    """Shared resolver in ``tests/conftest.py``, loaded by path (see
    ``tests/test_tests_package_not_shadowed.py``)."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "_shared_conftest", Path(__file__).with_name("conftest.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.aws_creds_available()


@pytest.fixture(scope="module")
def uganda_module():
    uganda_dir = str(countries_root() / "Uganda" / "_")
    sys.path.insert(0, uganda_dir)
    try:
        import uganda
    except ImportError as exc:  # pragma: no cover
        pytest.skip(f"cannot import uganda module: {exc!r}")
    return uganda


# ---------------------------------------------------------------------------
# _labor_persondays: the four flag/cell cases, for both gate codes
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("no_code, yes_code", [(0, 3), (2, 1)],
                         ids=["family-gate-0", "hired-gate-2"])
def test_missing_flag_is_not_no(uganda_module, no_code, yes_code):
    df = pd.DataFrame({
        "flag": [np.nan, np.nan, no_code, no_code, yes_code, yes_code],
        "d1":   [np.nan, 4.0,    5.0,     np.nan,  2.0,      np.nan],
        "d2":   [np.nan, 1.0,    np.nan,  np.nan,  np.nan,   np.nan],
    })
    got = uganda_module._labor_persondays(df, ["d1", "d2"], "flag", no_code)
    expected = [np.nan,  # missing flag, no cells: NOT reported -> NaN
                5.0,     # missing flag, reported days: served as reported
                0.0,     # "No", cells present: the WB replace -> 0
                0.0,     # "No", no cells: a genuine reported zero
                2.0,     # "Yes": served as reported
                np.nan]  # "Yes" but no cells: not reported
    assert got.tolist() == pytest.approx(expected, nan_ok=True)


def test_missing_flag_column_values_from_stata_floats(uganda_module):
    """The raw .dta flags arrive as float64 with NaN; the cast to Int64 is
    what turned a NaN into <NA>.  Same answer either way."""
    df = pd.DataFrame({"flag": pd.Series([np.nan, 2.0, 1.0], dtype="float64"),
                       "d": [3.0, 3.0, 3.0]})
    got = uganda_module._labor_persondays(df, ["d"], "flag", 2)
    assert got.tolist() == [3.0, 0.0, 3.0]


# ---------------------------------------------------------------------------
# plot_labor_for_wave: the builder serves no fabricated zero
# ---------------------------------------------------------------------------

def _agsec3a_2009_10():
    """Synthetic 2009-10 AGSEC3A, named as LABOR_COLMAPS['2009-10']['A']."""
    nan = np.nan
    rows = [
        # HHID,       parcel, plot, fam_n, fam_d, h_flag, man,  wom,  chi,  wage
        ("1013000101", 1.0, 1.0, nan, nan, nan, nan, nan, nan, nan),   # stub
        ("1013000101", 1.0, 2.0, nan, 12.0, nan, 3.0, nan, nan, nan),  # flag NA, days
        ("1013000101", 1.0, 3.0, 0.0, 5.0, 2.0, 4.0, nan, nan, nan),   # said No
        ("1013000101", 1.0, 4.0, 2.0, 7.0, 1.0, 1.0, 2.0, nan, 900.0),  # said Yes
        ("1013000102", nan, nan, nan, nan, nan, nan, nan, nan, nan),   # plot-less stub
        ("1013000102", nan, nan, 0.0, nan, 2.0, nan, nan, nan, nan),   # plot-less "No"
    ]
    cols = ["HHID", "a3aq1", "a3aq3", "a3aq38", "a3aq39", "a3aq41",
            "a3aq42a", "a3aq42b", "a3aq42c", "a3aq43"]
    return pd.DataFrame(rows, columns=cols)


def test_builder_serves_no_fabricated_zero(uganda_module):
    u = uganda_module
    df = u.plot_labor_for_wave("2009-10", _agsec3a_2009_10(), None,
                               u.LABOR_COLMAPS["2009-10"])
    got = {k: v for k, v in df["PersonDays"].astype("float64").items()}
    hh1, hh2 = "1013000101", "1013000102"
    expected = {
        ("2009-10", hh1, f"{hh1}-1-2", "family", "A"): 12.0,
        ("2009-10", hh1, f"{hh1}-1-2", "hired", "A"): 3.0,
        ("2009-10", hh1, f"{hh1}-1-3", "family", "A"): 0.0,
        ("2009-10", hh1, f"{hh1}-1-3", "hired", "A"): 0.0,
        ("2009-10", hh1, f"{hh1}-1-4", "family", "A"): 7.0,
        ("2009-10", hh1, f"{hh1}-1-4", "hired", "A"): 3.0,
    }
    # The plot-less "No" row is a genuine reported zero and is still served;
    # the two stubs are not.  Since GH #1120 D3 its plot_id is the registered
    # epistemic sentinel 'unknown_plot', no longer NaN.
    assert not any(pd.isna(k[2]) for k in got), "a NaN plot_id key is served"
    nan_plot = {k: v for k, v in got.items() if k[2] == "unknown_plot"}
    clean = {k: v for k, v in got.items() if k[2] != "unknown_plot"}
    assert clean == expected
    assert sorted((k[1], k[3], v) for k, v in nan_plot.items()) == [
        (hh2, "family", 0.0), (hh2, "hired", 0.0)]
    # This fixture has no wage-only hired row (no row with a Wage and no
    # days), so every served row carries days; the wage-only rule is pinned
    # by test_wage_only_hired_rows_are_served below.
    assert df["PersonDays"].notna().all()
    assert df.loc[("2009-10", hh1, f"{hh1}-1-4", "hired", "A"), "Wage"] == 900.0


# ---------------------------------------------------------------------------
# Wage-only hired rows (ruling @ligon 2026-10-23, the #1250 follow-up)
# ---------------------------------------------------------------------------
#
# A hired row with a reported Wage and no reported days is served on its wage,
# with PersonDays NaN -- Malawi's rule (malawi.py:3766-3767, ``| (source ==
# 'hired') & Wage.notna()``).  Family / other rows keep the days-only rule,
# and reported zeros stay (Malawi's ``PersonDays > 0`` half is NOT adopted).

def _agsec3a_2009_10_wages():
    """Synthetic 2009-10 AGSEC3A rows exercising every wage-only class."""
    nan = np.nan
    rows = [
        # HHID,       parcel, plot, fam_n, fam_d, h_flag, man,  wom,  chi,  wage
        ("1013000201", 1.0, 1.0, 2.0, 6.0, 1.0, nan, nan, nan, 5000.0),  # Yes, no days, wage
        ("1013000201", 1.0, 2.0, nan, nan, nan, nan, nan, nan, 6000.0),  # flag NA, no days, wage
        ("1013000201", 1.0, 3.0, nan, nan, 1.0, nan, nan, nan, 0.0),     # Yes, no days, wage 0
        ("1013000201", 1.0, 4.0, nan, nan, 2.0, 4.0, nan, nan, 7000.0),  # No, days, wage
        ("1013000201", 1.0, 5.0, nan, nan, 2.0, nan, nan, nan, 8000.0),  # No, no days, wage
        ("1013000201", 1.0, 6.0, nan, nan, 1.0, nan, nan, nan, nan),     # Yes, nothing
        ("1013000201", 1.0, 7.0, nan, nan, nan, nan, nan, nan, nan),     # stub
        ("1013000201", 1.0, 8.0, nan, nan, 3.0, nan, nan, nan, 9000.0),  # off-scheme flag, wage
    ]
    cols = ["HHID", "a3aq1", "a3aq3", "a3aq38", "a3aq39", "a3aq41",
            "a3aq42a", "a3aq42b", "a3aq42c", "a3aq43"]
    return pd.DataFrame(rows, columns=cols)


def test_wage_only_hired_rows_are_served(uganda_module):
    u = uganda_module
    df = u.plot_labor_for_wave("2009-10", _agsec3a_2009_10_wages(), None,
                               u.LABOR_COLMAPS["2009-10"])
    hh = "1013000201"

    def row(plot, source):
        return df.loc[("2009-10", hh, f"{hh}-1-{plot}", source, "A")]

    served = {(k[2], k[3]) for k in df.index}
    # Served on the wage alone, PersonDays NaN (not 0): Yes, missing flag,
    # a reported zero wage, and an off-scheme flag code.
    for plot, wage in ((1, 5000.0), (2, 6000.0), (3, 0.0), (8, 9000.0)):
        r = row(plot, "hired")
        assert pd.isna(r["PersonDays"]), (plot, r["PersonDays"])
        assert r["Wage"] == wage, (plot, r["Wage"])
    # "No" + wage: the gate zeroes the days and the wage is served beside them
    # (served before this change too; Malawi would serve it as well).
    for plot, wage in ((4, 7000.0), (5, 8000.0)):
        r = row(plot, "hired")
        assert r["PersonDays"] == 0.0 and r["Wage"] == wage, (plot, r.tolist())
    # Neither days nor wage: not served.
    assert (f"{hh}-1-6", "hired") not in served
    assert (f"{hh}-1-7", "hired") not in served
    # The family source keeps the days-only rule: plot 1 has family days, the
    # other plots none, and a hired wage on the row is not a family wage.
    assert row(1, "family")["PersonDays"] == 6.0
    assert {p for p, s in served if s == "family"} == {f"{hh}-1-1"}
    # Every NaN-PersonDays row is a hired row with a reported wage.
    nan_days = df[df["PersonDays"].isna()]
    assert len(nan_days) == 4
    assert set(nan_days.index.get_level_values("source")) == {"hired"}
    assert nan_days["Wage"].notna().all()


# ---------------------------------------------------------------------------
# data-gated: every served zero has a "No" or a reported zero behind it
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("t", ["2009-10", "2010-11"])
def test_every_served_zero_is_justified_by_the_source(uganda_module, t):
    if not _aws_creds_available():
        pytest.skip("S3 credentials unavailable; cannot read raw AGSEC3A/3B")
    from lsms_library.local_tools import format_id, get_dataframe

    u = uganda_module
    wave = countries_root() / "Uganda" / t / "Data"
    raw = {}
    for season, fn in (("A", "AGSEC3A.dta"), ("B", "AGSEC3B.dta")):
        raw[season] = get_dataframe(str(wave / fn), convert_categoricals=False)
    served = u.plot_labor_for_wave(t, raw["A"], raw["B"], u.LABOR_COLMAPS[t])

    justified = set()
    for season, df in raw.items():
        cm = u.LABOR_COLMAPS[t][season]
        hh = u._format_agsec_hhid(df[cm["hhid"]], t)
        pid = (hh.astype(str) + "-" + df[cm["parcel"]].apply(format_id).astype(str)
               + "-" + df[cm["plot"]].apply(format_id).astype(str))
        for source, flag_col, code in (("family", cm["family_flag"], 0),
                                       ("hired", cm["hired_flag"], 2)):
            cells = df[cm[source]].apply(lambda s: pd.to_numeric(s, errors="coerce"))
            said_no = pd.to_numeric(df[flag_col], errors="coerce") == code
            zero_days = cells.sum(axis=1, min_count=1) == 0
            ok = said_no | zero_days
            justified |= {(t, i, None if pd.isna(p) else p, source, season)
                          for i, p in zip(hh[ok], pid[ok])}

    zeros = served[served["PersonDays"] == 0].index
    unjustified = [k for k in zeros
                   if (k[0], k[1], None if pd.isna(k[2]) else k[2], k[3], k[4])
                   not in justified]
    assert len(zeros) > 1000, "expected thousands of genuine 'No' zeros"
    assert unjustified == [], (
        f"{len(unjustified)} served zero(s) in {t} have neither a 'No' flag nor "
        f"reported zero days behind them (development: 4,504 in 2009-10, "
        f"569 in 2010-11)")


# ---------------------------------------------------------------------------
# data-gated: every reported hired wage is served (the #1250 follow-up)
# ---------------------------------------------------------------------------

_RAW_FILES = {"2019-20": ("Data/Agric/agsec3a.dta", "Data/Agric/agsec3b.dta")}
# Hired rows served on a wage alone (PersonDays NaN), per wave, measured by the
# raw oracle on 2026-10-23 (slurm_logs/.../1250-uganda-labor-flag/r2/): 78 with
# a "Yes" flag and 7 with a missing flag, all in 2009-10 and 2010-11.  At
# 90fc1e18d none of them was served.
_WAGE_ONLY_ROWS = {"2009-10": 67, "2010-11": 18, "2011-12": 0, "2013-14": 0,
                   "2015-16": 0, "2018-19": 0, "2019-20": 0}


@pytest.mark.parametrize("t", sorted(_WAGE_ONLY_ROWS))
def test_every_reported_hired_wage_is_served(uganda_module, t):
    if not _aws_creds_available():
        pytest.skip("S3 credentials unavailable; cannot read raw AGSEC3A/3B")
    from lsms_library.local_tools import format_id, get_dataframe

    u = uganda_module
    colmap = u.LABOR_COLMAPS[t]
    files = dict(zip("AB", _RAW_FILES.get(t, ("Data/AGSEC3A.dta", "Data/AGSEC3B.dta"))))
    raw = {s: get_dataframe(str(countries_root() / "Uganda" / t / files[s]),
                            convert_categoricals=False)
           for s in colmap}
    served = u.plot_labor_for_wave(t, raw.get("A"), raw.get("B"), colmap)
    hired = served.xs("hired", level="source")

    def norm(p):
        return None if pd.isna(p) else p

    got_wage = {(k[1], norm(k[2]), k[3]): w for k, w in hired["Wage"].items()}
    got_nan_days = {(k[1], norm(k[2]), k[3])
                    for k in hired.index[hired["PersonDays"].isna()]}

    # Independent of _labor_persondays: raw float flag, raw day cells, raw wage.
    expected_wage, raw_rows = {}, {}
    for season, df in raw.items():
        cm = colmap[season]
        hh = u._format_agsec_hhid(df[cm["hhid"]], t)
        pid = (hh.astype(str) + "-" + df[cm["parcel"]].apply(format_id).astype(str)
               + "-" + df[cm["plot"]].apply(format_id).astype(str))
        wage = pd.to_numeric(df[cm["wage"]], errors="coerce")
        flag = pd.to_numeric(df[cm["hired_flag"]], errors="coerce")
        days = df[cm["hired"]].apply(lambda s: pd.to_numeric(s, errors="coerce"))
        for i, p, w, f, has_days in zip(hh, pid, wage, flag,
                                        days.notna().any(axis=1)):
            key = (i, norm(p), season)
            raw_rows.setdefault(key, []).append((f, bool(has_days), w))
            if pd.notna(w):
                expected_wage[key] = max(w, expected_wage.get(key, -np.inf))

    missing = [k for k in expected_wage if k not in got_wage]
    by_season = pd.Series([k[2] for k in missing], dtype=object).value_counts().to_dict()
    assert not missing, (
        f"{len(missing)} hired row(s) in {t} carry a reported Wage but are not "
        f"served, by season {by_season} (90fc1e18d: 67 in 2009-10, 18 in 2010-11)")
    wrong = [k for k, w in expected_wage.items() if got_wage[k] != w]
    assert wrong == [], f"{len(wrong)} served hired Wage(s) differ from the raw max"
    # A NaN-days hired row is served ONLY on a wage: every raw row of its key
    # has no day cell and no "No" flag (a "No" would have served 0 days), and
    # at least one carries a wage.
    assert len(got_nan_days) == _WAGE_ONLY_ROWS[t]
    bad = [k for k in got_nan_days
           if any(has_days for _, has_days, _ in raw_rows[k])
           or any(f == 2 for f, _, _ in raw_rows[k])
           or not any(pd.notna(w) for _, _, w in raw_rows[k])]
    assert not bad, (f"{len(bad)} NaN-days hired row(s) in {t} are not "
                     f"wage-only in the raw source")
