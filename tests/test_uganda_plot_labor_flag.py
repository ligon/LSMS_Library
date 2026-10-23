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
    # The plot-less "No" row is a genuine reported zero and is still served
    # (its NaN plot_id is GH #1120 D3, unchanged here); the two stubs are not.
    nan_plot = {k: v for k, v in got.items() if pd.isna(k[2])}
    clean = {k: v for k, v in got.items() if not pd.isna(k[2])}
    assert clean == expected
    assert sorted((k[1], k[3], v) for k, v in nan_plot.items()) == [
        (hh2, "family", 0.0), (hh2, "hired", 0.0)]
    assert df["PersonDays"].notna().all()
    assert df.loc[("2009-10", hh1, f"{hh1}-1-4", "hired", "A"), "Wage"] == 900.0


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
