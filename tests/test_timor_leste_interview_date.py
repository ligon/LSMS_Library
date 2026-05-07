"""Unit tests for Timor-Leste interview_date formatters (#207).

Covers the per-wave ``Int_t`` formatters in
``Timor-Leste/{wave}/_/mapping.py`` without invoking the full
framework pipeline (which requires DVC pulls and is slow on CI).
The integration check (``Country('Timor-Leste').interview_date()``
returning sane datetimes) is exercised separately when the parquet
caches are warm.
"""
import importlib.util
from pathlib import Path

import numpy as np
import pandas as pd
import pytest


REPO = Path(__file__).resolve().parents[1]
TL_2001 = REPO / "lsms_library/countries/Timor-Leste/2001/_/mapping.py"
TL_2007 = REPO / "lsms_library/countries/Timor-Leste/2007-08/_/mapping.py"


def _load_module(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def m2001():
    return _load_module(TL_2001, "tl_2001_mapping")


@pytest.fixture(scope="module")
def m2007():
    return _load_module(TL_2007, "tl_2007_mapping")


# -- 2001: DDMMYY integer parser ---------------------------------------

def test_2001_int_t_full_six_digit(m2001):
    assert m2001.Int_t(260901) == pd.Timestamp("2001-09-26").date()


def test_2001_int_t_five_digit_zero_padded(m2001):
    """``50101`` → DD=05, MM=01, YY=01 → 2001-01-05."""
    assert m2001.Int_t(50101) == pd.Timestamp("2001-01-05").date()


def test_2001_int_t_nan(m2001):
    assert m2001.Int_t(np.nan) is pd.NaT


def test_2001_int_t_string_input(m2001):
    """Stata can deliver int columns as strings under some conversions —
    we still want a sensible answer."""
    assert m2001.Int_t("260901") == pd.Timestamp("2001-09-26").date()


def test_2001_int_t_invalid_returns_nat(m2001):
    """Non-numeric / out-of-range input drops to NaT rather than raising."""
    assert m2001.Int_t("nonsense") is pd.NaT


def test_2001_int_t_two_digit_year_century_handling(m2001):
    """Two-digit years <=30 → 20xx; >=31 → 19xx (defensive only — the
    2001 wave never sees yy outside {01, 02})."""
    assert m2001.Int_t(101095) == pd.Timestamp("1995-10-10").date()
    assert m2001.Int_t(101015) == pd.Timestamp("2015-10-10").date()


# -- 2007-08: three-component combiner ---------------------------------

def test_2007_int_t_basic(m2007):
    assert m2007.Int_t(pd.Series([10, 7, 2007])) == pd.Timestamp("2007-07-10").date()


def test_2007_int_t_nan_day(m2007):
    assert m2007.Int_t(pd.Series([np.nan, 7, 2007])) is pd.NaT


def test_2007_int_t_invalid_month_returns_nat(m2007):
    """Month 13 is invalid; pd.Timestamp raises ValueError → NaT."""
    assert m2007.Int_t(pd.Series([10, 13, 2007])) is pd.NaT


def test_2007_int_t_2008_year(m2007):
    assert m2007.Int_t(pd.Series([15, 3, 2008])) == pd.Timestamp("2008-03-15").date()


def test_2007_int_t_string_components(m2007):
    """Stata sometimes delivers integer columns as string-of-int."""
    assert m2007.Int_t(pd.Series(["10", "7", "2007"])) == pd.Timestamp("2007-07-10").date()
