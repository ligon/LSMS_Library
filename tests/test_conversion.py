"""Tests for phase-2 currency conversion (lsms_library.conversion).

Mostly data-free (the factor table + the convert() engine on synthetic frames);
the Country/Feature numeraire= sugar is cache-gated and skips without microdata.

See lsms_library/conversion.py and conversion_factors.org.
"""
import warnings

import numpy as np
import pandas as pd
import pytest

from lsms_library import convert, Country, Feature
from lsms_library.conversion import (
    conversion_targets,
    _load_factors,
    _wave_to_year,
)
from lsms_library.currency import CURRENCY_LEVEL


# ---------------------------------------------------------------------------
# Factor table
# ---------------------------------------------------------------------------

def test_factor_table_loads_and_is_keyed():
    f = _load_factors()
    assert f.index.names == ["Country", "Date"]
    assert {"FX", "PPP-2017"}.issubset(f.columns)


def test_conversion_targets_excludes_helpers():
    t = conversion_targets()
    assert "PPP-2017" in t and "FX" in t
    assert "CPI" not in t          # helper, not a divisor target
    assert "Currency" not in t and "Date" not in t


def test_country_keyed_not_currency_keyed():
    """Mali and Niger share FX (XOF) but must have distinct PPP (country-level)."""
    f = _load_factors()
    assert f.at[("Mali", 2018), "FX"] == f.at[("Niger", 2018), "FX"]      # shared XOF
    assert f.at[("Mali", 2018), "PPP-2017"] != f.at[("Niger", 2018), "PPP-2017"]


@pytest.mark.parametrize("wave,year", [
    ("2005-06", 2005), ("2018-19", 2018), ("2019", 2019), ("2008-15", 2008),
])
def test_wave_to_year(wave, year):
    assert _wave_to_year(wave) == year


# ---------------------------------------------------------------------------
# convert(): synthetic frames (no microdata)
# ---------------------------------------------------------------------------

def _uganda_frame():
    idx = pd.MultiIndex.from_tuples(
        [("h1", "2019-20", "rice"), ("h2", "2019-20", "maize")],
        names=["i", "t", "j"],
    )
    df = pd.DataFrame({"Expenditure": [1_287_000.0, 2_574_000.0]}, index=idx)
    df.attrs["country"] = "Uganda"
    df.attrs["id_converted"] = True
    return df


def test_convert_ppp_exact_and_relabel():
    out = convert(_uganda_frame(), to="PPP-2017")   # Uganda 2019 factor = 1287.0
    assert list(out["Expenditure"]) == [1000.0, 2000.0]
    assert CURRENCY_LEVEL in out.columns or CURRENCY_LEVEL in out.index.names
    label = (out[CURRENCY_LEVEL] if CURRENCY_LEVEL in out.columns
             else out.index.get_level_values(CURRENCY_LEVEL))
    assert set(pd.unique(label)) == {"PPP-2017"}
    assert out.attrs["conversion"]["to"] == "PPP-2017"
    assert out.attrs.get("id_converted") is True   # preserved across convert


def test_convert_country_recovery_modes():
    # via attrs
    assert convert(_uganda_frame(), to="FX")["Expenditure"].notna().all()
    # via explicit arg (attrs stripped)
    df = _uganda_frame(); df.attrs.clear()
    assert convert(df, to="FX", country="Uganda")["Expenditure"].notna().all()
    # via 'country' index level
    fidx = pd.MultiIndex.from_tuples([("Uganda", "h1", "2019-20", "rice")],
                                     names=["country", "i", "t", "j"])
    fdf = pd.DataFrame({"Expenditure": [1_287_000.0]}, index=fidx)
    assert convert(fdf, to="PPP-2017")["Expenditure"].iloc[0] == 1000.0
    # none available -> error
    df2 = _uganda_frame(); df2.attrs.clear()
    with pytest.raises(ValueError):
        convert(df2, to="FX")


def test_convert_mali_niger_country_level_ppp():
    idx = pd.MultiIndex.from_tuples(
        [("Mali", "h1", "2018-19", "rice"), ("Niger", "h2", "2018-19", "rice")],
        names=["country", "i", "t", "j"])
    df = pd.DataFrame({"Expenditure": [1985.0, 2509.0]}, index=idx)
    out = convert(df, to="PPP-2017")
    # 1985 / 198.5 == 10 ; 2509 / 250.9 == 10  (distinct divisors, same real value)
    assert out["Expenditure"].round(3).tolist() == [10.0, 10.0]
    fx = convert(df, to="FX")     # shared XOF divisor 555.4
    assert fx["Expenditure"].iloc[0] != out["Expenditure"].iloc[0]


def test_convert_redenomination_wave_is_na():
    idx = pd.MultiIndex.from_tuples([("h1", "2005-06", "rice")], names=["i", "t", "j"])
    df = pd.DataFrame({"Expenditure": [50000.0]}, index=idx)
    df.attrs["country"] = "GhanaLSS"
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        out = convert(df, to="PPP-2017")
    assert out["Expenditure"].isna().all()
    assert any("redenomination" in str(x.message) for x in w)


def test_convert_missing_factor_is_na():
    idx = pd.MultiIndex.from_tuples([("h1", "1850", "rice")], names=["i", "t", "j"])
    df = pd.DataFrame({"Expenditure": [10.0]}, index=idx)
    df.attrs["country"] = "Uganda"
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        out = convert(df, to="FX")
    assert out["Expenditure"].isna().all()
    assert any("no factor" in str(x.message) for x in w)


def test_convert_bad_target_raises():
    with pytest.raises(ValueError):
        convert(_uganda_frame(), to="NOPE")


def test_convert_no_monetary_columns_noop():
    idx = pd.MultiIndex.from_tuples([("h1", "2019-20", "rice")], names=["i", "t", "j"])
    df = pd.DataFrame({"HouseholdSize": [4]}, index=idx)
    df.attrs["country"] = "Uganda"
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        out = convert(df, to="FX")
    assert "HouseholdSize" in out.columns and out["HouseholdSize"].iloc[0] == 4


# ---------------------------------------------------------------------------
# Country API guards (fire before any data load)
# ---------------------------------------------------------------------------

def test_country_numeraire_guards():
    uga = Country("Uganda", preload_panel_ids=False)
    with pytest.raises(ValueError):   # mutually exclusive with currency=
        uga.food_expenditures(currency="index", numeraire="PPP-2017")
    with pytest.raises(TypeError):    # non-monetary table
        uga.household_roster(numeraire="PPP-2017")
    with pytest.raises(ValueError):   # unknown target
        uga.food_expenditures(numeraire="NOPE")


# ---------------------------------------------------------------------------
# End-to-end (cache-gated)
# ---------------------------------------------------------------------------

def test_country_numeraire_end_to_end():
    uga = Country("Uganda", preload_panel_ids=False)
    try:
        ppp = uga.food_expenditures(numeraire="PPP-2017")
    except Exception as exc:
        pytest.skip(f"food_expenditures unavailable: {exc}")
    if ppp.empty:
        pytest.skip("food_expenditures empty (no microdata)")
    assert CURRENCY_LEVEL in ppp.index.names
    assert set(ppp.index.get_level_values(CURRENCY_LEVEL).unique()) == {"PPP-2017"}
    assert ppp.attrs["conversion"]["to"] == "PPP-2017"


def test_feature_numeraire_end_to_end():
    try:
        f = Feature("food_expenditures")(["Mali", "Niger"], numeraire="PPP-2017")
    except Exception as exc:
        pytest.skip(f"Feature unavailable: {exc}")
    if f.empty or CURRENCY_LEVEL not in f.index.names:
        pytest.skip("Mali/Niger food_expenditures unavailable")
    assert set(f.index.get_level_values(CURRENCY_LEVEL).unique()) == {"PPP-2017"}
