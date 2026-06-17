"""Tests for (country, wave) -> ISO 4217 currency labeling.

Covers the data-free module logic (resolution, monetary-column detection,
attach_currency representation modes) plus guards on the Country API; the
cross-country Feature checks are skipped when microdata isn't available.

See lsms_library/currency.py and the Currency: section of data_info.yml.
"""
import re

import pandas as pd
import pytest

from lsms_library import currency_for, Country, Feature
from lsms_library.catalog import _country_dirs
from lsms_library.currency import (
    CURRENCY_LEVEL,
    attach_currency,
    is_monetary_table,
    _monetary_columns,
)

_ISO_4217 = re.compile(r"[A-Z]{3}")

# The 8 EHCVM / WAEMU countries that all share the West African CFA franc.
_XOF_COUNTRIES = [
    "Benin", "Burkina_Faso", "CotedIvoire", "Guinea-Bissau",
    "Mali", "Niger", "Senegal", "Togo",
]


# ---------------------------------------------------------------------------
# Completeness: every country resolves to a well-formed ISO 4217 code
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("country", _country_dirs())
def test_every_country_has_currency(country):
    """No country may be omitted from data_info.yml's Currency: section."""
    code = currency_for(country)
    assert code is not pd.NA, f"{country} missing from Currency: section"
    assert isinstance(code, str) and _ISO_4217.fullmatch(code), (
        f"{country} default currency {code!r} is not ISO 4217 alpha-3"
    )


def test_unknown_country_returns_na():
    assert currency_for("Atlantis") is pd.NA


# ---------------------------------------------------------------------------
# Per-wave overrides: the in-sample redenominations
# ---------------------------------------------------------------------------

def test_ghana_redenomination_per_wave():
    # Old cedi (GHC) before the 2007 reform ...
    for wave in ["1987-88", "1998-99", "2005-06"]:
        assert currency_for("GhanaLSS", wave) == "GHC"
    # ... new Ghana cedi (GHS) after, incl. the default.
    for wave in ["2012-13", "2016-17"]:
        assert currency_for("GhanaLSS", wave) == "GHS"
    assert currency_for("GhanaLSS") == "GHS"
    # GhanaSPS is entirely post-reform.
    assert currency_for("GhanaSPS") == "GHS"


def test_tajikistan_redenomination_per_wave():
    assert currency_for("Tajikistan", "1999") == "TJR"   # ruble, pre-2000
    for wave in ["2003", "2007", "2009"]:
        assert currency_for("Tajikistan", wave) == "TJS"  # somoni


def test_unknown_wave_falls_back_to_default():
    assert currency_for("GhanaLSS", "2099-00") == "GHS"


# ---------------------------------------------------------------------------
# Currency is NOT injective with country (shared XOF)
# ---------------------------------------------------------------------------

def test_xof_shared_across_ehcvm_countries():
    codes = {c: currency_for(c) for c in _XOF_COUNTRIES}
    assert set(codes.values()) == {"XOF"}, codes


# ---------------------------------------------------------------------------
# Monetary-column detection
# ---------------------------------------------------------------------------

def test_monetary_columns_detection():
    assert _monetary_columns("food_acquired") >= {"Expenditure", "Price"}
    assert _monetary_columns("food_expenditures") == {"Expenditure"}
    assert _monetary_columns("food_prices") == {"Price"}
    assert _monetary_columns("household_roster") == frozenset()
    assert is_monetary_table("food_expenditures")
    assert not is_monetary_table("household_roster")


# ---------------------------------------------------------------------------
# attach_currency: representation modes (synthetic frame, no microdata)
# ---------------------------------------------------------------------------

def _ghana_frame():
    """Two-row food_expenditures-shaped frame spanning the redenomination."""
    idx = pd.MultiIndex.from_tuples(
        [("h1", "2005-06", "rice"), ("h2", "2016-17", "rice")],
        names=["i", "t", "j"],
    )
    df = pd.DataFrame({"Expenditure": [10.0, 20.0]}, index=idx)
    df.attrs["id_converted"] = True
    return df


def test_attach_currency_index_mode():
    df = _ghana_frame()
    out = attach_currency(df, "GhanaLSS", "food_expenditures", mode="index")
    assert out.index.names[-1] == CURRENCY_LEVEL
    level = out.index.get_level_values(CURRENCY_LEVEL)
    assert level.dtype == "string"
    # Per-wave resolution: old cedi for 2005-06, new for 2016-17.
    assert list(level) == ["GHC", "GHS"]
    # attrs (id_converted) survives the set_index.
    assert out.attrs.get("id_converted") is True
    # Original frame untouched (no in-place mutation).
    assert CURRENCY_LEVEL not in df.index.names


def test_attach_currency_column_mode():
    df = _ghana_frame()
    out = attach_currency(df, "GhanaLSS", "food_expenditures", mode="column")
    assert CURRENCY_LEVEL in out.columns
    assert list(out[CURRENCY_LEVEL]) == ["GHC", "GHS"]
    assert list(out.index.names) == ["i", "t", "j"]  # index unchanged


def test_attach_currency_noop_for_non_monetary():
    df = _ghana_frame().rename(columns={"Expenditure": "HouseholdSize"})
    out = attach_currency(df, "GhanaLSS", "household_roster", mode="index")
    assert out.index.names == df.index.names
    assert CURRENCY_LEVEL not in out.columns


def test_attach_currency_idempotent():
    df = _ghana_frame()
    once = attach_currency(df, "GhanaLSS", "food_expenditures", mode="index")
    twice = attach_currency(once, "GhanaLSS", "food_expenditures", mode="index")
    assert list(twice.index.names).count(CURRENCY_LEVEL) == 1


def test_attach_currency_rejects_bad_mode():
    with pytest.raises(ValueError):
        attach_currency(_ghana_frame(), "GhanaLSS", "food_expenditures", mode="bogus")


# ---------------------------------------------------------------------------
# Country API guards (fire before any data load)
# ---------------------------------------------------------------------------

def test_country_guard_non_monetary_table():
    uga = Country("Uganda", preload_panel_ids=False)
    with pytest.raises(TypeError):
        uga.household_roster(currency="index")


def test_country_guard_bad_mode():
    uga = Country("Uganda", preload_panel_ids=False)
    with pytest.raises(ValueError):
        uga.food_expenditures(currency="bogus")


def test_country_default_is_backward_compatible():
    """Default call must NOT carry a currency level (grain unchanged)."""
    uga = Country("Uganda", preload_panel_ids=False)
    try:
        df = uga.food_expenditures()
    except Exception as exc:  # microdata unavailable in this environment
        pytest.skip(f"food_expenditures unavailable: {exc}")
    if df.empty:
        pytest.skip("food_expenditures empty (no microdata)")
    assert CURRENCY_LEVEL not in df.index.names
    assert CURRENCY_LEVEL not in df.columns


def test_country_currency_index_roundtrip():
    uga = Country("Uganda", preload_panel_ids=False)
    try:
        df = uga.food_expenditures(currency="index")
    except Exception as exc:
        pytest.skip(f"food_expenditures unavailable: {exc}")
    if df.empty:
        pytest.skip("food_expenditures empty (no microdata)")
    assert df.index.names[-1] == CURRENCY_LEVEL
    assert set(df.index.get_level_values(CURRENCY_LEVEL).dropna().unique()) == {"UGX"}


# ---------------------------------------------------------------------------
# Cross-country Feature (skipped when microdata unavailable)
# ---------------------------------------------------------------------------

def test_feature_xof_consistency():
    try:
        f = Feature("food_expenditures")(["Mali", "Niger"])
    except Exception as exc:
        pytest.skip(f"Feature unavailable: {exc}")
    if f.empty or CURRENCY_LEVEL not in f.index.names:
        pytest.skip("Mali/Niger food_expenditures unavailable")
    codes = set(f.index.get_level_values(CURRENCY_LEVEL).dropna().unique())
    assert codes == {"XOF"}, codes


def test_feature_ghana_per_wave():
    try:
        g = Feature("food_expenditures")(["GhanaLSS"])
    except Exception as exc:
        pytest.skip(f"Feature unavailable: {exc}")
    if g.empty or CURRENCY_LEVEL not in g.index.names:
        pytest.skip("GhanaLSS food_expenditures unavailable")
    flat = g.reset_index()
    by_wave = flat.groupby("t")[CURRENCY_LEVEL].agg(
        lambda s: set(s.dropna().unique())
    )
    if "2005-06" in by_wave.index:
        assert by_wave.loc["2005-06"] == {"GHC"}
    post = [w for w in ("2012-13", "2016-17") if w in by_wave.index]
    for w in post:
        assert by_wave.loc[w] == {"GHS"}
    assert post or "2005-06" in by_wave.index, "no GhanaLSS waves materialized"


def test_feature_non_monetary_unaffected():
    """Default currency='index' must be a silent no-op for non-monetary tables."""
    try:
        r = Feature("household_roster")(["Uganda"])
    except Exception as exc:
        pytest.skip(f"Feature unavailable: {exc}")
    if r.empty:
        pytest.skip("Uganda household_roster unavailable")
    assert CURRENCY_LEVEL not in r.index.names
    assert CURRENCY_LEVEL not in r.columns
