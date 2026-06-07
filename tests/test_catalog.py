"""Tests for the top-level catalog helpers ll.countries() / ll.features()."""
from __future__ import annotations

import pytest

import lsms_library as ll
from lsms_library.country import JSON_CACHE_METHODS


def test_exported_at_top_level():
    assert callable(ll.countries)
    assert callable(ll.features)


def test_countries_basic():
    cs = ll.countries()
    assert isinstance(cs, list) and cs == sorted(cs)
    assert len(cs) > 20
    # multi-word names survive intact (the shell-glob pitfall)
    assert "South Africa" in cs
    assert not any(c in {"South", "Africa", "Republic"} for c in cs)


def test_countries_are_country_able():
    for c in ll.countries():
        # construction must not raise (config-valid); data access is separate
        ll.Country(c, preload_panel_ids=False)


def test_features_basic():
    fs = ll.features()
    assert isinstance(fs, list) and fs == sorted(fs)
    assert "food_acquired" in fs
    # derived features are listed
    assert "household_characteristics" in fs
    assert {"food_expenditures", "food_prices", "food_quantities"} <= set(fs)
    # dict-valued properties are NOT Feature-able and must be excluded
    for nf in JSON_CACHE_METHODS:
        assert nf not in fs


def test_features_are_feature_able():
    for f in ll.features():
        ll.Feature(f)  # must construct


def test_countries_filtered_by_feature():
    fa = ll.countries(feature="food_acquired")
    assert "Uganda" in fa and "Malawi" in fa
    assert set(fa) <= set(ll.countries())
    # derived feature resolves via its source table
    assert ll.countries(feature="food_quantities") == fa


def test_features_filtered_by_country():
    uga = ll.features(country="Uganda")
    assert "food_acquired" in uga
    assert "household_characteristics" in uga
    assert set(uga) <= set(ll.features())
    assert "panel_ids" not in uga


def test_bad_feature_raises():
    with pytest.raises(ValueError):
        ll.countries(feature="not_a_feature")


def test_bad_country_raises():
    with pytest.raises(ValueError):
        ll.features(country="Xanadu")
