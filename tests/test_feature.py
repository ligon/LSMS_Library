"""Tests for the cross-country Feature class."""

import pytest
import yaml
from pathlib import Path

from lsms_library.feature import Feature, _discover_countries_for_table, _load_global_columns


# ---------------------------------------------------------------------------
# Discovery and metadata
# ---------------------------------------------------------------------------

class TestFeatureDiscovery:
    """Feature discovers countries and columns without loading data."""

    def test_countries_nonempty(self):
        """household_roster is declared by at least one country."""
        f = Feature("household_roster")
        assert len(f.countries) > 0

    def test_countries_returns_strings(self):
        f = Feature("household_roster")
        assert all(isinstance(c, str) for c in f.countries)

    def test_countries_sorted(self):
        f = Feature("household_roster")
        assert f.countries == sorted(f.countries)

    def test_unknown_table_empty(self):
        """A table nobody declares yields an empty country list."""
        f = Feature("nonexistent_table_xyz")
        assert f.countries == []

    def test_columns_household_roster(self):
        """Required columns for household_roster match data_info.yml."""
        f = Feature("household_roster")
        cols = f.columns
        for expected in ["Sex", "Age", "Generation", "Distance", "Affinity"]:
            assert expected in cols, f"{expected} missing from Feature.columns"

    def test_columns_unknown_table(self):
        """A table with no Columns entry returns an empty list."""
        f = Feature("nonexistent_table_xyz")
        assert f.columns == []

    def test_repr(self):
        f = Feature("household_roster")
        assert repr(f) == "Feature('household_roster')"

    def test_lazy_discovery(self):
        """Countries are not discovered until .countries is accessed."""
        f = Feature("household_roster")
        assert f._countries is None
        _ = f.countries
        assert f._countries is not None


# ---------------------------------------------------------------------------
# Cross-check: every table in data_info.yml Columns is declared by ≥1 country
# ---------------------------------------------------------------------------

_DATA_INFO_PATH = Path(__file__).resolve().parent.parent / "lsms_library" / "data_info.yml"
with open(_DATA_INFO_PATH, "r", encoding="utf-8") as _f:
    _CANONICAL = yaml.safe_load(_f)

_COLUMNS_TABLES = list(_CANONICAL.get("Columns", {}).keys())


@pytest.mark.parametrize("table", _COLUMNS_TABLES)
def test_column_table_has_countries(table):
    """Every table with canonical columns should be declared by ≥1 country."""
    countries = _discover_countries_for_table(table)
    assert len(countries) > 0, f"No countries declare '{table}' in data_scheme.yml"
