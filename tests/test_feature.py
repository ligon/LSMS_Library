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


# ---------------------------------------------------------------------------
# _harmonize_country_frame: missing-level fabrication opt-in (GH #506)
# ---------------------------------------------------------------------------

import pandas as pd
from lsms_library.feature import _harmonize_country_frame, _fabricates_missing_levels


def _reduced_frame():
    """A reduced (t,v,i) frame vs a canonical (t,v,i,visit) target."""
    idx = pd.MultiIndex.from_tuples([('2020', 'c1', 'h1'), ('2020', 'c1', 'h2')],
                                    names=['t', 'v', 'i'])
    return pd.DataFrame({'Int_t': ['2020-01-01', '2020-02-01']}, index=idx)


class TestFabricateMissingLevels:
    def test_optout_leaves_reduced_frame_untouched(self):
        """Default (no opt-in): a missing canonical level is NOT fabricated."""
        df = _harmonize_country_frame(_reduced_frame(), ['t', 'v', 'i', 'visit'],
                                      'X', 'sometable', fabricate_missing=False)
        assert list(df.index.names) == ['t', 'v', 'i']

    def test_optin_fabricates_missing_level_as_na(self):
        """Opt-in (#506): the missing 'visit' level is added as a pd.NA level,
        in canonical order, so the frame shares the full canonical shape."""
        df = _harmonize_country_frame(_reduced_frame(), ['t', 'v', 'i', 'visit'],
                                      'X', 'interview_date', fabricate_missing=True)
        assert list(df.index.names) == ['t', 'v', 'i', 'visit']
        assert df.index.get_level_values('visit').isna().all()
        assert len(df) == 2  # no rows lost

    def test_optin_preserves_existing_level(self):
        """A frame that already has the per-visit level is left intact (ordered)."""
        idx = pd.MultiIndex.from_tuples([('2020', 'c1', 'h1', '1'), ('2020', 'c1', 'h1', '2')],
                                        names=['t', 'v', 'i', 'visit'])
        full = pd.DataFrame({'Int_t': ['a', 'b']}, index=idx)
        df = _harmonize_country_frame(full, ['t', 'v', 'i', 'visit'],
                                      'Y', 'interview_date', fabricate_missing=True)
        assert list(df.index.names) == ['t', 'v', 'i', 'visit']
        assert df.index.get_level_values('visit').tolist() == ['1', '2']

    def test_interview_date_opts_in(self):
        """interview_date is registered for fabrication; a non-listed table is not."""
        assert _fabricates_missing_levels('interview_date') is True
        assert _fabricates_missing_levels('assets') is False
