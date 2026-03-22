"""
Cross-country schema consistency tests.

Validates that data_scheme.yml files across all countries follow shared
conventions: consistent column naming, required columns for key tables,
parsability, and no silent duplicate keys.
"""

import re
from pathlib import Path

import pytest
import yaml

from lsms_library.paths import COUNTRIES_ROOT
from lsms_library.yaml_utils import load_yaml


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _all_data_scheme_paths() -> list[Path]:
    """Return sorted list of all data_scheme.yml paths."""
    return sorted(COUNTRIES_ROOT.glob("*/_/data_scheme.yml"))


def _schemes_with_household_roster() -> list[tuple[str, Path, dict]]:
    """Return (country, path, roster_spec) for every scheme that declares household_roster."""
    results = []
    for yml in _all_data_scheme_paths():
        country = yml.parent.parent.name
        data = load_yaml(yml)
        if not isinstance(data, dict):
            continue
        ds = data.get("Data Scheme")
        if not isinstance(ds, dict):
            continue
        roster = ds.get("household_roster")
        if roster is not None and isinstance(roster, dict):
            results.append((country, yml, roster))
    return results


ROSTER_SCHEMES = _schemes_with_household_roster()
ALL_SCHEME_PATHS = _all_data_scheme_paths()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestHouseholdRosterNaming:
    """Ensure household_roster uses 'Relationship', never 'Relation'."""

    @pytest.mark.parametrize(
        "country,path,roster",
        ROSTER_SCHEMES,
        ids=[c for c, _, _ in ROSTER_SCHEMES],
    )
    def test_household_roster_uses_relationship(self, country, path, roster):
        """household_roster should use 'Relationship', not 'Relation'."""
        columns = set(roster.keys()) - {"index", "materialize", "backend"}
        assert "Relation" not in columns, (
            f"{country}: household_roster uses 'Relation' instead of "
            f"'Relationship'. Rename to 'Relationship' in {path}"
        )


class TestHouseholdRosterRequiredColumns:
    """Ensure household_roster declares the minimum required columns."""

    REQUIRED = {"Sex", "Age", "Relationship"}

    @pytest.mark.parametrize(
        "country,path,roster",
        ROSTER_SCHEMES,
        ids=[c for c, _, _ in ROSTER_SCHEMES],
    )
    def test_household_roster_has_required_columns(self, country, path, roster):
        """household_roster must declare Sex, Age, and Relationship columns."""
        columns = set(roster.keys()) - {"index", "materialize", "backend"}
        missing = self.REQUIRED - columns
        assert not missing, (
            f"{country}: household_roster is missing required columns "
            f"{sorted(missing)} in {path}"
        )


class TestDataSchemeParsing:
    """Verify every data_scheme.yml parses without errors."""

    @pytest.mark.parametrize(
        "path",
        ALL_SCHEME_PATHS,
        ids=[p.parent.parent.name for p in ALL_SCHEME_PATHS],
    )
    def test_all_data_scheme_files_parse(self, path):
        """data_scheme.yml should load without raising exceptions."""
        data = load_yaml(path)
        assert isinstance(data, dict), (
            f"{path}: expected a dict at top level, got {type(data).__name__}"
        )
        assert "Country" in data, f"{path}: missing 'Country' key"
        assert "Data Scheme" in data, f"{path}: missing 'Data Scheme' key"


class TestNoDuplicateYamlKeys:
    """Detect duplicate top-level keys under 'Data Scheme'.

    YAML silently drops duplicates, keeping only the last value. This test
    reads the raw text to catch such problems before they cause silent
    data loss.
    """

    # Matches top-level keys under Data Scheme (exactly 2-space indented lines
    # that end with a colon or have a colon followed by content).
    _KEY_RE = re.compile(r"^  (\w[\w_]*):", re.MULTILINE)

    @pytest.mark.parametrize(
        "path",
        ALL_SCHEME_PATHS,
        ids=[p.parent.parent.name for p in ALL_SCHEME_PATHS],
    )
    def test_no_duplicate_yaml_keys(self, path):
        """No duplicate top-level keys under 'Data Scheme'."""
        text = path.read_text(encoding="utf-8")

        # Extract the section after "Data Scheme:"
        marker = "Data Scheme:"
        idx = text.find(marker)
        if idx == -1:
            pytest.skip(f"{path}: no 'Data Scheme:' section found")

        section = text[idx + len(marker):]
        keys = self._KEY_RE.findall(section)

        seen: dict[str, int] = {}
        duplicates: list[str] = []
        for key in keys:
            seen[key] = seen.get(key, 0) + 1
            if seen[key] == 2:
                duplicates.append(key)

        assert not duplicates, (
            f"{path.parent.parent.name}: duplicate keys under 'Data Scheme': "
            f"{duplicates}"
        )
