"""
Cross-country schema consistency tests.

Validates that data_scheme.yml files across all countries follow shared
conventions defined in the canonical schema (lsms_library/data_info.yml):
required columns, rejected column-name spellings, parsability, and no
silent duplicate keys.
"""

import re
from pathlib import Path

import pytest
import yaml

from lsms_library.paths import countries_root

# ---------------------------------------------------------------------------
# Load canonical schema from data_info.yml
# ---------------------------------------------------------------------------

_DATA_INFO_PATH = Path(__file__).resolve().parent.parent / "lsms_library" / "data_info.yml"

with open(_DATA_INFO_PATH, "r", encoding="utf-8") as _f:
    _CANONICAL = yaml.safe_load(_f)

_COLUMNS = _CANONICAL.get("Columns", {})
_REJECTED = _CANONICAL.get("Rejected Spellings", {})

_SKIP_KEYS = {"index", "materialize", "backend"}


def _required_columns(table: str) -> set[str]:
    """Return the set of required column names for a table."""
    spec = _COLUMNS.get(table, {})
    return {col for col, meta in spec.items()
            if isinstance(meta, dict) and meta.get("required")}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _SchemeLoader(yaml.SafeLoader):
    """SafeLoader that handles the !make tag used in data_scheme.yml."""

_SchemeLoader.add_constructor(
    "!make", lambda loader, node: {"__make__": True}
)


def _load_yaml(path: Path) -> dict:
    """Load a YAML file, returning {} on empty."""
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.load(f, Loader=_SchemeLoader)
    return data or {}


def _all_data_scheme_paths() -> list[Path]:
    """Return sorted list of all data_scheme.yml paths."""
    return sorted(countries_root().glob("*/_/data_scheme.yml"))


def _schemes_with_table(table: str) -> list[tuple[str, Path, dict]]:
    """Return (country, path, table_spec) for every scheme declaring *table*."""
    results = []
    for yml in _all_data_scheme_paths():
        country = yml.parent.parent.name
        data = _load_yaml(yml)
        if not isinstance(data, dict):
            continue
        ds = data.get("Data Scheme")
        if not isinstance(ds, dict):
            continue
        spec = ds.get(table)
        if spec is not None and isinstance(spec, dict):
            results.append((country, yml, spec))
    return results


ALL_SCHEME_PATHS = _all_data_scheme_paths()


# ---------------------------------------------------------------------------
# Build parametrized test data for every table that has required columns
# ---------------------------------------------------------------------------

_TABLES_WITH_REQUIREMENTS = [
    table for table, cols in _COLUMNS.items()
    if any(isinstance(m, dict) and m.get("required")
           for m in cols.values())
]

_TABLE_SCHEMES: dict[str, list[tuple[str, Path, dict]]] = {
    table: _schemes_with_table(table)
    for table in _TABLES_WITH_REQUIREMENTS
}

# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestRequiredColumns:
    """Ensure every table declares its required columns per data_info.yml."""

    @staticmethod
    def _cases():
        for table, schemes in _TABLE_SCHEMES.items():
            required = _required_columns(table)
            for country, path, spec in schemes:
                yield pytest.param(
                    country, path, spec, table, required,
                    id=f"{country}:{table}",
                )

    @pytest.mark.parametrize(
        "country,path,spec,table,required", list(_cases.__func__())
    )
    def test_required_columns_present(self, country, path, spec, table, required):
        columns = set(spec.keys()) - _SKIP_KEYS
        missing = required - columns
        assert not missing, (
            f"{country}: {table} is missing required columns "
            f"{sorted(missing)} in {path}"
        )


class TestRejectedSpellings:
    """Ensure no column name uses a rejected spelling."""

    @pytest.mark.parametrize(
        "path",
        ALL_SCHEME_PATHS,
        ids=[p.parent.parent.name for p in ALL_SCHEME_PATHS],
    )
    def test_no_rejected_column_spellings(self, path):
        data = _load_yaml(path)
        ds = data.get("Data Scheme")
        if not isinstance(ds, dict):
            pytest.skip("no Data Scheme section")

        violations = []
        for table_name, table_spec in ds.items():
            if not isinstance(table_spec, dict):
                continue
            for col in table_spec:
                if col in _SKIP_KEYS:
                    continue
                for rejected, canonical in _REJECTED.items():
                    if col == rejected or rejected in col:
                        violations.append(
                            f"{table_name}.{col} -> use {canonical}"
                        )

        country = path.parent.parent.name
        assert not violations, (
            f"{country}: rejected column spellings found: {violations}"
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
        data = _load_yaml(path)
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


class TestCountryConfigIsReachable:
    """A country's table registry must live in a filename the framework reads.

    GH #684: `Peru/_/` held a fully-populated `Data Scheme:` block inside a
    file called `data_info.yml`.  `Country.resources` reads `data_scheme.yml`,
    so `Country('Peru').data_scheme` was `[]`, every table was unreachable, and
    Peru contributed ZERO cells to the coverage matrix.

    The failure was silent and TOTAL, which is why it survived so long: an
    empty `data_scheme` produces no tables, so there is no build to fail.  The
    country could not even be graded `broken` -- a grader that iterates over
    declared features cannot see a country that declares none.  Every guard we
    had sat downstream of the config that never loaded.

    `data_info.yml` is the WAVE-level extraction spec (`{country}/{wave}/_/`).
    At COUNTRY level (`{country}/_/`) it is always a mistake, and the mistake
    is invisible at runtime, so it is asserted here instead.
    """

    def test_no_country_level_data_info_yml(self):
        """`{country}/_/data_info.yml` is never read; the registry is `data_scheme.yml`."""
        offenders = sorted(
            p.parent.parent.name
            for p in countries_root().glob("*/_/data_info.yml")
        )
        assert not offenders, (
            f"{offenders}: a COUNTRY-level `_/data_info.yml` is never read by "
            f"`Country.resources`, which loads `_/data_scheme.yml`.  Any "
            f"`Data Scheme:` block in it is dead text and the country's tables "
            f"are silently unreachable (GH #684).  Rename the file to "
            f"`_/data_scheme.yml`.  (Wave-level `{{wave}}/_/data_info.yml` is "
            f"correct and unaffected.)"
        )

    @pytest.mark.parametrize(
        "path",
        ALL_SCHEME_PATHS,
        ids=[p.parent.parent.name for p in ALL_SCHEME_PATHS],
    )
    def test_data_scheme_declares_at_least_one_table(self, path):
        """A `Data Scheme:` block that declares nothing makes the country invisible."""
        data = _load_yaml(path)
        scheme = data.get("Data Scheme") or {}
        country = path.parent.parent.name
        assert scheme, (
            f"{country}: `Data Scheme:` is empty, so `Country({country!r})"
            f".data_scheme` is `[]` and the country contributes no cells to "
            f"the coverage matrix -- it cannot even be graded `broken` "
            f"(GH #684).  Declare its tables, or remove `{path}` so the "
            f"country is reported as `unconfigured` instead of silently "
            f"absent."
        )
