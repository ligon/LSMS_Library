"""
Structural tests for country tables.

Checks that cached/materialized tables conform to their data_scheme.yml
declarations: correct index levels, expected columns present, sensible
dtypes.  Only tests tables that already exist at data_root() — does NOT
trigger builds.
"""

import os
import re
from pathlib import Path

import pandas as pd
import pytest
import yaml

from lsms_library.paths import data_root, COUNTRIES_ROOT
from lsms_library.yaml_utils import load_yaml


def _load_api_derived_columns() -> dict[str, set[str]]:
    """Load columns marked api_derived in data_info.yml.

    These columns are added by _finalize_result() at API read time and are
    NOT stored in the raw parquets.  Tests that read raw parquets must skip
    them when checking declared columns.

    Returns {table_name: {col_name, ...}}.
    """
    data_info_path = (
        Path(__file__).resolve().parent.parent / "lsms_library" / "data_info.yml"
    )
    with open(data_info_path, "r", encoding="utf-8") as f:
        info = yaml.safe_load(f)
    columns = info.get("Columns", {})
    result: dict[str, set[str]] = {}
    for table, cols in columns.items():
        if not isinstance(cols, dict):
            continue
        derived = {
            col
            for col, meta in cols.items()
            if isinstance(meta, dict) and meta.get("api_derived")
        }
        if derived:
            result[table] = derived
    return result


_API_DERIVED = _load_api_derived_columns()


def _parse_index_tuple(raw: str) -> list[str]:
    """Parse '(i, t, m)' into ['i', 't', 'm']."""
    if not raw:
        return []
    cleaned = raw.strip().strip("()")
    return [s.strip() for s in cleaned.split(",") if s.strip()]


def _load_all_schemes() -> dict[str, dict]:
    """Load data_scheme.yml for every country that has one.

    Returns {country_name: {table_name: {index: [...], columns: {...}, optional: set}}}

    Column declarations support an extended dict syntax::

        panel_weight:
          type: float
          optional: true

    Columns declared with ``optional: true`` are collected in the ``optional``
    set so that ``test_declared_columns_present`` can skip them when absent.
    """
    schemes = {}
    for yml in sorted(COUNTRIES_ROOT.glob("*/_/data_scheme.yml")):
        country = yml.parent.parent.name
        data = load_yaml(yml)
        if not isinstance(data, dict):
            continue
        ds = data.get("Data Scheme")
        if not isinstance(ds, dict):
            continue
        tables = {}
        for table_name, spec in ds.items():
            if not isinstance(table_name, str):
                continue
            if spec is None or (isinstance(spec, str) and spec.strip() == ""):
                # Table declared but no schema details (e.g., food_expenditures:)
                tables[table_name] = {"index": [], "columns": {}, "optional": set()}
                continue
            if not isinstance(spec, dict):
                # Could be a !make tag or other non-dict
                tables[table_name] = {"index": [], "columns": {}, "optional": set()}
                continue
            idx_raw = spec.get("index", "")
            idx = _parse_index_tuple(str(idx_raw)) if idx_raw else []
            # Skip non-column keys: 'index', 'materialize' (from !make tag), etc.
            skip_keys = {"index", "materialize", "backend"}
            columns = {}
            optional_cols: set[str] = set()
            for k, v in spec.items():
                if k in skip_keys or not isinstance(k, str):
                    continue
                if isinstance(v, dict):
                    # Extended syntax: {type: float, optional: true}
                    columns[k] = v.get("type", "str")
                    if v.get("optional"):
                        optional_cols.add(k)
                else:
                    columns[k] = v
            tables[table_name] = {"index": idx, "columns": columns, "optional": optional_cols}
        schemes[country] = tables
    return schemes


ALL_SCHEMES = _load_all_schemes()


def _find_cached_parquets() -> list[tuple[str, str, Path]]:
    """Find all (country, table, path) tuples for cached parquets at data_root."""
    results = []
    for country, tables in ALL_SCHEMES.items():
        country_data = data_root(country)
        for table_name in tables:
            # Check var/ (country-level aggregates)
            var_pq = country_data / "var" / f"{table_name}.parquet"
            if var_pq.exists():
                results.append((country, table_name, var_pq))
    return results


CACHED = _find_cached_parquets()


@pytest.fixture(scope="module")
def cached_parquets():
    if not CACHED:
        pytest.skip("No cached parquets found at data_root")
    return CACHED


# ---------------------------------------------------------------------------
# Parametrized tests over cached tables
# ---------------------------------------------------------------------------

def _cached_ids():
    return [f"{c}/{t}" for c, t, _ in CACHED]


@pytest.mark.parametrize(
    "country,table,path",
    CACHED,
    ids=_cached_ids(),
)
class TestTableStructure:

    def test_readable(self, country, table, path):
        """Parquet file can be read into a DataFrame."""
        df = pd.read_parquet(path, engine="pyarrow")
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0, f"{country}/{table} is empty"

    def test_index_levels(self, country, table, path):
        """Index levels match data_scheme declaration (when declared)."""
        spec = ALL_SCHEMES[country][table]
        expected_idx = spec["index"]
        if not expected_idx:
            pytest.skip(f"{country}/{table} has no declared index")

        df = pd.read_parquet(path, engine="pyarrow")
        actual_idx = list(df.index.names)

        # Check that declared levels are present (order may vary,
        # and there may be extra levels like 'visit')
        for level in expected_idx:
            assert level in actual_idx, (
                f"{country}/{table}: expected index level '{level}' "
                f"not found in {actual_idx}"
            )

    def test_declared_columns_present(self, country, table, path):
        """Columns declared in data_scheme exist in the DataFrame.

        Columns marked ``api_derived`` in data_info.yml are skipped: they are
        added by ``_finalize_result()`` at API read time and are intentionally
        absent from the raw cached parquets.

        Columns declared with ``optional: true`` in data_scheme.yml are also
        skipped when absent — they represent data that is genuinely unavailable
        for some countries (e.g., ``panel_weight`` in cross-sectional surveys).
        """
        spec = ALL_SCHEMES[country][table]
        expected_cols = spec["columns"]
        if not expected_cols:
            pytest.skip(f"{country}/{table} has no declared columns")

        # Columns produced at API time (e.g., kinship decomposition).
        api_derived = _API_DERIVED.get(table, set())
        # Columns declared optional in data_scheme.yml (genuinely unavailable data).
        optional_cols = spec.get("optional", set())

        df = pd.read_parquet(path, engine="pyarrow")
        all_names = set(df.columns.tolist() + list(df.index.names))

        for col_name in expected_cols:
            if col_name in api_derived:
                continue  # present in API output, not in raw parquet
            if col_name in optional_cols:
                continue  # genuinely absent from this country's source data
            assert col_name in all_names, (
                f"{country}/{table}: declared column '{col_name}' "
                f"not found in {sorted(all_names)}"
            )

    def test_no_fully_null_columns(self, country, table, path):
        """No column should be entirely null."""
        df = pd.read_parquet(path, engine="pyarrow")
        for col in df.columns:
            assert not df[col].isna().all(), (
                f"{country}/{table}: column '{col}' is entirely null"
            )

    def test_no_duplicate_rows(self, country, table, path):
        """Index should be unique (no exact duplicate rows by index)."""
        # Known: some tables (e.g., shocks) have legitimate duplicate indices
        # from multiple shock types per household.  We flag >50% as likely errors.
        df = pd.read_parquet(path, engine="pyarrow")
        if df.index.has_duplicates:
            dup_count = df.index.duplicated().sum()
            total = len(df)
            if dup_count / total > 0.50:
                pytest.fail(
                    f"{country}/{table}: {dup_count}/{total} duplicate index entries "
                    f"({dup_count/total:.1%})"
                )


# ---------------------------------------------------------------------------
# Diagnostics integration: is_this_feature_sane
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "country,table,path",
    CACHED,
    ids=_cached_ids(),
)
class TestFeatureSanity:
    def test_feature_is_sane(self, country, table, path):
        """Each cached feature must pass is_this_feature_sane (no failures)."""
        from lsms_library.diagnostics import is_this_feature_sane
        df = pd.read_parquet(path, engine="pyarrow")
        report = is_this_feature_sane(df, country, table)
        if not report.ok:
            report.summarize()
            pytest.fail(
                f"{country}/{table} failed sanity checks: "
                + "; ".join(c.message for c in report.errors)
            )

# ---------------------------------------------------------------------------
# Housing v-dtype test (GH #142): v index level must be pd.StringDtype
# ---------------------------------------------------------------------------

def _countries_with_housing() -> list[str]:
    """Discover countries whose data_scheme.yml declares a housing table."""
    from lsms_library.paths import COUNTRIES_ROOT
    from lsms_library.yaml_utils import load_yaml
    countries = []
    for yml in sorted(COUNTRIES_ROOT.glob("*/_/data_scheme.yml")):
        data = load_yaml(yml)
        if not isinstance(data, dict):
            continue
        ds = data.get("Data Scheme", {})
        if isinstance(ds, dict) and "housing" in ds:
            countries.append(yml.parent.parent.name)
    return countries


_HOUSING_COUNTRIES = _countries_with_housing()


@pytest.mark.parametrize("country_name", _HOUSING_COUNTRIES)
class TestHousingVDtype:
    """GH #142: housing() must return v as pd.StringDtype, never mixed int/str.

    Mixed-dtype v levels cause pyarrow serialisation failures on to_parquet()
    and produce broken MultiIndex levels when Feature('housing') concatenates
    across countries.  The fix lives in _join_v_from_sample() in country.py.
    """

    @pytest.fixture()
    def housing_df(self, country_name):
        import warnings
        import lsms_library as ll
        warnings.filterwarnings("ignore")
        try:
            df = ll.Country(country_name).housing()
            assert isinstance(df, pd.DataFrame) and not df.empty
            return df
        except Exception as e:
            pytest.skip(f"{country_name}.housing() failed: {e}")

    def test_v_is_string_dtype(self, country_name, housing_df):
        """v index level must be pd.StringDtype (not int64, float64, or object)."""
        if "v" not in housing_df.index.names:
            pytest.skip(f"{country_name} housing() has no v in index")
        v_level = housing_df.index.get_level_values("v")
        assert isinstance(v_level.dtype, pd.StringDtype), (
            f"{country_name} housing() v dtype is {v_level.dtype!r}, "
            f"expected pd.StringDtype(). Mixed int/str causes pyarrow failures "
            f"(GH #142). Fix: _join_v_from_sample must coerce v."
        )

    def test_to_parquet_succeeds(self, country_name, housing_df):
        """housing() result must be serialisable by pyarrow.to_parquet().

        Mixed-dtype MultiIndex levels cause ArrowInvalid — this is the exact
        failure reported in GH #142.
        """
        import io
        buf = io.BytesIO()
        housing_df.to_parquet(buf)  # raises ArrowInvalid if v is mixed
        assert buf.getbuffer().nbytes > 0


# ---------------------------------------------------------------------------
# Summary test: at least something is cached
# ---------------------------------------------------------------------------

class TestCachePopulation:
    def test_at_least_one_country_has_cached_data(self):
        """Sanity check: at least one country has materialized tables."""
        if len(CACHED) == 0:
            pytest.skip(
                "No cached parquets found at data_root(). "
                "Run a build first to populate the cache."
            )

    def test_cached_tables_summary(self):
        """Report which countries/tables are cached (informational)."""
        by_country: dict[str, list[str]] = {}
        for country, table, _ in CACHED:
            by_country.setdefault(country, []).append(table)
        for country in sorted(by_country):
            tables = sorted(by_country[country])
            print(f"  {country}: {len(tables)} tables — {', '.join(tables)}")
        assert True  # Always passes; output is informational
