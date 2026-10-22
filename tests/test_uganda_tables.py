import pandas as pd
import pytest

import lsms_library as ll
from lsms_library.paths import data_root

pytestmark = pytest.mark.requires_s3


def _has_cached_table(country_name: str, table: str, *, market=False) -> bool:
    """Resolve runtime-derived prerequisites without creating outputs."""
    # Reuse dispatch memberships, not a second table registry (ledger sections 2 and 5).
    if table in ll.Country._FOOD_DERIVED:
        sources = ("food_acquired", "sample")
    elif table in ll.Country._ROSTER_DERIVED:
        sources = ("household_roster", "sample")
    else:
        sources = (table,)
    if market:
        sources += ("cluster_features",)
    root = data_root(country_name) / "var"
    return all((root / f"{source}.parquet").is_file() for source in sources)


UGANDA_TABLES = [
    "household_characteristics",
    "food_expenditures",
    "food_quantities",
    "shocks",
    "income",
    "cluster_features",
    "food_prices",
]

DERIVED_TABLES = [
    "household_characteristics",
    "food_expenditures",
    "food_quantities",
    "food_prices",
]


def _assert_default_derived(result: pd.DataFrame, table: str) -> None:
    assert isinstance(result, pd.DataFrame)
    assert not result.empty, f"{table} should not be empty"
    required_levels = {"t", "v", "i"}
    if table in ll.Country._FOOD_DERIVED:
        required_levels.update({"j", "s"})
        if table != "food_expenditures":
            required_levels.add("u")
    missing = required_levels.difference(result.index.names)
    assert not missing, f"{table} missing index levels {sorted(missing)}"
    assert "m" not in result.index.names, "Default API must keep cluster v"


def _assert_region_preserves_observations(default, region):
    # Region can exclude unresolved geography, but cannot change retained data
    # or multiply rows. Uganda's changing v and missing Region are in ledger section 3.
    assert not region.empty
    assert "m" in region.index.names
    assert "v" not in region.index.names
    assert region.index.is_unique
    assert region.index.get_level_values("m").notna().all()
    default_keys = default.droplevel("v")
    region_keys = region.droplevel("m")
    assert default_keys.index.is_unique
    assert region_keys.index.is_unique
    assert len(region_keys) <= len(default_keys)
    assert region_keys.index.isin(default_keys.index).all()
    retained = default_keys.loc[region_keys.index]
    assert list(region_keys.columns) == list(retained.columns)
    # Compare each Series exactly. HC's existing columns.name metadata changes
    # through the market merge; column payload dtypes and values must not.
    for column in retained.columns:
        pd.testing.assert_series_equal(
            region_keys[column], retained[column], check_exact=True,
        )


@pytest.mark.parametrize("table", UGANDA_TABLES)
def test_uganda_cached_source_api(table):
    if not _has_cached_table("Uganda", table):
        pytest.skip(f"Uganda/{table} source prerequisites not cached")
    country = ll.Country("Uganda", preload_panel_ids=False, verbose=False)
    result = getattr(country, table)()
    assert result is not None
    if hasattr(result, "empty"):
        assert not result.empty, f"{table} should not be empty"
    if table in DERIVED_TABLES:
        _assert_default_derived(result, table)


@pytest.mark.parametrize("table", DERIVED_TABLES)
def test_uganda_derived_explicit_region_preserves_observations(table):
    if not _has_cached_table("Uganda", table, market=True):
        pytest.skip(f"Uganda/{table} source and Region prerequisites not cached")
    country = ll.Country("Uganda", preload_panel_ids=False, verbose=False)
    method = getattr(country, table)
    default = method()
    _assert_default_derived(default, table)
    region = method(market="Region")
    _assert_region_preserves_observations(default, region)
