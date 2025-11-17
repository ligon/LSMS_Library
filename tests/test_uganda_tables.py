import os
import pytest

import lsms_library as ll

UGANDA_TABLES = [
    "household_characteristics",
    "food_expenditures",
    "food_quantities",
    "shocks",
    "income",
    "cluster_features",
    "food_prices",
]

@pytest.mark.parametrize("table", UGANDA_TABLES)
def test_uganda_makefile_backfill(table):
    os.environ.setdefault("LSMS_USE_DVC_CACHE", "false")
    country = ll.Country("Uganda", preload_panel_ids=False, verbose=False)
    method = getattr(country, table)
    result = method()
    assert result is not None
    if hasattr(result, "empty"):
        assert not result.empty, f"{table} should not be empty"
