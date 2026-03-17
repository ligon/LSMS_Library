import os
import shutil
from pathlib import Path
from typing import Optional
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
    if hasattr(result, "index") and method.__name__ in {"household_characteristics", "food_expenditures", "food_quantities", "food_prices"}:
        required_levels = {"m", "i"}
        missing = required_levels.difference(result.index.names or [])
        assert not missing, f"{table} missing index levels {sorted(missing)}"


def _backup_and_remove(path: Path, tmpdir: Path) -> Optional[Path]:
    """Copy the existing file to tmpdir (if any) and remove the original."""
    if not path.exists():
        return None
    token = "__".join(path.parts[-3:])
    backup = tmpdir / token
    backup.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(path, backup)
    path.unlink()
    return backup


def _restore_or_cleanup(path: Path, backup: Optional[Path]) -> None:
    """Restore from backup when provided; otherwise remove any generated file."""
    if backup and backup.exists():
        if path.exists():
            path.unlink()
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(backup), path)
    else:
        if path.exists():
            path.unlink()


def test_uganda_household_characteristics_has_m_index(tmp_path):
    """Verify household_characteristics has 'm' (market/region) in its index.

    Requires pre-built household_characteristics parquet (skips in CI).
    """
    from lsms_library.paths import data_root
    country = ll.Country("Uganda", preload_panel_ids=False, verbose=False)

    # Only run if cached data exists (building from scratch may not produce 'm')
    var_path = country.file_path / "var" / "household_characteristics.parquet"
    ext_path = data_root("Uganda") / "var" / "household_characteristics.parquet"
    if not var_path.exists() and not ext_path.exists():
        pytest.skip("household_characteristics parquet not cached (requires data build)")

    df = country.household_characteristics()

    assert df is not None
    assert hasattr(df, "index")
    assert not df.empty, "household_characteristics returned an empty dataframe"
    assert "m" in (df.index.names or []), "household_characteristics missing 'm' index"
