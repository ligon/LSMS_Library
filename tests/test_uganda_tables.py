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
    previous_cache_setting = os.environ.get("LSMS_USE_DVC_CACHE")
    os.environ["LSMS_USE_DVC_CACHE"] = "true"

    df = None
    after_mtime = None
    before_mtime = None

    try:
        country = ll.Country("Uganda", preload_panel_ids=False, verbose=False)
        var_path = country.file_path / "var" / "household_characteristics.parquet"
        legacy_path = country.file_path / "_" / "household_characteristics.parquet"

        before_mtime = var_path.stat().st_mtime if var_path.exists() else None

        backup_var = _backup_and_remove(var_path, tmp_path)
        backup_legacy = _backup_and_remove(legacy_path, tmp_path)

        try:
            df = country.household_characteristics()
            if var_path.exists():
                after_mtime = var_path.stat().st_mtime
        finally:
            _restore_or_cleanup(var_path, backup_var)
            _restore_or_cleanup(legacy_path, backup_legacy)
    finally:
        if previous_cache_setting is None:
            os.environ.pop("LSMS_USE_DVC_CACHE", None)
        else:
            os.environ["LSMS_USE_DVC_CACHE"] = previous_cache_setting

    assert df is not None
    assert hasattr(df, "index")
    assert "m" in (df.index.names or []), "household_characteristics missing 'm' index"
    assert not df.empty, "household_characteristics returned an empty dataframe"

    assert after_mtime is not None, "Expected household_characteristics parquet to be materialized"
    if before_mtime is not None:
        assert after_mtime != before_mtime, "Materialized parquet timestamp did not change despite regeneration"
