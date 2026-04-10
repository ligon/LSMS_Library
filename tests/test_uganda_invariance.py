"""
Regression tests for Uganda parquet outputs.

Compares every parquet under the Uganda country directory against a
previously generated baseline manifest (tests/fixtures/uganda_baseline.json).

The manifest records shape, columns, index names, dtypes, and a
deterministic content hash for each DataFrame.  Any deviation fails the test.

To regenerate the baseline after an intentional change:
    python tests/generate_baseline.py lsms_library/countries/Uganda
"""

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
BASELINE_PATH = FIXTURES_DIR / "uganda_baseline.json"


def _load_baseline():
    if not BASELINE_PATH.exists():
        pytest.skip(f"Baseline manifest not found at {BASELINE_PATH}")
    with open(BASELINE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _find_uganda_root():
    """Locate the Uganda directory, checking common locations."""
    candidates = [
        Path(__file__).resolve().parent.parent / "lsms_library" / "countries" / "Uganda",
    ]
    # Also check LSMS_UGANDA_ROOT env var
    import os
    env_root = os.environ.get("LSMS_UGANDA_ROOT")
    if env_root:
        candidates.insert(0, Path(env_root).resolve())

    for c in candidates:
        if c.is_dir():
            return c
    return None


def _fingerprint(df: pd.DataFrame) -> dict:
    """Compute a deterministic fingerprint for a DataFrame."""
    cols = sorted(df.columns.tolist())
    idx_names = list(df.index.names)
    dtypes = {str(k): str(v) for k, v in sorted(df.dtypes.items(), key=lambda x: str(x[0]))}
    row_hashes = pd.util.hash_pandas_object(df, index=True)
    content_hash = hashlib.sha256(row_hashes.values.tobytes()).hexdigest()
    return {
        "shape": list(df.shape),
        "columns": cols,
        "index_names": idx_names,
        "dtypes": dtypes,
        "content_hash": content_hash,
    }


BASELINE = _load_baseline()
UGANDA_ROOT = _find_uganda_root()

# Baselines known to drift after the sample() v-migration (Phases 2/3/4).
# These fixtures were captured before v was joined at API time and m was
# removed from baked parquets, so the recorded index_names / shape / dtype
# no longer match the post-migration output.  Tracked in GH #135;
# regenerate via `python tests/generate_baseline.py lsms_library/countries/Uganda`
# once the v-migration is released.
KNOWN_BASELINE_DRIFT = {
    "var/food_acquired.parquet",
    "var/food_expenditures.parquet",
    "var/food_prices.parquet",
    "var/food_quantities.parquet",
    "var/household_roster.parquet",
    "var/locality.parquet",
}


@pytest.fixture(scope="module")
def uganda_root():
    if UGANDA_ROOT is None:
        pytest.skip("Uganda country directory not found")
    # Skip if no parquets are built (e.g., CI without data access)
    has_parquets = any(UGANDA_ROOT.rglob("*.parquet"))
    if not has_parquets:
        from lsms_library.paths import data_root
        data_root.cache_clear()
        ext = data_root("Uganda")
        has_parquets = ext.exists() and any(ext.rglob("*.parquet"))
    if not has_parquets:
        pytest.skip("No Uganda parquets built (requires data access)")
    return UGANDA_ROOT


def _parquet_ids():
    """Generate test IDs from baseline keys."""
    return sorted(BASELINE.keys())


@pytest.mark.parametrize("rel_path", _parquet_ids())
def test_parquet_matches_baseline(uganda_root, rel_path):
    """Each parquet must exactly match its baseline fingerprint."""
    baseline_entry = BASELINE[rel_path]

    if "error" in baseline_entry:
        pytest.skip(f"Baseline recorded an error for {rel_path}: {baseline_entry['error']}")

    if rel_path in KNOWN_BASELINE_DRIFT:
        pytest.xfail(
            f"{rel_path}: baseline predates sample() v-migration "
            f"(Phases 2/3/4); regenerate fixture. Tracked in GH #135."
        )

    # Check both in-tree and data_root locations
    parquet_path = uganda_root / rel_path
    if not parquet_path.exists():
        from lsms_library.paths import data_root
        data_root.cache_clear()
        alt_path = data_root("Uganda") / rel_path
        if alt_path.exists():
            parquet_path = alt_path
    if not parquet_path.exists():
        pytest.skip(f"Parquet not built: {rel_path}")

    df = pd.read_parquet(parquet_path, engine="pyarrow")
    actual = _fingerprint(df)

    # Check shape
    assert actual["shape"] == baseline_entry["shape"], (
        f"{rel_path}: shape mismatch: {actual['shape']} != {baseline_entry['shape']}"
    )

    # Check columns
    assert actual["columns"] == baseline_entry["columns"], (
        f"{rel_path}: column mismatch:\n"
        f"  actual:   {actual['columns']}\n"
        f"  expected: {baseline_entry['columns']}"
    )

    # Check index names
    assert actual["index_names"] == baseline_entry["index_names"], (
        f"{rel_path}: index names mismatch:\n"
        f"  actual:   {actual['index_names']}\n"
        f"  expected: {baseline_entry['index_names']}"
    )

    # Check dtypes
    assert actual["dtypes"] == baseline_entry["dtypes"], (
        f"{rel_path}: dtype mismatch:\n"
        f"  actual:   {actual['dtypes']}\n"
        f"  expected: {baseline_entry['dtypes']}"
    )

    # Check content hash
    assert actual["content_hash"] == baseline_entry["content_hash"], (
        f"{rel_path}: content hash mismatch (data differs):\n"
        f"  actual:   {actual['content_hash']}\n"
        f"  expected: {baseline_entry['content_hash']}"
    )
