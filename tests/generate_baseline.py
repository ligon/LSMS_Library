#!/usr/bin/env python3
"""
Generate a baseline fingerprint manifest for Uganda parquet outputs.

Walks a Uganda build tree, reads each parquet into a DataFrame, and records
a deterministic fingerprint (shape, columns, index names, dtypes, content hash).

Usage:
    python tests/generate_baseline.py /path/to/countries/Uganda

Writes the manifest to tests/fixtures/uganda_baseline.json.
"""

import hashlib
import json
import sys
from pathlib import Path

import pandas as pd


def fingerprint(df: pd.DataFrame) -> dict:
    """Compute a deterministic fingerprint for a DataFrame."""
    # Sort columns for determinism
    cols = sorted(df.columns.tolist())
    idx_names = list(df.index.names)

    # dtype dict (sorted)
    dtypes = {str(k): str(v) for k, v in sorted(df.dtypes.items(), key=lambda x: str(x[0]))}

    # Content hash: hash_pandas_object produces per-row hashes;
    # we hash the concatenation for a single digest.
    row_hashes = pd.util.hash_pandas_object(df, index=True)
    content_hash = hashlib.sha256(row_hashes.values.tobytes()).hexdigest()

    return {
        "shape": list(df.shape),
        "columns": cols,
        "index_names": idx_names,
        "dtypes": dtypes,
        "content_hash": content_hash,
    }


def build_manifest(uganda_root: Path, data_root_path: Path | None = None) -> dict:
    """Walk all parquets under uganda_root (and optionally data_root) and fingerprint each.

    Mirrors the test's search logic: in-tree first, data_root as fallback.
    For each unique relative path, the in-tree version takes precedence.
    """
    manifest = {}

    # First pass: in-tree parquets (take precedence)
    import pyarrow.lib as _pa_lib
    _parquet_errors = (OSError, ValueError, _pa_lib.ArrowInvalid)
    for pq in sorted(uganda_root.rglob("*.parquet")):
        rel = str(pq.relative_to(uganda_root))
        try:
            df = pd.read_parquet(pq, engine="pyarrow")
            manifest[rel] = fingerprint(df)
        except _parquet_errors as e:
            print(f"WARNING: Could not read {rel}: {e}", file=sys.stderr)
            manifest[rel] = {"error": str(e)}

    # Second pass: data_root parquets (only for paths not already in manifest)
    if data_root_path and data_root_path.is_dir():
        for pq in sorted(data_root_path.rglob("*.parquet")):
            rel = str(pq.relative_to(data_root_path))
            if rel not in manifest:
                try:
                    df = pd.read_parquet(pq, engine="pyarrow")
                    manifest[rel] = fingerprint(df)
                except _parquet_errors as e:
                    print(f"WARNING: Could not read {rel}: {e}", file=sys.stderr)
                    manifest[rel] = {"error": str(e)}

    return manifest


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} /path/to/countries/Uganda [/path/to/data_root/Uganda]", file=sys.stderr)
        sys.exit(1)

    uganda_root = Path(sys.argv[1]).resolve()
    if not uganda_root.is_dir():
        print(f"Not a directory: {uganda_root}", file=sys.stderr)
        sys.exit(1)

    # Optional second arg: data_root path for wave-level parquets
    data_root_path = None
    if len(sys.argv) >= 3:
        data_root_path = Path(sys.argv[2]).resolve()
    else:
        # Auto-detect data_root if lsms_library is importable
        try:
            from lsms_library.paths import data_root
            data_root_path = data_root("Uganda")
        except (ImportError, KeyError, ValueError):
            pass

    manifest = build_manifest(uganda_root, data_root_path)

    fixtures_dir = Path(__file__).resolve().parent / "fixtures"
    fixtures_dir.mkdir(exist_ok=True)
    out_path = fixtures_dir / "uganda_baseline.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)

    print(f"Wrote {len(manifest)} entries to {out_path}")


if __name__ == "__main__":
    main()
