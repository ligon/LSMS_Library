"""
Regression tests for Uganda parquet outputs.

Compares every parquet under the Uganda country directory against a
previously generated baseline manifest (tests/fixtures/uganda_baseline.json).

The manifest records shape, columns, index names, dtypes, and a
deterministic content hash for each DataFrame.  Any deviation fails the test.

To regenerate the baseline after an intentional change:
    python tests/generate_baseline.py lsms_library/countries/Uganda

A ``null`` value for any of the five fields means "not pinned" and that
field is not checked.  It is for the case where a fix lands from a machine
without microdata access, so the new value is not computable here but the
old one is known wrong: pinning the stale value would be a lie, deleting
the entry would silently drop the parquet from the sweep, and failing would
leave the suite red for everyone with data until someone regenerated.  The
whole entry is still checked on every field that IS pinned.  ``null`` is a
debt marker -- ``generate_baseline.py`` never writes one, so a regeneration
run clears it.  No entry currently carries a null: the two that did (GH #772)
were regenerated once data access was available.  The mechanism is kept for
the next fix that lands without it.

An optional ``content_hash_variants`` list permits only measured exact raw
fingerprints in addition to the primary hash.  Uganda nutrition's Haswell
OpenBLAS result differs from its approved SkylakeX result by at most two
float64 ULPs in eight cells (ledger ``uganda-nutrition-cpu-hash.md`` sections
3-4).  This finite allowance does not round values, relax structural checks,
or accept an unmeasured exact fingerprint.  A null primary remains unpinned.

A parquet whose ``lsms_cache_waves`` manifest does not cover the waves its
baseline entry was taken over is skipped, not compared (GH #1113): it is a
correct partial build left by a subset request (GH #891), and under xdist
another test file's single-wave call can write one before this file runs.
"""

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

pytestmark = pytest.mark.requires_s3


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


# Canonical dtype equivalence for invariance comparison.  pandas 3.0 is
# migrating string-holding columns from 'object' (generic Python object
# storage) to explicit 'string' dtype; both spellings represent the same
# logical type for our purposes, and baselines captured at different
# points in that migration should not be treated as divergent.
#
# Conservative on purpose: we deliberately do NOT collapse int64↔Int64
# or float64↔Float64, since nullable vs. non-nullable numeric semantics
# differ in ways that DO matter (NaN handling, arithmetic behavior).
_DTYPE_EQUIVALENCE = {
    'object': 'string',
    'str': 'string',          # some extraction paths write bare 'str'
    'string[python]': 'string',
    'string[pyarrow]': 'string',
    # pandas 3.0 changed the default datetime resolution from [ns] to [us];
    # collapse so baselines captured on the old default still match.
    # Sub-microsecond precision is never meaningful for survey interview
    # timestamps, so treating [ns] == [us] is semantically safe.  Uganda
    # interview_date baseline tripped this on 2026-04-13.
    'datetime64[ns]': 'datetime64[us]',
}


def _canonical_dtypes(d: dict) -> dict:
    return {k: _DTYPE_EQUIVALENCE.get(v, v) for k, v in d.items()}

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


@pytest.fixture(scope="module")
def uganda_waves():
    from lsms_library import Country
    return [str(w) for w in Country("Uganda").waves]


def _expected_coverage(rel_path: str, uganda_waves: list[str]) -> list[str]:
    """The waves a baseline entry was taken over: every Uganda wave for a
    ``var/`` (L2-country) entry, the entry's own wave for ``{wave}/_/``."""
    head = rel_path.split("/", 1)[0]
    return list(uganda_waves) if head == "var" else [head]


def _coverage_skip_reason(rel_path, manifest, expected):
    """Why a parquet cannot be compared, or ``None`` when it can (GH #1113).

    Since GH #891 a subset request on a cold cache writes ``var/<t>.parquet``
    holding only the waves it asked for, and records them in the
    ``lsms_cache_waves`` manifest.  Such a parquet is a correct partial build,
    not a regression, so comparing it to a full-coverage baseline is
    meaningless.  Skip ONLY when a manifest is present and short: a parquet
    with no manifest (every wave-tier parquet, and the Makefile-written
    ``var/`` tables such as ``fct`` and ``earnings``) is compared as before,
    since its coverage cannot be read and must not be guessed from ``t``.
    """
    if manifest is None:
        return None
    missing = sorted(set(expected) - set(manifest))
    if not missing:
        return None
    return (
        f"{rel_path}: partial build, not compared -- its lsms_cache_waves "
        f"manifest covers {sorted(manifest)}, the baseline was taken over "
        f"{sorted(expected)} (missing {missing}).  A subset request built "
        f"this parquet (GH #891); see GH #1113."
    )


def _parquet_ids():
    """Generate test IDs from baseline keys."""
    return sorted(BASELINE.keys())


@pytest.mark.parametrize("rel_path", _parquet_ids())
def test_parquet_matches_baseline(uganda_root, uganda_waves, rel_path):
    """Each parquet must exactly match its baseline fingerprint."""
    from lsms_library.local_tools import read_parquet_cache_waves

    baseline_entry = BASELINE[rel_path]

    if "error" in baseline_entry:
        pytest.skip(f"Baseline recorded an error for {rel_path}: {baseline_entry['error']}")

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

    # The manifest is read again after the data: writes are atomic renames,
    # so a concurrent rebuild can swap the file between the two reads.
    expected = _expected_coverage(rel_path, uganda_waves)
    reason = _coverage_skip_reason(
        rel_path, read_parquet_cache_waves(parquet_path), expected)
    if reason:
        pytest.skip(reason)
    df = pd.read_parquet(parquet_path, engine="pyarrow")
    reason = _coverage_skip_reason(
        rel_path, read_parquet_cache_waves(parquet_path), expected)
    if reason:
        pytest.skip(reason)
    actual = _fingerprint(df)

    # Check shape
    if baseline_entry["shape"] is not None:
        assert actual["shape"] == baseline_entry["shape"], (
            f"{rel_path}: shape mismatch: {actual['shape']} != {baseline_entry['shape']}"
        )

    # Check columns
    if baseline_entry["columns"] is not None:
        assert actual["columns"] == baseline_entry["columns"], (
            f"{rel_path}: column mismatch:\n"
            f"  actual:   {actual['columns']}\n"
            f"  expected: {baseline_entry['columns']}"
        )

    # Check index names
    if baseline_entry["index_names"] is not None:
        assert actual["index_names"] == baseline_entry["index_names"], (
            f"{rel_path}: index names mismatch:\n"
            f"  actual:   {actual['index_names']}\n"
            f"  expected: {baseline_entry['index_names']}"
        )

    # Check dtypes (after canonicalizing string-family equivalence —
    # see _DTYPE_EQUIVALENCE above).
    if baseline_entry["dtypes"] is not None:
        assert _canonical_dtypes(actual["dtypes"]) == _canonical_dtypes(baseline_entry["dtypes"]), (
            f"{rel_path}: dtype mismatch:\n"
            f"  actual:   {actual['dtypes']}\n"
            f"  expected: {baseline_entry['dtypes']}"
        )

    # Check content hash
    if baseline_entry["content_hash"] is not None:
        accepted_hashes = [baseline_entry["content_hash"],
                           *baseline_entry.get("content_hash_variants", [])]
        assert actual["content_hash"] in accepted_hashes, (
            f"{rel_path}: content hash mismatch (data differs):\n"
            f"  actual:   {actual['content_hash']}\n"
            f"  expected exact hash(es): {accepted_hashes}"
        )
