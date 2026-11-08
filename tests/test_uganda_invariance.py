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

Coverage is reported explicitly at session end (GH1480). For a read-only
complete check, use ``scripts/check_uganda_invariance.py --data-root PRIVATE
--complete``; it never builds or falls back to configured/shared roots.

A parquet whose ``lsms_cache_waves`` manifest does not cover the waves its
baseline entry was taken over is skipped, not compared (GH #1113): it is a
correct partial build left by a subset request (GH #891), and under xdist
another test file's single-wave call can write one before this file runs.
"""

import hashlib
import json
from pathlib import Path, PurePosixPath

import pandas as pd
import pytest

from tests.uganda_invariance_reporting import FIELDS, PROPERTY, entry_result

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
    # Resolve only after the existing source-data gate admits this test.
    # The CLI and default terminal reporter never call this fixture.
    return _find_uganda_root()


@pytest.fixture(scope="module")
def uganda_waves():
    from lsms_library import Country
    return [str(w) for w in Country("Uganda", preload_panel_ids=False).waves]


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


def _assert_field(field, actual, baseline_entry, rel_path):
    """The existing scientific assertions, selected one field at a time.

    Keeping them together lets every required check run after a mismatch,
    without introducing a different CLI oracle (ledger1480 §§2–4).
    """
    # Check shape
    if field == "shape" and baseline_entry["shape"] is not None:
        assert actual["shape"] == baseline_entry["shape"], (
            f"{rel_path}: shape mismatch: {actual['shape']} != {baseline_entry['shape']}"
        )

    # Check columns
    if field == "columns" and baseline_entry["columns"] is not None:
        assert actual["columns"] == baseline_entry["columns"], (
            f"{rel_path}: column mismatch:\n"
            f"  actual:   {actual['columns']}\n"
            f"  expected: {baseline_entry['columns']}"
        )

    # Check index names
    if field == "index_names" and baseline_entry["index_names"] is not None:
        assert actual["index_names"] == baseline_entry["index_names"], (
            f"{rel_path}: index names mismatch:\n"
            f"  actual:   {actual['index_names']}\n"
            f"  expected: {baseline_entry['index_names']}"
        )

    # Check dtypes (after canonicalizing string-family equivalence —
    # see _DTYPE_EQUIVALENCE above).
    if field == "dtypes" and baseline_entry["dtypes"] is not None:
        assert _canonical_dtypes(actual["dtypes"]) == _canonical_dtypes(baseline_entry["dtypes"]), (
            f"{rel_path}: dtype mismatch:\n"
            f"  actual:   {actual['dtypes']}\n"
            f"  expected: {baseline_entry['dtypes']}"
        )

    # Check content hash
    if field == "content_hash" and baseline_entry["content_hash"] is not None:
        accepted_hashes = [baseline_entry["content_hash"],
                           *baseline_entry.get("content_hash_variants", [])]
        assert actual["content_hash"] in accepted_hashes, (
            f"{rel_path}: content hash mismatch (data differs):\n"
            f"  actual:   {actual['content_hash']}\n"
            f"  expected exact hash(es): {accepted_hashes}"
        )



def _unknown_key_reason(rel_path, waves, tables):
    path = PurePosixPath(rel_path)
    parts = path.parts
    if path.is_absolute() or ".." in parts or path.as_posix() != rel_path:
        return "baseline key is not a normalized relative cache path"
    if not ((len(parts) == 2 and parts[0] == "var") or
            (len(parts) == 3 and parts[0] in waves and parts[1] == "_")):
        return "baseline key has an unknown wave/tier layout"
    if path.suffix != ".parquet" or path.stem not in tables:
        return "baseline key names an unknown declared table or file type"
    return None


def inspect_parquet(rel_path, baseline_entry, parquet_path, waves, tables):
    """Read ONE explicitly named file; no root fallback, acquisition or build.

    Result fields count all required comparisons, including those following a
    mismatch. Missing/partial/error entries are explicit gaps (ledger1480 §4).
    """
    from lsms_library.local_tools import read_parquet_cache_waves

    result = entry_result(rel_path, baseline_entry)
    unknown = _unknown_key_reason(rel_path, waves, tables)
    if unknown:
        result.update(status="unknown_key", reason=unknown)
        return result
    if "error" in baseline_entry:
        result.update(status="baseline_error", reason=str(baseline_entry["error"]))
        return result
    absent = sorted(set(FIELDS) - set(baseline_entry))
    if absent:
        result.update(status="baseline_error", reason=f"Baseline fields absent: {absent}")
        return result
    try:
        if parquet_path is None or not parquet_path.is_file():
            result.update(status="missing", reason=f"Parquet not built: {rel_path}")
            return result
        expected = _expected_coverage(rel_path, waves)
        manifest = read_parquet_cache_waves(parquet_path)
        reason = _coverage_skip_reason(rel_path, manifest, expected)
        if reason:
            result.update(status="partial", reason=reason)
            return result
        df = pd.read_parquet(parquet_path, engine="pyarrow")
        manifest = read_parquet_cache_waves(parquet_path)
        reason = _coverage_skip_reason(rel_path, manifest, expected)
        if reason:
            result.update(status="partial", reason=reason)
            return result
        result["coverage_metadata_absent"] = manifest is None
        actual = _fingerprint(df)
        failures = []
        for field in FIELDS:
            if baseline_entry[field] is None:
                continue
            try:
                _assert_field(field, actual, baseline_entry, rel_path)
            except AssertionError as exc:
                result["fields"][field] = "failed"
                failures.append(str(exc))
            else:
                result["fields"][field] = "passed"
        compared = any(value in {"passed", "failed"} for value in result["fields"].values())
        result.update(status=("mismatch" if failures else "matched") if compared else "unpinned",
                      reason="\n".join(failures) or None)
    except Exception as exc:
        result.update(status="error", reason=f"{type(exc).__name__}: {exc}")
    return result


@pytest.mark.parametrize("rel_path", _parquet_ids())
def test_parquet_matches_baseline(uganda_root, uganda_waves, rel_path, record_property):
    """Compare existing caches, preserving the ordinary source-data test gate."""
    from lsms_library import Country

    entry = BASELINE[rel_path]
    parquet_path = uganda_root / rel_path if uganda_root is not None else None
    if parquet_path is not None and not parquet_path.exists():
        from lsms_library.paths import data_root
        data_root.cache_clear()
        parquet_path = data_root("Uganda") / rel_path
    tables = set(Country("Uganda", preload_panel_ids=False).resources["Data Scheme"])
    result = inspect_parquet(rel_path, entry, parquet_path, uganda_waves, tables)
    record_property(PROPERTY, json.dumps(result))
    if result["status"] in {"missing", "partial", "baseline_error", "unpinned"}:
        pytest.skip(result["reason"] or "No pinned comparisons")
    assert result["status"] == "matched", result["reason"]
