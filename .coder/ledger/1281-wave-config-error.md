# Prior-Art Ledger — GH #1281, declared-wave configuration failures

Search tier: ripgrep + git. Inherits `STANDING.md`.

## §1 Task, restated

Prevent a source-column KeyError in a declared YAML wave from silently
removing that wave and stamping a partial L2-country parquet as current.
Baseline development: `57d2555855f23739a4beca86297c6d5fa702fcf3`.
The change is shared loading behavior, exercised with synthetic countries;
it changes no country-specific survey policy.

## §2 Existing machinery

| Machinery | Source | Decision / tests |
|---|---|---|
| Per-wave extraction | `country.py:1828`, `Wave.grab_data` | Reuse real YAML and sanctioned CSV reader; GH323 Site4 tests exercise config errors versus unavailable optional sources. |
| Build/concat/cache descent | `country.py:5391`, nested `load_from_waves` / `build_wave` | Extend the KeyError branch only, before legacy fallback, concat and country writes. `tests/test_dvc_caching.py` covers cache routes. |
| Worker exception relay | `_parallel_waves.py`, `prebuild` / `relay` | Reuse original exception propagation; existing parallel-wave tests. |
| Hash invalidation | `Wave._input_hash`, `Country._table_cache_hash`, `_build_registry` | Reuse the fingerprinted orchestrator; no exclusion or manual schema bump. Measure parent/candidate hashes. |
| Metadata census | `slurm_logs/isa_backlog_2026-10-21/1221-followup/hash_census.py` | Reuse against empty data roots with raw-source/network guard; counts are hash coverage, not successful build counts. |

## §3 Definitions and conventions in force

`build_wave` calls a method only when the wave declares it in data_scheme
(`country.py:5419`). An undeclared table is legitimate absence, not a
configuration failure. The GH323/GH675 distinction in `Wave.grab_data`
(`country.py:2010–2192`) already treats malformed configured columns
differently from an unavailable optional source. Preserve that distinction.
Use canonical schema and I/O per `STANDING.md` sections 3–4.

## §4 Invariants and assumptions

- A failing declared-wave KeyError must propagate before country concat and
  either L2-country write route. Preserve the original error as the cause.
- Keep the existing AttributeError legacy fallback in this narrow change.
- Undeclared waves, healthy waves, empty answers, optional-source degradation,
  subset selection and existing warm reads retain their established meaning.
- A failed rebuild must not overwrite/restamp a previously stale cache.
- The attempted-wave coverage manifest does not prove every wave succeeded;
  metadata alone cannot count historical swallowed KeyErrors.
- This orchestrator is fingerprinted. Hash changes are expected to invalidate
  possibly poisoned old country caches; do not hide them with exclusions.
- Tests use synthetic CSV sources only, private roots and scoped opt-in for
  in-tree fixture sources. No real data acquisition or cache clearing.

## §5 Reuse decision

Extend the existing per-wave exception branch; use contextual RuntimeError
with a chained KeyError rather than a parallel cache metadata mechanism or a
new exception taxonomy. Reuse the existing hash, writer and worker relay.
Tests must demonstrate the failure on the actual baseline and distinguish
healthy/undeclared/empty cases.

## §6 Open questions for the human

None blocks the documented configuration-error fix. A full source-build
census is separate from hash coverage and must be reported with its actual
scope; no corpus-clean claim follows from synthetic verification.

## Phase 3 verification

Pending baseline reproduction, candidate tests, hash impact and independent
review. The ledger does not claim the issue is fixed.
