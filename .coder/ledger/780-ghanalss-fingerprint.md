# Prior-Art Ledger - GH #780 GhanaLSS framework fingerprint

Search tier: ripgrep + git floor, direct metadata-only Python probes.
Baseline: development 8b312a5c6. Inherits STANDING.md.

## §1 Task, restated

Restore framework import fingerprints for GhanaLSS country and wave cache
hashes: its nutrition script reaches compiled regular expressions that the
serializer rejects. Make future failures visible without changing the existing
best-effort cache-hash policy under normal warning filters. No data transformations change.

## §2 Existing machinery

| Symbol | Source | Existing tests | Decision |
|--------|--------|----------------|----------|
| `_ser` | `lsms_library/_build_registry.py:294` | `tests/test_build_transform_hash.py:test_serialiser_rejects_unknown_types` | Extend deterministic type dispatch for compiled patterns |
| `_CONST_TYPES`, `_closure_parts` | `_build_registry.py:327,375` | nested-scope, closure and identity-address tests in same module | Extend recognized constants so standalone patterns are not silently omitted |
| `framework_imports_fingerprint` | `_build_registry.py:431` | dynamically-dispatched helper tests | Reuse closure walk and existing memoization |
| `Wave._input_hash`, `Country._table_cache_hash` | `lsms_library/country.py:839,3330` | fingerprint wiring tests | Extend the two failure catches with warnings |
| `_EXCLUDED_CALLABLES`, `_EXCLUDED_CONSTANTS` | `_build_registry.py:108,201` | cache-mechanism exclusion tests | Reuse exclusion convention if warning helper is needed |

## §3 Definitions & conventions in force

- STANDING.md §3/§4 and CLAUDE.md caching section define pre-finalize cache
  inputs and the existing cache gates; no replacement invalidation mechanism.
- `_ser` docstring requires deterministic serialization and rejection of
  unsupported types rather than leaking identity addresses.
- The existing import-folding comment said "Best-effort; never raises."
  With failure warnings, this is qualified to the normal warning policy;
  caller-selected warnings-as-errors is respected.
- GhanaLSS `_/CONTENTS.org:2054` records nutrition as landed. Its imported
  transformation closure is a legitimate build dependency, not something to
  exclude merely to avoid an exception.

## §4 Invariants & assumptions

- Follow STANDING.md §4. No raw-data reads, parquet writes, DVC commands,
  dependency changes, or cache clears are needed for this metadata-only fix.
- Pattern identity includes pattern text (str or bytes) and flags, with a
  distinct type tag. Previously supported constant serialization stays identical.
- Standalone patterns and patterns nested in containers must both be versioned.
- Under normal warning filters, both wave and country failure paths retain
  their fallback behavior but warn with enough context to identify degraded
  framework fingerprinting. With RuntimeWarning promoted to an error, the
  wave gate raises; the country gate's existing outer catch returns None.
- Warning delivery, message wording and any deduplication state must not become
  build inputs or alter healthy-country hashes. Prefer warning-filter machinery
  over new mutable process state.
- Non-Ghana fingerprints may legitimately move if previously omitted standalone
  patterns become covered; measure, explain, and report rather than assume.

## §5 Reuse decision

| Quantity | Decision | Reason |
|----------|----------|--------|
| Pattern fingerprint | Extend `_ser` / `_CONST_TYPES` | Existing deterministic serializer owns every other constant |
| Country/wave cache hash | Reuse | Existing gates already consume the missing framework fingerprint |
| Failure notification | Extend catches | Preserve best-effort loading while exposing lost protection |
| Regression tests | Extend `tests/test_build_transform_hash.py` | Existing module owns serialization, closure reachability and hash wiring |

Metadata baseline is measured before edits for GhanaLSS, Uganda, Malawi,
Nigeria, Ethiopia and Niger in `/tmp/780-hash-before.json`. This file is an
untracked execution artifact outside the worktree, not a committed test oracle.
Verification will compare fresh-process results and test text/flags sensitivity,
standalone pattern reachability, both warning gates and Ghana's real closure.

## §6 Open questions for the human

Parent authorized implementation after surfacing the ledger. No commits are
authorized. Standalone pattern recognition also versions the previously omitted
`local_tools._DCT_VAR_RE`: the five control countries all move hashes once.
Parent has been notified of this broader, correct invalidation consequence.

### Phase 3 - verification

- `_ser` / `_CONST_TYPES`: OK (anchored on §2-§4). Pattern text, flags,
  str/bytes distinction, nested serialization and standalone closure tests pass.
- Hash gates: OK (§3-§4). Both failure injections warn and return a hash; no
  mutable warning state or new helper enters the closure.
- Metadata-only country closure sweep: OK (§4). Ghana now produces a real
  fingerprint and all shipped configured-country closures serialize.
- Hash control comparison: five countries (Uganda, Malawi, Nigeria, Ethiopia,
  Niger) change because standalone `_DCT_VAR_RE` is newly covered. Disabling
  ONLY `re.Pattern` in `_CONST_TYPES` in-process restores both framework and
  roster country hashes byte-for-byte for every control. This establishes that
  the serializer extension and warning edits do not cause unrelated drift.
- Final targeted run: 33 passed in 21.55 seconds, including real Ghana
  cross-process/PYTHONHASHSEED determinism. Command: `LSMS_BUILD_WORKERS=1
  PYTHONPATH=/local/job38734650/worktrees/ghanalss-780 taskset -c 8-15
  /local/job38734650/mirrors/LSMS_Library/.venv/bin/python -m pytest
  tests/test_build_transform_hash.py --no-purge -q`.
- No data caches were cleared or built; no source data was read.

-- Sue

### Strict warning policy - review clarification

The review reproduced an overstatement in the unconditional "never raises"
claim. The parent explicitly chose to respect standard caller-selected
warnings-as-errors rather than suppress warning exceptions. Under normal
warning filters the partial hash is retained; in strict mode the wave gate
raises RuntimeWarning and the country gate's existing outer catch returns
None. Two focused regression cases pin those behaviors. This clarification
changes comments and tests, not warning delivery or serialization logic.

Post-review targeted verification: 35 passed in 22.14 seconds, including both
strict-warning cases. Same pytest command as above with taskset cores 0-7;
log `/tmp/780-review-strict-tests.log`. `git diff --check` passed. No unresolved
review blocker under the clarified normal-warning contract.
