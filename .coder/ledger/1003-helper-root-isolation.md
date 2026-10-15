# Prior-Art Ledger — #1003 helper-test root isolation

**Search tier used:** ripgrep + git; framework test task, no country transform.

## §1 Task, restated

Isolate only the two synthetic helper tests in
`tests/test_gh866_stale_country_parquet_not_laundered.py`. The absent-parquet
helper calls a cached `data_root('Uganda')`: an earlier cached root can override
its temporary environment and have its livestock parquet deleted, or its own
temporary root can survive environment teardown. The JSON helper returns before
resolving that root and does not itself cause this poisoning. Keep the delivered
tests on their intended ambient cache. No production cache-policy change and
no attribution to #941.

## §2 Existing machinery

| symbol | source | behavior and coverage | decision |
|---|---|---|---|
| `paths.data_root` | `lsms_library/paths.py:91` | lru-cached per argument; env overrides require explicit `cache_clear`; existing path tests | reuse |
| `_evict_stale_country_cache` | `lsms_library/country.py:3941` | JSON methods early-return; other methods unlink under `data_root(self.name)`; this test file covers it | unchanged |
| `cold_private_root` | `tests/test_gh970_no_json_make.py:57` | clears cached path roots before use and in `finally`; cold JSON regressions | reuse fixture pattern locally |
| `pytest.MonkeyPatch.context` | pytest; used throughout repository | scopes environment restoration independently of the caller | reuse |
| `_warm` and Tier 2 tests | GH866 test file | read intended existing country caches and restore their poisoned test content | unchanged |

## §3 Definitions and conventions in force

- Inherits `STANDING.md` §§3–4 and `CLAUDE.md` cache/path rules.
- `paths.countries_root` docstring: "Cached; call ... cache_clear() after
  changing the env var or config ... exactly like ... data_root."
- `data_root()` and `data_root('Uganda')` are separate lru-cache keys; a restored
  environment alone does not restore either cached value.
- A test-owned dummy `.parquet` file is a filesystem sentinel, not survey data.
  No sanctioned raw-data reader or feature build is needed for this defect.

## §4 Invariants and assumptions

- Entry must ignore a previous root B while using temporary root A.
- After normal or exceptional teardown, both argument forms must resolve the
  restored root B; no A entry may survive.
- A synthetic parquet in B must survive an absent-parquet helper invocation
  scoped to A.
- Apply the fixture explicitly to the two helper tests only, never autouse or
  module-wide. Do not empty or redirect delivered-test caches.
- Countries/config root is not overridden here; do not introduce unrelated
  production/path changes or broaden credentials/skip behavior.

## §5 Reuse decision

Reuse the GH970 before/finally cache-clear pattern with an explicitly scoped
MonkeyPatch context. Add one local fixture, plus a two-root regression with
normal/exceptional exit cases exercising the actual absent helper. Existing
GH970 fixture also stubs Malawi data and subprocesses, so importing that fixture
would pull unrelated behavior into this narrow file. No shared global fixture
or new path abstraction is warranted.

## §6 Open questions for the human

None for this authorized repair. Whether this defect caused any historical CI
failure remains unproved and separate. Parent owns independent review/publishing.

### Phase 3 — verification

Pending targeted execution and discriminatory missing-entry/missing-exit-clear
controls. Frozen discovery source is d7626368f; implementation base is GitHub
development 1973242b4. Aggregate discovery evidence will accompany the fix;
it contains only disposable path names and booleans, no survey rows or credentials.
