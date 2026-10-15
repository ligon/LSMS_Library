# Prior-Art Ledger — CI Ghana repeated builds

**Search tier used:** ripgrep + git floor; no index mutation.

## §1 Task, restated
Explain the controlled credentialed CI timeout at `test_ghanalss_unitvalue_and_units_modes_are_unaffected`, on the second `food_prices` API call, without changing scientific assertions, production cache policy, or timeout. Reproduce on a physical private GhanaLSS cache copy at cf1840e02317bac052b99e1e6c9fa066a8ca45bc, including preceding test-session environment changes.

## §2 Existing machinery
| symbol | path | purpose | reuse |
|---|---|---|---|
| `_run` / rebuild control | tests/test_collectonly_no_purge.py | Calls actual root pytest_configure with purge spies; changes LSMS_NO_CACHE | inspect teardown |
| `pytest_configure` | conftest.py | Explicit rebuild sets LSMS_NO_CACHE directly | preserve policy |
| `load_dataframe_with_dvc` | lsms_library/country.py | NO_CACHE bypass; country hash and wave manifest gate | observe, no replacement |
| hashless script-wave eviction | country.py; ledger/891-wave-subset-cache.md | Rebuild reruns scripts; NO_CACHE ignores manifest | preserve |
| fingerprint definitions | ledger/780-ghanalss-fingerprint.md | Deterministic build input hashing | avoid observer mutation |
| private path reset | ledger/1003-helper-root-isolation.md | Explicit fixture before/finally clearing cached roots | compare only |

## §3 Definitions & conventions in force
Inherits STANDING.md §3, CLAUDE.md and lsms_library/data_info.yml. GhanaLSS `_/CONTENTS.org` food-acquisition and currency-unit sections distinguish `u='Value'` cedi expenditure from physical mass: such rows cannot have fabricated kg prices. The current source-derived farmgate values cover four waves; historical prose saying only 2016-17 has usable prices predates that addition. The older sentence that the other six waves correctly return empty is not a current acceptance criterion. CONTENTS was read first, including searches for TODO/WAITING/LOGBOOK (only the TODO declaration appears).

## §4 Invariants & assumptions
Per STANDING.md §4 and user AGENTS §3b: isolated physical L2, immutable source cache, shared read-through DVC blobs only; own package/config identity; CPUs16–23, BUILD_WORKERS=1 and BLAS1. Existing counts 1,676,093 unitvalue prices, 189,656 unitprice prices, 1,694,623 native-unit quantities remain exact. Initial probes are unmodified-source; logs contain aggregate metadata only. A passing isolated test does not disprove test-order contamination. The CI stack shows subprocess wait, not proof of deadlock or GH #941 cause.

## §5 Reuse decision
Reuse actual test helper, MonkeyPatch teardown, actual Ghana API and parquet footer metadata. Record external events using private instrumentation rather than wrapping hashed production transformations. If the helper leaks NO_CACHE, repair its test isolation locally and add a failing-before teardown regression. Do not alter runtime cache policy, assertions, fixtures, or dependencies.

## §6 Open questions
None block the authorized diagnosis and narrow test isolation repair. Whether this completely explains runner wall time requires rerunning controlled CI; local evidence alone cannot prove that.

### Phase 3 — verification
Pending.
