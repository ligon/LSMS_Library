# GH #1010 completed old-versus-fixed ordered replay

This addendum completes the old replay that was still running in the independently reviewed report checkpoint. The original report and checkpoint files remain unchanged. All three original row-count assertions passed in both runs; the food acquisition row count also matched. This establishes repeated unnecessary rebuilds under the old helper, not different scientific results.

| API | Before seconds | Fixed seconds | Rows | Before wave builds | Fixed wave builds |
|---|---:|---:|---:|---:|---:|
| `_country()` | 329.27 | 24.20 | 5,340,606 | 7 | 0 |
| `food_prices(unitvalue)` | 327.56 | 31.01 | 1,676,093 | 7 | 0 |
| `food_prices(unitprice)` | 321.85 | 24.98 | 189,656 | 7 | 0 |
| `food_quantities(units)` | 332.68 | 29.57 | 1,694,623 | 7 | 0 |

Before: 1311.35 seconds across four API calls and 28 completed Ghana wave-build subprocesses. Fixed: 109.76 seconds and zero Ghana wave-build subprocesses. These are diagnostic timings on private copies with shared read-through L1, not a controlled cross-machine performance benchmark.

The old helper leaves `LSMS_NO_CACHE=1`; the fixed helper restores absence. In both runs, expected/stored food hash, full seven-wave manifest, and private root stay constant at every observed boundary. The 88 original source files were checked unchanged again after completion. No cache clear, fixture change, assertion relaxation, production edit, timeout increase, or new skip was used.

Exact complete raw-log and event-stream hashes/byte counts are in `OLD_REPLAY_FINAL.json`; those raw artifacts remain private. The corrected whole-job CI comparison and Uganda scientific hold remain separate gates. No #941 causation or runner-level complete-resolution claim follows.
