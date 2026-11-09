# GH1440 — cold market loader route

**Search tier:** git + ripgrep floor. No GitNexus tools are exposed. Initial source is exact `11714b185fe2c90f1bc6c60164aa40d358acf0f3` on the dispatched private branch.

## §1 Task, restated

Diagnose why `market=` can fail cold when a country declares script-built `cluster_features`, although an explicit country-level `cluster_features()` call makes the same lookup work. Reproduce only with invented private providers/frames through actual Country market helpers and Country/Feature dispatch. If proved, make the smallest market-loader callsite repair. No real-country API, sources, acquisition or builds; no Make transaction/freshness change (ROOT1288), geography inheritance (1478/1441), country metadata or scientific defaults.

## §2 Existing machinery

| Symbol / evidence | Anchor | Role / reuse and tests |
|---|---|---|
| `Country._market_lookup` | `lsms_library/country.py:3161` | Direct per-wave `w.cluster_features()` scan, warns/skips FileNotFoundError/KeyError/ValueError; all-empty raises RuntimeError; maps raw geography using `_categorical_key_column`, then caches `(t,v)->m`. Existing `test_categorical_key_resolver.py::test_market_lookup_honours_the_declared_key`. |
| `Wave.grab_data` script branch | `country.py:2293-2378` | Cold no country Makefile: warning + empty/missing, even when a wave script exists. Warm wave parquet reads before that branch. Read-only diagnosis; DO NOT edit. |
| `Country.load_dataframe_with_dvc` / nested `load_from_waves` | `country.py:5758-5910` | Country route has an existing legacy fallback for an empty script wave; `run_make_target` can run declared wave scripts without a Makefile. Reuse this loader, never implement another Make path. |
| generated `Country.__getattr__` method | `country.py:6773,7101` | Dispatches registered tables and then calls `_add_market_index`. Actual methods on invented providers allowed. |
| `_add_market_index`, `_join_v_from_sample`, `_report_unlabelled_markets` | `country.py:3430,3244,3575` | HH label priority; full `(t,v)` fallback; retain NA `m`, report through Site I; explicit attrs copy. Existing `test_gh1592_market_sample_refusal.py`, `test_add_market_index_keeps_unlabelled.py`, `test_add_market_index_dedup.py`. Reuse unchanged. |
| `Feature.__call__` | `lsms_library/feature.py:1043+` | Country axis and loud per-country error degrade; current forwarding/index assembly stays unchanged. New invented providers must test this boundary. |
| prior market refusal work | `.coder/ledger/1592-market-sample-unavailable.md` | Already integrated: no-v + unavailable sample preserves its original exception; with-v fallback attempts cluster labels. Not GH1440's loader fix. |
| issue snapshot | prepared `assets/context/current-issue.json` | Open GH1440 / zero comments: cold `cluster_features returned no data` / Makefile warning; explicit country build enables warm. No live GitHub action authorized. |
| earlier receipts | `reports/framework/{before,after}/Pakistan.log` | Both logs show script's honest missing cluster warning; not proof GH1440 is repaired at current source. Read-only, no receipt replay. |

## §3 Definitions and conventions in force

Inherit `.coder/ledger/STANDING.md` §§3–4; cite `CLAUDE.md` sections “Derived Values”, “sample() and Cluster Identity”, “The minted m level”, and `lsms_library/data_info.yml` rather than duplicating definitions. Sample owns `(i,t)->v`; cluster features owns `(t,v)->geography`. `m` is added on request, never baked into cached tables. Build/read composition depending on cache state is a defect, not a derivation.

Pakistan's FULL `_/CONTENTS.org` was read first, including all headings (no LOGBOOK/WAITING/TODO entries beyond the TODO declaration). Actual idiosyncrasy: “Cluster =2202029= (a single-household cluster) is entirely absent from REGIONS.DTA”; its “=Region=/=Rural= are left ~<NA>~” and the hollow-row policy leaves 300 of 301 clusters. No label inference is authorized. Its documented wave-script loader agrees with the script/no-Make hypothesis; no contradiction has been established. The weight delivery gap and intentionally unwired education/shocks are unrelated and untouched.

Instructions read: actual AGENTS.md and identical-byte CLAUDE.md, full Pakistan CONTENTS, repo ledger README/TEMPLATE/STANDING, prepared shared prior-art skill, cross-country-features skill, sample skill, feature audit README. The sample skill says absent-v fallback uses household_roster; actual current `_add_market_index` uses sample. This is a quoted prose/code discrepancy, not authority to change geography or edit that skill.

## §4 Invariants and assumptions

Preserve household-label priority, genuine source identity, full time/cluster keys, country axis, tall row multiplicity, nullable labels/values, input immutability, attrs (especially id_converted), warnings and original refusal behavior. Keep current categorical-key semantics and avoid applying a mapping twice to already finalized data. Do not fabricate a missing lookup/market, swallow failed declared loaders as absent, rebuild tables on a no-market call, or introduce a new writer. Existing assertions/fixtures/skips/tolerances stay intact.

Prepared mounted PROJECT_PYTHON was asserted from exact cwd with explicit PYTHONPATH, LSMS_COUNTRIES_ROOT, private LSMS config/data and DVC_SITE_CACHE_DIR before import. Credentials absent; BLAS/OMP/MKL/build workers = 1; pytest maximum 2 with loadfile and logfiles. HOME/XDG/CODEX_HOME/SDK guards remain unchanged. No real providers, native state or shared cache operations.

## §5 Reuse decision / checkpoint

Reuse the existing declared Country loader where the direct Wave route lacks script fallback. Do not duplicate a resolved fix: first write a failing invented cold→explicit-load→warm witness at current source. Preserve the raw wave categorical contract on existing healthy routes and the finalized Country contract on the repaired route. Exact mechanical callsite choice depends on that witness and existing fixture compatibility. No source or performance/hash-cost claim follows from static inspection.

## §6 Questions / authorization

No routine approval needed: the dispatch charter explicitly authorizes bounded implementation after this surfaced ledger checkpoint. If a correct repair requires reserved Make/freshness edits, stop only that product part and deliver the counterexample and independent tests. ROOT owns independent review, CI, integration and GitHub/merge. No subagents.

## Phase 3 — verification (pending)

UNRUN: new invented failing witness, candidate controls, existing focused source-free regressions, final strict API audit and exhaustive source-byte/scope proof. `make api-audit STRICT=1` failed (exit 2, Poetry absent); its actual prepared-Python recipe `scripts/api_surface_audit.py --strict` passed (exit 0), all 15 skills listed. Runtime import identity passed (exit 0). Evidence files remain UNFINISHED until actual gates finish.

## Phase 3 — final anchored verification / code freeze

Code/tests frozen separately at `72046bf11b4dac00ee4c37dae96cb8e48fa3d9a9`. Only `Country._market_lookup` changes in production. When a declared script wave returns empty without a YAML extraction block, reuse `self.cluster_features()` once for all waves, outside the existing per-wave skip catch. A returned Country frame is already categorically mapped; the old raw-wave mapping runs only on the old route. Empty/absent YAML waves do not acquire another build path. The public dispatch, Make transaction/freshness functions, sample joins, geography and every country configuration remain unchanged.

- **OK (§§2/5, reuse):** invented private country uses actual Wave.grab_data's no-Makefile branch, actual Country table dispatch/categorical mapping/market helper, and actual Feature assembly. A fake in-memory producer replaces only the Country build boundary. No actual source, API, Make or parquet writer executes. This diagnoses the route, not real Pakistan source correctness.
- **OK (§§3/4, identity/NA):** 29 new focused controls cover explicit/no market, cold vs explicit-load→warm exact frames, sample-first labels, unchanged YAML route, mixed warm/cold waves, one declared all-wave load/no extra production, distinct lookup-column caches, full `(t,v)` keys, country axis, tall-row multiplicity, nullable values/labels/time/cluster keys, attrs and input immutability. A non-idempotent declared-key mapping proves Country labels are not mapped twice. Empty/absent lookup never creates a label; failures do not poison lookup cache. Existing wave FileNotFoundError/KeyError/ValueError warning/skip and Country producer exception identity/Feature degrade/strict NA contracts are pinned.
- **OK (§4, failure evidence):** first baseline run had 4 failures, including a fixture bug (invented waves not declared). That receipt remains. After correcting only the NEW fixture, unchanged source gave 3 expected cold-route failures and 1 passing explicit-load/warm control. Candidate first focused run: 23 passed. Final selection: **122 passed, 0 failed/skipped/deselected**, including all 29 new controls and 93 existing source-free controls, in 12.52 s pytest. All pre-existing tests/assertions/fixtures/markers/tolerances remain byte-identical. The real-country-only dedup suite was not run; tall-row checks are invented here.
- **OK (§§2/4, source scope):** read-only git/tree and byte proof at code freeze compares 13,530 production tracked entries: 13,529 unchanged, only country.py changed. All 13,471 country config entries and 508 pre-existing test entries unchanged. Of 203 callables in country.py, only Country._market_lookup differs; the entire module outside that method is byte-identical. Reserved run_make_target/aggregate/Wave.grab_data, sample/market joins and categorical mapping functions match source. Final ledger-only commit must preserve the code-freeze tree outside this ledger.
- **OK (§4, runtime/API):** exact cwd/PYTHONPATH/package/config roots and mounted interpreter identity passed, absent credentials, private data/DVC site cache, BLAS/OMP/MKL/build1, pytest2 loadfile/logfiles. New-test source/write/subprocess denial seams were not hit and fixture teardown asserts zero parquet outputs. make api-audit STRICT=1 failed exit 2 because Poetry is absent; actual prepared-Python strict recipe passed before and after code, all 15 skills listed, no untracked hashed inputs. git diff --check passed.

No contradiction with Pakistan's missing-geography documentation was found; no new country fact was inferred. The sample skill's roster-vs-sample discrepancy remains documented, not silently edited. Build/cache fingerprint costs, performance, real Pakistan/native/source/warm acceptance, independent review and full CI are **UNRUN**. ROOT retains those gates and integration/GitHub/merge ownership. Exact commands, failure receipts, final SHA, scope proof and unexecuted gates are in the five required evidence artifacts; task outcome completed for this bounded lane, coordinator acceptance pending.
