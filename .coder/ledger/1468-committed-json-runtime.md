# GH1468 — committed crosswalk runtime routing

## §1 Task and authority

Root's local handoff supersedes the original remote brief's6863 source pin:
base29de1acf35e6eff0cf7c2d47977745fcd0bab6ab, one local implementation owner.
The remote1468 attempt failed before native execution and contributed no code.
Route panel_ids/updated_ids through the existing JSON reader under both build
backends. Do not run the offline publisher merely because make is absent.
Preserve the existing reader's lookup/fallback, error and retry contracts;
non-panel Make/script behavior is outside the fix. No source/API campaign.

## §2 Existing machinery and search

Search tier: ripgrep, git objects/history, direct source/test reads.

* Country._aggregate_wave_data, country.py:5411, owns the runtime router.
  Its non-DVC JSON branch currently calls load_from_waves instead of the
  existing load_json_cache. That bypass can reach run_make_target/try_script.
* Nested load_json_cache, country.py:6009, reads private JSON then committed
  JSON, then its existing parquet variants, then the supported legacy wave
  fallback. Reuse unchanged; do not add another parser or candidate policy.
* Nested load_with_dvc_cache already uses that reader on the default backend.
* Country._compute_panel_ids and lazy properties preserve corrupt/Unicode JSON
  failures as retryable; tests/test_gh1590_wave_failure_manifest.py pins this.
* tests/test_panel_ids_persistence.py and test_gh970_no_json_make.py pin the
  committed-source convention and the legacy missing-crosswalk fallback.
* .coder/ledger/894-panel-ids-design-a.md records design A, prior equality and
  the historical non-DVC wave route. Current1447 tests pin Ethiopia's removal
  of obsolete wave declarations; no1447 or914 per-task ledger exists here.

## §3 Definitions in force

Cite STANDING.md, CLAUDE.md's read-only countries_root and committed crosswalk
sections, plus add-feature/panel-ids/SKILL.md. The latter states:
"The committed JSON is a SOURCE, not a build product."
The existing method names are exactly JSON_CACHE_METHODS, not arbitrary JSON.
No crosswalk contents, household linkage, split-off or scientific policy moves.

Nigeria grounding: this continuing agent's full4019-line read, recorded in own
1502 REPORT.md, is authenticated as the exact current prefix SHA1e190907...
All59 appended lines were personally read before country-code work; current
4078 lines SHA603a6157713ff5352094620158f6f72ab4adb4f11e5f9e97b435ebf65193993d.
The prior read includes LOGBOOK/WAITING/TODO. W2PP/PH food item-code meanings
differ; W4 purchases are30-day events while gifts/own production are7-day.
The live12-month versus PAST6MONTHS recall caveat is held. The older statement
"These three native names are not new Aggregate buckets" is qualified by the
dated two-alias decision; Pepper remains unqualified. Neither is reinterpreted.

## §4 Invariants and explicit limits

The normal JSON reader's private-over-committed candidate precedence stays.
Present malformed or non-UTF8 inputs must raise and recover after repair;
legitimate missing/legacy Design-B behavior stays unchanged. In particular,
this task does not invent a new global missing-crosswalk refusal policy.
Missing all reader candidates retains the existing wave fallback; that is a
different boundary from ignoring a present committed source.
Committed bytes/mode/mtime must not change. No publisher, Make, source provider
or wave build should run when a supported JSON candidate is present.
No non-panel values/rows/index/attrs or script/Make dispatch changes.

## §5 Reuse and verification plan

Extend only the non-DVC JSON route to call load_json_cache, mirroring the
existing default route. New invented-country controls exercise actual dispatch
for both JSON methods, both backends, make present/absent, private no-cache,
read-only config, candidate precedence, errors/retry and legacy wave fallback.
Producer/subprocess/provider tripwires must prove bypass prevention. A healthy
non-panel actual script fallback must still run. Demonstrate the new test's
parent failure without relabeling candidate code as parent. Run only meaningful
new controls and selected existing source-free corrupt-JSON regressions under
private roots/false credentials/network isolation and two loadfile workers.
No broad suite, metadata census or real-country runtime is claimed.

## §6 Questions and Phase3 status

No new scientific choice. Root was notified of the missing-reader fallback
boundary before product edits; existing reader behavior is preserved.
Phase1 ledger delivery precedes product changes. Runtime/review/fullCI are
UNRUN at this checkpoint; Phase3 will be a separate ledger-only commit.
