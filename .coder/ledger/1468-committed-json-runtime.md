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
* Wave.grab_data's script-generation fallback can call
  _build_wave_script_target before the ordered country fallback. The new
  missing-source controls exercise that real edge separately.
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
Present malformed or non-UTF8 inputs must raise and recover after repair.
Root clarified the initial brief before product edits: preserve legitimate
Design-B YAML-derived maps and readable wave caches, but never preserve
offline publisher execution when JSON is absent. Missing all reader candidates
retains valid wave fallback. Exhausted unproductive waves follow the existing
rebuild failure, without country or wave Make/Python publishers. This is the
committed-SOURCE invariant, not a new blanket missing-JSON refusal.
Committed bytes/mode/mtime must not change. No publisher, Make, source provider
or wave build should run when a supported JSON candidate is present.
No non-panel values/rows/index/attrs or script/Make dispatch changes.

## §5 Reuse and verification plan

Extend the non-DVC JSON route to call load_json_cache, mirroring the default.
At the existing country and Wave publisher execution boundaries, return their
existing no-output/missing shapes for JSON_CACHE_METHODS only. Both readable
wave caches and YAML extraction remain before the Wave generation guard.
New invented-country controls exercise actual dispatch
for both JSON methods, both backends, make present/absent, private no-cache,
read-only config, candidate precedence, errors/retry and legacy wave fallback.
Producer/subprocess/provider tripwires must prove bypass prevention. A healthy
non-panel actual script fallback must still run. Demonstrate the new test's
parent failure without relabeling candidate code as parent. Run only meaningful
new controls and selected existing source-free corrupt-JSON regressions under
private roots/false credentials/network isolation and two loadfile workers.
No broad suite, metadata census or real-country runtime is claimed.

## §6 Questions and Phase3 status

No new scientific choice. The initial missing-reader ambiguity was resolved by
Root's explicit never-publish/keep-valid-YAML ruling before product edits.

Phase3 (2026-11-09): ledger-first df686ac26 preceded runtime/product4a82da400.
Fixture-only bd28786d binds the provider tripwire to data_access, where
get_data_file actually lives. Country runtime bytes are unchanged from4a82.

* OK (§2/§3): existing JSON reader/parser/candidate precedence reused unchanged;
  non-DVC route now uses it. No new parser or JSON policy.
* OK (§4): both actual publisher execution edges guarded for exactly the two
  existing JSON_CACHE_METHODS. No non-panel condition/body/signature changes.
* Actual candidate bd28786d:43new+5existing source-free regressions PASS48,
  0fail/error/skip, pytest7.71s, wrapper12.88s, inner/outer0. Both package and
  config/interpreter identities and false credentials asserted in controller
  plus two loadfile workers, unshare-rn, private empty initial data/config/L1,
  BLAS/build1. This is ordinary targeted testing, not a universal IO sandbox.
* Actual immutable29de counterfactual:22selected,8PASS14FAIL/0error/skip.
  Eight Make-backend present-JSON bypasses, two real Wave script generation
  edges, four country/ordered-wave fallback edges fail the new invariants.
  Baseline source stays clean. These are synthetic tripwires, not evidence
  that actual crosswalks were rewritten in this run.
* Preserved first baseline-v1:22setup errors/0test-function executions from a
  new-fixture wrong-module attribute patch. No product evidence; corrected
  only that fixture binding, without assertion/semantic relaxation.
* OK (§4/§5): read-only JSON bytes/mode/mtime, missing/corrupt/Unicode recovery,
  both backends/make states/private no-cache, candidate precedence, real YAML
  extraction with invented reader leaf, readable wave-cache preservation and
  actual healthy non-panel Python fallback all pass.
* Source scope: only Country._aggregate_wave_data and Wave.grab_data change.
  No fingerprints are excluded. Broad build-hash impact is possible and must
  be measured by release metadata gates; no shared rewarm is authorized.
* No real-country/source/API/Make campaign, metadata census, fullCI or
  independent runtime acceptance is claimed. Root owns separate independent
  review/current integration/release. Source and runtime receipts live at
  B/reports/1468-committed-json-runtime-local-20261109.

## Independent review documentation follow-up

Independent review at51d8 found no P1/P2 and passed10 additional invented controls.
Its nonblocking panel-ids skill caveat is corrected here: both backends read the
existing JSON/parquet candidates first; valid YAML fallback remains when absent,
and offline publishers never run. This commit changes only that skill prose and
this ledger, preserving every tested production/test object. It introduces no
new runtime evidence; exact-final-head API/hash/CI gates remain ROOT-owned.
