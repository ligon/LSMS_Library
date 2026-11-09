# GH381 — proposed exact-target diagnostic correction, UNRUN

## §1 Task / base
Source-only draft off79f5979cf8022e64928c5150950dfb00456994da/tree69f8aefb.
Follow ROOT381-BOUNDED-FIX-BRIEF; no runtime before source review. Fresh381
issue still describes historical food-Make-first, but currentCountry7559+
starts from food_acquired (STANDING §2). Do not restore that old premise or
create a no-data-country/food semantic special case. No production incidence
claim from planned invented fixtures.

## §2 Machinery / git+rg floor
- country._make_target_is_missing207: exact quoted requested target, refuses
  prerequisite/needed-by, nested/error/unknown extra stderr and unfamiliar
  language. Reuse unchanged predicate (no new parser). Existing13 classifier
  controls in test_gh1288_make_failed_recipe.py202+.
- _aggregate_wave_data.run_make_target.try_make: ordered targets, Make-first
  then script fallback. Currently only strict directcountry captures stderr;
  exact absence falls through to the same Makefile-execution-failed warning.
- _reemit_make_stderr235 retains nativebytes/text-only escaped output. Reuse
  for actual success/failure diagnostics, never decode actual stderr away.
- run_make_target final outputNone logger warning6174 and caller rebuild error
  are existing all-absent diagnosis. Preserve; no completed-coverage inference.
- Existing GH1288 strict guard rejects genuine failed country recipe BEFORE
  alternate/cache/Python return; preserve RuntimeError from originalCPE.
- FakeFakeland/GH1039 and GH1288 tests provide private actualMake/scripts.
  Existing missing-target-alternate test explicitly pins old warnings and
  stderr, requiring the narrow intentional expectation change below.

## §3 Authority / existing contract
STANDING §§3–4/CLAUDE cache invariants and ledgers1288-make-failed-recipe,
1590-wave-failure-manifest apply. Former direct strict refusal is a deliberate
exception to legacy fallback; this task changes exact absence diagnostics,
not that policy. Wave coverage, write receipts, private transactions, JSON
publishers, output authority, cache eviction and food default/strict behavior
remain untouched. No source/hash exclusions or schema bump.

## §4 Minimal proposal and bounds
Capture try_make stderr on all its existing routes, stdout stays streamed.
Keep existing C-locale setting strict-only: legacy subprocess environment
unchanged; unfamiliar/non-English stderr remains loud/fails conservative
classification. Classify exact target absence BEFORE replay/warning; debug
log it and continue same ordered candidates. Every other CPE re-emits its
original stderr; strict raises originalcontext/cause; legacy retains warning
and fallback. Missing executable retains its existing warning/fallback.
Successful Make stderr re-emits exactly as before; final all-absent existing
logger warning remains. No deferred diagnostic engine or Make preprobe added.

## §5 Named expectation change / future finite tests
Only test_missing_requested_target_keeps_real_intree_alternate changes old
len(seen)==3/per-targetwarning and No-rule-stderr assertions to no exact-
absence warning/stderr, keeping ALLtarget-order/output/path/values/no-intree
checks. Existing missingMake warning stays; classifier13/strictrecipe and
unknown/nonUTF8/success-stderr assertions remain unchanged. Planned tiny
actualMake controls: healthy alternate and Python after exactmissing; all
absent retains named finaldiagnostic; legacy vs strict realrecipe/prereq/
nested/unknown-extra stderr retain evidence; missingMake; healthyYAML never
runsMake. Both default/make modes where applicable. No tests run yet.

## §6 Review boundary
ROOT must review frozen source and exactselection before tests. The tradeoff
is buffering/re-emitting stderr after each subprocess rather than streaming
it live for legacy calls; bytes/content remain visible for genuine failures.
No locale/decoder/science/default/data behavior change is proposed. Hash cost
of changed tagged orchestrator must be measured if/when authorized later;
no census or new exclusion in this preparation grant.
