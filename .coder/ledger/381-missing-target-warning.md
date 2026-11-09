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

## §7 Approved execution and preserved failed attempts
ROOT reviewed29ff source and authorized the pure selection;2ecb added ten
invented controls alongside26 existing classifier/strict/fallback cases.
Initial focused-v1/22443 outer1 had36setupERROR/0bodies: the external provider
tripwire named nonexistent _try_get_from_worldbank. Sole external correction
used existing _try_get_data_file. No product/oracle changes at that stage.
Focused-v2/5342 outer1 ran35PASS/1FAIL; new all-absent test incorrectly expected
empty rather than the existing named RuntimeError. Product refusal was correct.
1f727 strengthens only that new assertion to require the named private cache,
no output and existing final diagnosis. Affected-v3/66697 outer0 passed the
one affected case (7.11s pytest/13.753s wrapper); prior35 were not replayed.
This proves36distinct cases across35initial+ONEcorrected passes, not one36P
run. All original failed logs/XML/wrappers stay intact. Three exact source/
config identities and zero provider/network tripwires are retained; empty
controller config/data checks passed. Synthetic Make/CSV/parquets only.

## §8 Current integration and measured footprint
Ordinary merge5ad2f832bb624ebbbfdbce9f7531531e13873bd8 has parents actual
50a927d69fcba3497b6956f7c5dff55004d42590 and corrected1f727. All19,248 nonowned
mode/type/blob objects match50. try_make text is exact reviewed29ff;
_read_assumed_fresh_cache exact50; replacing only try_make/classifier docstring
restores the entire50 Country source. Tests/old ledger objects exact1f727.
ROOT granted ONE unchanged metadata collector, twice preflight and private
empty lane, versus closed340c original header/mapSHA6a4b8a7a4be5686908bbfcc24a1cde4c15324f2867f833e52dfbf711928eee96.
50 adds excluded1066 readgate only; no baseline replay. Actual session92345
outer0/130.587s; actual changed hashes table=452, wave=1885, btf=37, fif=0.
All452country/1903wave/37BTF/186FIF keysets and14nonhash fields agree;
18None wave cells retained; errors0/denials0; private data/config files0;
source clean/head unchanged. Complete map and exact changed keys retained.
Final ledger-only child carries measured/tested semantics without replay.

## §9 Release scope / residual / evidence
Addresses#381; do not claim whole closure. Current food dispatch already
builds food_acquired before transform. Original no-source-country one-clear-
notice request remains unverified: no blanket Nepal/Armenia shortcut. Armenia
has held-but-unwired survey data, not uniform absence. Existing final logger
and rebuild refusal remain. No actual-country/native/API/build/corpus/full
suite/shared-cache work or policy/default change. Legacy stderr buffering is
an approved timing tradeoff; actual success/failure bytes remain visible.
Durable evidence: B/reports/381-missing-target-warning-proposal-20261109
(PROPOSAL,SOURCE/CURRENT proofs, NO-DATA-SCOPE, CLOSED-FOCUSED-EVIDENCE,
all3attempts, metadata-current complete comparison/receipt, PR-BODY).
Independent1452 qualified test/source PASS sealed test-carry-v1; current
integration/final ledger review is separate additive admission, no replay.
