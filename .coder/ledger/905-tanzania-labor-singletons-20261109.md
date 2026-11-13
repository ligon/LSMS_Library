# #905 Tanzania labor singleton reduction — 2026-11-09

Search tier: git + ripgrep + stdlib AST floor; no GitNexus/runtime used.
Inherits STANDING.md §§2–4, AGENTS.md (CLAUDE.md is byte-identical), canonical
lsms_library/data_info.yml and 905-tanzania-public-extraction.md. The shared
prior-art-ledger skill is absent from the isolated dispatch home; initially
followed the repository README/TEMPLATE ledger workflow. Coordinator later
named /global/home/users/ligon/.sucoder/skills/prior-art-ledger/SKILL.md;
that exact instruction text was read, without traversing native state. Its
ledger-first / separately committed final ledger discipline applies. The
profiling skill was read; all project execution is expressly deferred.

## §1 Task

Optimize ONLY the duplicate-key reduction in plot_labor_for_wave, originally
tanzania.py:1995–2001. Reuse PR #1682's reviewed singleton strategy: grouped
numeric results may serve singletons; EVERY multirow PersonDays/Wage group
retains its original scalar Series.sum(min_count=1) sequence. Do not redo the
pending plot-input patch. ROOT combines accepted labor work into that PR.

Initial full HEAD: 4381d701ffb06ab447a37c66362119d1c1e23009. Asserted top level:
/local/job41080223/mirrors/dispatch_dfb9b018b16e4bbf8fa1a2b7199cd968.
First country read covered all 3,156 lines, including LOGBOOK/WAITING/TODO;
CONTENTS.org SHA256 193a99008a5a7681c267ce5337773e7cb61386c4d1071ce1042c3c19d47be44a.
Module SHA256 138fd458056125ccb354c620591311756595789277e520e24b05d62ad87b3040;
labor function SHA256 9fcea5ddf45d86206c2873676d229c9a7c106d1768f31d63875f167930a2b9b7.
Commit this ledger before changing production or tests; final work separately.

## §2 Existing machinery

| Source | Behavior / evidence | Decision |
|---|---|---|
| Tanzania/_/tanzania.py:1901 _clamp_days | Float64 numeric coercion; [0,730] day clamp | Keep exact |
| Tanzania/_/tanzania.py:1908 plot_labor_for_wave | hired/family construction then conditional duplicate reduction | Change reduction only |
| Tanzania/{2019-20,2020-21}/_/plot_labor.py | two direct production callers; sdd_hhid/plotnum vs y5_hhid/plot_id | Keep exact |
| tests/test_gh905_tanzania_signed_sums.py | independently frozen public SHA c61473b1…; four-ULP comparisons; signed [1e16,1,1,-1e16] | Preserve file, digest and bound |
| tests/test_gh905_tanzania_vectorised_sums.py | actual helper invented-source controls, all-NA and wave-specific labor | Reuse operands, do not edit |
| tests/tanzania_crop_marker_oracle.py | frozen pre1119 input restoration and complete strict comparison | Reuse without changing |
| PR1682 6d188129e4370073e327d0f26036b606c268363e | reviewed input singleton patch / 42 pure controls | Reuse strategy only, leave input bytes exact |

PR source was inspected via the supplied normal CANDIDATE.patch, REPORT.md,
SOURCE-PROOF.json, ACTUAL-SUMMARY.json and FINAL-SOURCE-PROOF.json under
reports/review-tanzania-905-singleton-queue-20261109 in the 20261103 burst.
The commit is absent from this local object store; final proof names 6d188129…
as a ledger-only follow-up to the tested source. Its empty-control correction
retains ValueError("No objects to concatenate"); no healthy-empty policy.

## §3 Definitions and country idiosyncrasies

Use STANDING.md §3 / canonical data_info.yml, without schema changes.
CONTENTS.org says “This inconsistency is unresolved” about UPHI versus r_hhid
grain. UPHI back-casts Round-4 descendant households; four logical rounds share
2008-15; Extended2019 and Refresh2020 are disjoint; WAITING #114 remains live.
The labor helper and both wave script docstrings distinguish 2019 worker roster
IDs from 2020 family DAYS. Never sum the former as person-days. The no-family
days rule is source-documented; it was not separately established by this task.

The country note says “The proposed =plot_inputs_for_wave= and
=plot_labor_for_wave= optimizations are HELD and restored exactly to public
implementations”. Preserve the counterexample and historical claim: the held
proposal replaced multirow sums too. This new bounded candidate leaves every
multirow scalar sequence intact; no scientific ruling or hold is overturned.
All historical failed transport, startup and parity predicates remain failures.

## §4 Invariants and limits

Keep existing groupby(level=['t','i','plot_id','source']) defaults (including
sort/dropna), keys, conditional branch, complete row operands/order, nullable
dtypes, NA masks, axes/domains, attrs and refusal behavior. No clamps, eligibility,
family/hired construction, coercion, defaults, IDs, population or source changes.
All-NA groups use min_count=1. Empty or wholly gated-out sources retain the
actual pd.concat([]) refusal; missing-column errors remain errors.

Dispatch is SOURCE ONLY: no tests/collection, dependency/project imports,
venv/image/FUSE/apptainer/unshare, native/header/row inspection, providers,
Country/Feature, Make producers, acquisition, DVC/cache work, full audit or
source/GitHub push. Stdlib parsing and git/text comparisons only. Runtime JSON
confirms no LSMS runtime was staged. No agents. Publish only private results.

## §5 Reuse decision and proposed verification

Reuse grouped numeric sums followed by a scalar overlay on all repeated keys.
Select duplicates with index.duplicated(keep=False) and retain the exact old
groupby defaults on both routes. No special case for positive/finite values.
Keep numeric column order PersonDays then Wage. The overlay includes every
duplicate group, even all-NA/infinite/cancellation cases, without prefiltering.

New uniquely named tests will reverse only this labor block and require the
exact full dispatch-base SHA above; restore pre1119 inputs and legacy livestock
to verify the independently frozen public SHA too. Compare actual helpers on
invented inputs, both wave colmaps, one repeated key plus many singletons,
signed/permuted/mixed/NA/zero/infinite populations, duplicate scalar operands,
unique path, raw-frame conservation, full output metadata and exact refusals.
Keep the inherited four-ULP assertion and stronger exact comparisons.
Prove complete executable AST outside the reduction block and source prefix/
suffix unchanged, plus nonowned git mode/type/blob identity and notes prefix.

Blast radius: two modern labor producer callers; the module itself is a
country-wide build input, so budget Tanzania-wide invalidation. No live hash
census or native/runtime/performance claim. All pure execution belongs to ROOT.

## §6 Questions / integration constraints

No scientific question blocks the scoped patch. Existing
tests/test_gh905_tanzania_signed_sums.py:32 load_old() restores inputs/livestock
only (:48) and checks the whole module against OLD_PUBLIC_SHA256 at :49.
Callers: test_actual_helpers_preserve_adverse_population (:99; twelve cases)
and test_repeated_coded_record_raises (:172; one case). Its external-reference
branch (:33–40) also requires that same old digest. The input restoration in
tests/tanzania_crop_marker_oracle.py:24–31 replaces plot_inputs_for_wave only.
A labor edit therefore makes the unchanged standalone reconstruction fail;
this is a source-predicted check failure, not an executed test result. Never
edit/repin the loader here. ROOT must review exact restoration when combining
PR1682, whose focused test also inverses only inputs before its full-base hash.
Own tests first inverse ONLY labor, verify dispatch-base SHA138fd458…, then
apply existing input/livestock restoration and verify the unchanged historical
public SHAc61473b1…. No collection or fixture execution is authorized here.

### Phase 3 verification

Source-only tranche prepared; ROOT numerical/integration acceptance is pending.
Code/test/note commit 9f85cba6c2bb0b29ee1c592e70c0042b2733b98b follows the
ledger-first commit c3e3655a6. This final ledger is committed separately.

- plot_labor_for_wave — OK (anchored on §§2,4,5), source scope: inverse of the
  sole reduction branch recovers exact full dispatch-base source; executable
  AST, byte prefix and suffix outside it are exact. Every multirow group uses
  the old scalar reducer dict, retaining its operands; no scientific choice.
- Reuse — OK (§§2,5): same local singleton strategy independently reviewed in
  PR1682; no shared numeric abstraction introduced and no input patch copied.
- Focused tests — OK (§§4,5), prepared only: 43 cases by static source count,
  full-frame/input/refusal checks, signed/NA/infinity/permutations and callback
  operands. All numeric, callback and refusal executions remain UNRUN.
- Frozen historical reference — OK (§§2,6), source reconstruction only: exact
  c61473b1… digest retained; every old fixture/oracle/assertion unchanged.
  Existing standalone loader is an explicit integration gate, not a green test.
- Country documentation — OK (§3): original 3156-line prefix is exact; append
  qualifies candidate and all unrun claims. All prior failed predicates remain.
- Scope — OK (§4): only labor block, uniquely named tests, this ledger and the
  dated note changed. No framework/schema/registry/other-country edits.

Proposed finite selection: new focused module (43 cases) plus unchanged
tests/test_gh905_tanzania_vectorised_sums.py (7 cases), 50 total by static
source count, uncollected. Existing signed905's 13 cases are a separate pending
gate until ROOT reviews exact old-source delivery/restoration; do not claim
their omission as whole905 acceptance. Neither native/API/performance nor
build-hash/footprint checks ran. Tanzania-wide hash coupling is inherited.

Initial named skill lookup failures, missing local PR1682 git object, and the
Python3.6 AST probe failure (no end_lineno) are retained in private evidence.
Corrected /usr/bin/python3.12 stdlib parsing succeeded. No project/dependency
imports, tests or collection occurred. Full API audit deferred under the
source-only stop-list; skill descriptions enumerated as text. Five private
0600 single-link artifacts record source proof, decisions, unrun gates and
handoff. No native/shared cache state was inspected or changed.
