# Prior-Art Ledger — GH1606 stage-aware dtype diagnostics

Search: Git/ripgrep/AST floor; no GitNexus service. Inherits STANDING.md.

**REOPENED 2026-11-08 — finalized datetime fidelity follow-up.** Current base
b284112edee7dcc764415bb4e69a7d1814cf512b. ROOT's guarded metadata/invented probe
shows canonical datetime versus local str misgraded as finalized string. Prior
ROOT-supported limited-dtype assumption was incomplete for the original full
canonical-after-local requirement. Earlier tests/CI/merge receipts below remain
historical, not proof of datetime fidelity. This follow-up uses ONLY actual global
schema plus generic Alpha/Beta invented carriers; no real-country fixture/source
or Country construction. ROOT explicitly authorized recognition, not date policy.
Base ebb87135cdd11a63e3b463b6b21c3e556c628a8d. ROOT read/approved the external
charter, checker/canonical-loader and selected contract before implementation.
AGENTS full prior read authenticated SHA b5ea7b495528b61727e52df306fcd481ab8120b3f6d937c0c18c867aea47993a;
CLAUDE is its symlink. Initial comparison mistakenly compared symlink Git blob
to resolved file and stopped before any ledger/product/test action; corrected
identity proves exact same file, no instruction changes.

## §1 Task

Explicit stage="native" (legacy default) or "finalized" for rough dtype grading.
Known public API adapters explicitly finalized. ALL currently declared global
rough types override local, including GLOBAL datetime through the existing pandas
dtype predicate. Unknown FUTURE global types retain local grading. Native local
map/behavior remains exact legacy; no global datetime entry in _DTYPE_MAP. Canonical-only
present columns checked only in dtype check, even absent local feature; never
required. No data/caster/other-check/source/science/cache changes.

## §2 Existing machinery

| Existing machinery | Base location | Reuse / tests |
|---|---|---|
| _load_scheme | diagnostics.py:194 | local index/type/optional parser unchanged |
| _DTYPE_MAP / _check_dtype_consistency | diagnostics.py:248/402 | extend current numeric/string/bool predicates, list/string handling, first-five warn cap |
| _load_canonical_dtypes | country.py:7833 | reuse authoritative global Columns.type parser, no new parser |
| pandas.api.types.is_datetime64_any_dtype | existing pandas dependency | reuse dtype predicate ONLY on finalized GLOBAL datetime; no date coercion |
| _enforce_canonical_dtypes | country.py:7855 | precedence authority ONLY; mutating caster never called |
| is_this_feature_sane | diagnostics.py:716 | add explicit context keyword; native omission exact baseline |
| validate_feature | diagnostics.py:934 | public API result, explicit finalized helper call; test with invented strict fakes |
| scan public result | bench/feature_audit/scan.py:347 | explicit finalized sanity call |
| direct cached sanity | tests/test_table_structure.py:302 | native default remains |
| extra_optional integration | tests/test_coverage_matrix.py:290+ | other Check tuples/optionality independent |
| finalizer month controls | tests/test_gh1535_crop_month_carriers.py | existing contract, no source replay |

## §3 Definitions

STANDING.md §3/§4: cached parquets hold pre-finalize data. Existing canonical
caster says "the canonical schema (data_info.yml) wins over any country-level
override". Finalized GLOBAL datetime must be recognized with
pd.api.types.is_datetime64_any_dtype: naive, timezone-aware, units, NaT and typed
empty retain dtype recognition. No parse/cast/calendar/clamp/value inspection.
Recognition is tracked ONLY for globally declared datetime columns; LOCAL-only
datetime remains unsupported exactly as legacy. Unknown FUTURE global types
retain local grading, preventing a silent unknown-type exemption. The existing
native _DTYPE_MAP remains byte-exact, without a datetime key. Caller declares stage,
never inferred from dtype/attrs/path/country. Other checks receive unchanged local
scheme; SanityReport.ok continues allowing warnings.

## §4 Invariants

Native default does not load canonical schema and yields exact baseline checks.
Finalized reuses loader, never caster. Input values/index/masks/attrs/nested scheme
unchanged. Numeric broad acceptance/object-string/bool/constrained-list/missing
column/unknown type meanings and five-warning cap unchanged. Invalid stage raises
ValueError before schema/provider/data work. Canonical-only absent columns never
become required. No real Country/Feature/getter/API/Make/warm/cache/census/source.
Strict invented adapter/spine fakes; private empty nonsymlink config/data/L1 before
import, false credentials/unshare-rn, mounted MAIN Python/exact import-cwd-root,
BLAS/build1 and pytest2loadfile. No dependencies/shared writes/GitHub/push.
Generic framework scope only; no actual-country semantics or136-source-frame replay.

## §5 Reuse

Reuse canonical loader; extend existing rough grader; explicit stage is new
because callers have ambiguous frame stage. Reuse local scheme unchanged for
all other checks. New meaningful invented controls verify stage/caller/immutability.

## §6 Questions

None blocking: ROOT selected full current canonical priority including datetime,
native default unchanged, canonical-only present checks, and unknown-FUTURE-global
retention. Earlier datetime-as-unsupported fixtures must be explicitly reclassified
to genuinely future types; keep their assertions, add wrong-datetime negatives.
One affected+new guarded cohort only; no old35/full/source/actualCountry/census replay. Hash footprint UNMEASURED; no census grant. Freeze
before one invented focused cohort; independent review after frozen code/tests.

### Phase3 — historical limited-dtype verification; datetime follow-up PENDING

- `_check_dtype_consistency` — OK (§2/§3): reuses the existing canonical loader
  and rough predicates, never caster/parser. Supported global types override only
  for explicit finalized stage; unsupported types retain local grading, including
  known-local/unknown-global adversaries. No REINVENTION or CONTRADICTION.
- `is_this_feature_sane` — OK (§3/§4): native default exact parent parity;
  invalid stage before schema/provider; input/scheme unchanged; only dtype Check
  can change. Canonical-only columns never become required; all other checks
  receive untouched local schema and keep their previous grading.
- `validate_feature` and audit scan — OK (§2/§4): actual adapters explicitly
  finalized on their public result, verified through strict invented providers.
  Existing direct cached-parquet callers remain native by omission.
- Frozen runtime/test authority613073885edf1a3496659a3b3ca038fe108c4f88:
  ONE author selection35 new committed controls plus3 new external closed-parent
  controls,38PASS/0fail/0skip/0deselected in16.56s. Mounted MAIN/private empty
  roots/falsecredentials/unshare/BLASbuild1/2loadfile;3 guard receipts retain13
  active bindings each,0denials,datafiles0. Strict static API audit exit0.
- Independent ROOT review6 new controls PASS/0fail/error/skip,13.224s; runtime613
  clean/operator bytes exact,3early profiles0denials/data0. Historical runner
  label8 is not an actual count:6 controls ran. Author38 were not repeated.
- Independent controls cover actual canonical month metadata with invented
  carriers, native-loader refusal, supported/unsupported mixture, declaration/
  nested-attrs immutability, canonical-only/absent and unchanged allnull/coverage.
- Whole country.py/canonical loader, all configs and existing tests are exact
  base. This own ledger-only finalization leaves EVERY nonledger tracked mode/
  type/blob object exact613; final external byteproof binds the documentation head.
- Operator bookkeeping is preserved externally: instruction check initially
  compared a symlink blob to resolved contents; packet preparation initially
  required private config directory remain totally empty, but dependency import
  created an EMPTY private matplotlib directory (0files). Neither was a test or
  source failure; no product/data/cohort replay or guard relaxation followed.
- No real Country/Feature/source/table/API/Make/warm/census or closed136-frame
  replay. Metadata hash footprint remains UNMEASURED, full offline/configured CI
  and GitHub publication/merge are ROOT-owned pending gates. This is rough dtype
  grading, not exact finalizer/calendar verification or scientific endorsement.

External receipts: reports/1606-stage-aware-dtype-20261108 and independent
reports/review-1606-root-20261108 in the isa-sm-20261103 burst workspace.

### ROOT current-composition release verification (2026-11-08)

The reviewed runtime613 and final authora08 were combined with development
a799ce58, retaining all 19,118 mode/type/blob entries in the exact disjoint
union. Incoming #1549 changes only two docstrings and its own ledger.
The resulting frozen build is c8d1b5431b9cde8dcaeba74594dcad23ffb0556e.

The actual candidate-only census completed with zero changed fingerprints
among 450 country, 1,895 wave, 35 build-transform and 186 framework-import
cells. All metadata except the head is exactly the preserved closed a889 map;
the 18 None wave entries and public scope553/2313/39 remain unchanged. The
intervening docstrings and ledger-only changes are separately admitted and
verified; the closed baseline was not replayed. Strict API audit passed.

Full configured offline CI41023422 completed0 on its own mounted environment,
with false credentials, private empty nonsymlink L1 before runner import,
loopback-only namespace, BLAS/build1 and four loadfile workers: 12,161 passed,
5,652 skipped, four xfailed. JUnit has17,817 cases and zero failures/errors;
every prior17,782 case is retained, plus exactly35 new committed cases. The
three external author controls and six independent controls are separate.

The first launch41023382 remains failed126: a copied entry script had mode
0644 and could not execute, before runner or pytest. Bootstrap package import
did occur. Correcting only mode0755 and using a fresh output namespace led
to the completed run above; no tested code or assertions changed.

This final update changes only this task ledger. All nonledger objects remain
exactly the tested current composition. Configured GitHub checks and merge
remain the final publication gate; there is no source-corpus replay, scientific
rule change, native cache rewrite, production-weight change or master release.

### Current follow-up freeze — Phase1

Reuse existing canonical loader and pandas dtype predicate; no new parser,
producer or schema. Preserve all non-dtype checks, severity/order, immutable
frame/scheme, unknown-future fallback and native forbidden-loader behavior.
ROOT probeRESULT at reports/1606-datetime-fidelity-probe-20261108 is reused as
historical diagnosis, not replayed. New scope implements GLOBAL datetime only,
with actual canonical schema/invented carriers; reclassifies prior datetime
unsupported inputs explicitly. Freeze code/tests before one affected/new cohort
under accepted private MAIN/unshare/falsecreds/MPL/XDG/thread1/2loadfile guards.
Independent review/metadata/currentCI/publication remain ROOT gates. No new
runtime success at this ledger-first commit. Prior receipts are immutable.


### ROOT datetime follow-up publication checkpoint — 2026-11-08 13:04 UTC

The datetime follow-up975f9 passed46 focused/affected controls (33new,11affected
companions,2exact-parent comparisons) and strict static API audit. Independent
review passed20 additional controls,13.70s pytest/28.96s wrapper, with unchanged
source-refusal guards and no source/private-data reads. The current declared
canonical type vocabulary is recognized; dormant date/timestamp aliases remain
outside this correction. No parsing, casting, calendar or value validation was
added. The previous limited-type closure remains historical; issue1606 is open.

Current composition e3dc93d0f11f676bb13be11aa6795a20b70a76d3 has parents975f9 andd82,
with the exact19,121-entry source union and no overlapping paths. Candidate-only
guarded metadata and strict API audit passed; every non-head metadata field
and all450/1,895/35/186 fingerprints match the admitted closed1520baseline.
The18None cells and public553/2313/39 inventories are unchanged.

Full configured offlineCI41024398 is RUNNING at this dated checkpoint. Final
results and publication decision belong in the PR validation record and
reports/1606-datetime-current-verification-20261108. This is a historical
checkpoint, not a completion claim. The publication commit changes only this
ledger; all code/test objects remain frozen for the running full suite. No
master release, shared-cache rebuild, production weights or scientific
interpretation changes are authorized here.
