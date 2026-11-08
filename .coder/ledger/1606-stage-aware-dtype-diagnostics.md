# Prior-Art Ledger — GH1606 stage-aware dtype diagnostics

Search: Git/ripgrep/AST floor; no GitNexus service. Inherits STANDING.md.
Base ebb87135cdd11a63e3b463b6b21c3e556c628a8d. ROOT read/approved the external
charter, checker/canonical-loader and selected contract before implementation.
AGENTS full prior read authenticated SHA b5ea7b495528b61727e52df306fcd481ab8120b3f6d937c0c18c867aea47993a;
CLAUDE is its symlink. Initial comparison mistakenly compared symlink Git blob
to resolved file and stopped before any ledger/product/test action; corrected
identity proves exact same file, no instruction changes.

## §1 Task

Explicit stage="native" (legacy default) or "finalized" for rough dtype grading.
Known public API adapters explicitly finalized. Only supported global rough
types override local; unsupported global types retain local grading. Canonical-only
present columns checked only in dtype check, even absent local feature; never
required. No data/caster/other-check/source/science/cache changes.

## §2 Existing machinery

| Existing machinery | Base location | Reuse / tests |
|---|---|---|
| _load_scheme | diagnostics.py:194 | local index/type/optional parser unchanged |
| _DTYPE_MAP / _check_dtype_consistency | diagnostics.py:248/402 | extend current numeric/string/bool predicates, list/string handling, first-five warn cap |
| _load_canonical_dtypes | country.py:7833 | reuse authoritative global Columns.type parser, no new parser |
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
override". This diagnostic mirrors only types recognized by its EXISTING rough
_DTYPE_MAP; no new datetime/exact-dtype system. Unsupported global types retain
local grading, preventing a silent unknown-type exemption. Caller declares stage,
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

None blocking: ROOT selected native default, canonical-only present checks and
unsupported-global retention. Hash footprint UNMEASURED; no census grant. Freeze
before one invented focused cohort; independent review after frozen code/tests.

### Phase3 — pending

No implementation/test success at this ledger-first freeze. Final anchored
verification will be recorded in its own docs-only commit after checks close.
