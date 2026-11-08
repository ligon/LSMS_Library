# Prior-Art Ledger — GH1540 EHCVM condition helpers

Base: b8346cf1f953ac82517b1b7473c715a5655ca950. Search tier: ripgrep + git + static AST; no GitNexus mutation. This older pin is not ROOT's current integrated base. Supplied SOURCE-PROOF.json establishes the relevant object equivalence, not full-tree equivalence. ROOT reports subsequent GH1547/GH1606 integration and owns current-base integration.

## §1 Task, restated

Move duplicated pure crop condition code/label normalization into lsms_library/ehcvm.py, retaining instrument mappings and validation at their existing callers. The issue says “six wave scripts, mali.py and niger.py”; there are five wave-script helper homes plus those two country modules, seven homes. Guinea-Bissau is the sixth wave producer but has no condition helper/output and remains byte-identical. Correct only the misleading global five-answer wording in data_info.yml. No scientific, source, schema, amount, unit, eligibility or row-selection change.

## §2 Existing machinery

| Machinery | Reuse decision |
|---|---|
| Five wave scripts' _crop_conditions | Extract their existing numeric/Int64/map/string kernel; retain each strict mapping read, integer key normalization and assertion locally. |
| Mali ehcvm_crop_conditions | Same pure kernel; retain its ValueError validation, distinct from script assertions. |
| Niger _crop_conditions / _map_codes / _harmonized_codes | Retain local mapping normalization, other _map_codes users and sold helper. Its string-NA map is equivalent on its own supplied maps; verify exact original parity before claiming it. |
| Seven native-label helpers | Reuse their existing Series/string/strip/blank-to-NA operation. Côte d'Ivoire retains its local instrument fallback for codes 6 and 7 and the original numeric-normalization order. |
| Country categorical_mapping.org tables | Leave dictionaries and domains anchored to each instrument: five/seven/three answers are not interchangeable. |
| tests/test_ehcvm_crop_production_condition.py | Reuse finite AST extraction, never execute a producer/country module; extend namespace wiring for the shared pure functions. |
| .coder/ledger/1528-sold-condition.md | Preserve sold-side NA, no-sale, harvest disagreement and explicit payload transport; no replay of its source/API evidence. |
| STANDING.md, CLAUDE.md, feature/audit skills | Inherit interface/grain/access/cache conventions. Task's explicit source-free boundary overrides source/build examples. |

## §3 Definitions & conventions in force

Inherit STANDING.md §§3–4 and lsms_library/data_info.yml crop_production condition / condition_native / Condition_sold / condition_sold_native definitions. A harvest index blank is unknown_condition; a native-label or sold-state column blank is NA. Native answers retain literal wording, not canonical vocabulary. Full eight country CONTENTS.org files, including every LOGBOOK/WAITING/TODO, were personally read before inspecting country code; exact hashes/counts and country-specific notes are in the private country-read receipt.

Burkina's record says “The printed form is not what was fielded.” Mali says “The s16c variable numbers flip between the waves”: sold-state variable numbers cannot be generalized. Côte d'Ivoire vague 2 has seven answers, including Fève and Autre à préciser; neither becomes Grain. Niger 2018 five-answer and 2021 three-answer code 3 differ. Guinea-Bissau's later “Settles the question left open above” records chronology, not permission to change the producer. Togo uses 2018/Data1 and native French crop labels; Senegal's mojibake labels are load-bearing. Benin's 2021 crop builder is not newly wired here.

## §4 Invariants & assumptions

Preserve exact own-code/label domains, normalization, fold, sentinel/NA masks, pandas dtype/index/name/attrs, errors and input immutability. Retain mapping acquisition/validation order and exception types. All executable AST outside the two condition helpers per home and their added pure-helper imports must remain exact; sold helpers and all producer calls/tails remain exact. Guinea producer and all instrument dictionaries remain whole-file exact. No framework/fingerprint exclusion changes. Shared ehcvm.py text has a wider static dependency closure than the seven homes; actual additional hash changes are unmeasured and ROOT-owned.

## §5 Reuse decision and finite verification

Use two small existing pure operations: numeric-to-nullable-string mapping with an explicit caller-supplied dictionary, and literal-label whitespace/blank normalization. Sentinel filling stays in existing harvest wrappers. No parser, global answer dictionary or new default state. Keep private caller names as compatibility wrappers.

Freeze repository-relative original pure helper definitions and literal own-instrument mappings for exact synthetic parity controls; no Git-history lookup during tests. Cover every declared code/label, numeric/nullable/categorical/string carriers, off-code/fraction/missing/empty/typed-empty values, duplicate/nontrivial indexes, literal accents, CIV seven-answer fallback and negative five-answer oracle, sold NA versus harvest sentinel, attrs/errors and immutability. Static tests prove actual caller wiring and Guinea byte identity. Run one frozen meaningful affected/new cohort with byteexact accepted-fast_guard.py, mounted PROJECT_PYTHON, fresh private empty config/data/L1, false credentials, unshare -rn, thread/build limits 1, pytest -n 2 --dist=loadfile; source/API cohorts excluded. One strict static API audit, invoked directly without Make. Preserve failures; no guard/source/dependency bypass. No source coverage, served-row parity, hash census or release claim.

## §6 Open questions for ROOT

No blocking scientific choice is required for this mechanical scope. ROOT owns current-base integration, additional full hash census, release sequencing/rewarm and source/API/Make/Feature/warm verification if desired. No Country/Feature constructor, producer execution, source getter/reader/writer, acquisition, DVC CLI, network or shared-cache write is authorized here. Scope is surfaced through the existing absolute QUESTIONS_FROM_AGENT channel before product edits; proceed without another fixture gate under ROOT's explicit end-to-end authorization.

### Phase 3 — verification

Pending implementation and the single frozen source-free cohort.
