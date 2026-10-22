# Prior-art ledger: #1081 derived categorical errors

Root approved this exact three-error boundary after reading the complete diagnosis, proposal, tests and proof on 2026-10-21.
This ledger is committed before the runtime/test edit.
Search tier: git/ripgrep/AST/source inspection, with actual public-handler fault injection.
Base: frozen f8 `f8d236694c26fe9e709c5548f02c70f74ea72c33`.
Inherits `.coder/ledger/STANDING.md` sections2-4, `CLAUDE.md` categorical/derived contracts and `lsms_library/data_info.yml`.

## 1. Task, restated

Preserve the identity and actionable parser context of existing typed categorical defects through the generated food and roster derived handlers. Re-raise EmptyCategoricalTableError and CategoricalMappingConflictError before their generic legacy fallback catches; include the explicitly approved CategoricalKeyDeclarationError under its identical non-fallthrough contract. Do not change healthy derived values or unrelated legacy/UnitsUnavailable/label-selection policies.

## 2. Existing machinery

| Machinery | Current anchor | Tested/decision |
|---|---|---|
| `_FOOD_DERIVED` / `_ROSTER_DERIVED` | country.py:6069 /6076 | Existing runtime dispatch; reuse unchanged |
| Generated food derive try / broad catch | country.py:6267 /6292 | Actual public-handler probes, existing units Country tests; extend only typed re-raise |
| Existing UnitsUnavailable branch | country.py:6276 | `test_food_prices_units_kwarg.py` Country controls; reuse byte-exact |
| Generated roster derive try / broad catch | country.py:6351 /6362 | Public household_characteristics probes; extend only typed re-raise |
| `_aggregate_wave_data` | country.py:4980;5829 | Source materialization/finalization; broad issue logger re-raises unchanged; reuse without signature/body changes |
| `_rebuild_failure_error` | country.py:909 | Constructs generic legacy error; used by diagnosis/negative tests, leave unchanged |
| `EmptyCategoricalTableError` | local_tools.py:2239 | `test_categorical_mapping_empty_table.py`; reuse existing class/message/cause |
| `CategoricalMappingConflictError` | local_tools.py:2183 | `test_categorical_mapping_resolution.py`; reuse existing class/context |
| `CategoricalKeyDeclarationError` | local_tools.py:2225 | `test_categorical_key_resolver.py`; approved explicit third-type scope |
| Org cascade / level reader | local_tools.py:2418 /2308 | Existing parser and resolution tests; reuse for real exception fixtures, no parser edits |
| `_split_labels_arg` and post-try `_relabel_j` | generated handler country.py:6156 /6330 | Scalar Aggregate differs from mapping dict; preserve contract |
| Existing fault-injection test idiom | test_food_prices_units_kwarg.py:country_without | Patch aggregation and use real generated public handler; reuse |

## 3. Definitions and conventions in force

- CLAUDE.md:377-381: within-level conflict and declared-empty table are errors, never absence/fall-through. Empty means finish or retract the declaration, not use a lower-level answer.
- local_tools.py:2225-2236: malformed explicit `#+key:` is a RuntimeError so resolution cannot mistake it for an absent table or silently re-key. The two generated broad catches currently contradict this guarantee for all three types.
- CLAUDE.md:698: four derived tables auto-surface at runtime. Do not register them or manufacture derived caches.
- Aggregate skill: missing curation is LabelUnavailableError and Feature may degrade; malformed curation is a distinct defect. Keep that line.
- country.py:6293: explicit units, nonempty mapping-label dict and valuation require strict food behavior. Scalar Aggregate and LSMS_LABELS_STRICT env do not activate this branch. Roster does not share this strict-map branch.
- UnitsUnavailableError is ValueError, intentionally default legacy fall-through and strict country-named refusal (`errors.py:25-67`); this differs from a categorical defect and must stay different.
- Core IO, pre-finalize cache, IDs, source semantics and schema follow STANDING sections3-4; no scientific quantity is defined or changed here.

## 4. Invariants and limits

Bare raise must preserve exact object/args/cause/notes/context. Keep original RuntimeError subclasses rather than inventing a new wrapper/base class.
Only three typed classes bypass the two generic catches; all other FileNotFoundError/KeyError/ValueError/RuntimeError legacy success/failure behavior remains intact.
Keep UnitsUnavailable handler byte-identical, and keep scalar relabel/poststeps outside the derive except.
Do not alter the pre-try unit helper, parser, mapping order, missing-curation policy, optional fields, strict environments, transformations, country config, fixtures/goldens or persistence.
Ethiopia W2/W3 household_id2, disjoint panels/EAs, unit cleaning versus API mapping, W1 urban definition and W5 Tigray universe are prior-art caveats, not this task's defects.
Only private empty data/config/tmp and synthetic fault files; no source acquisition, shared cache, DVC CLI, assumed-fresh, stamping, stash/ref/GH mutation.
Actual candidate hash/fingerprint scope is a required measurement; no “read path implies zero” claim.

## 5. Reuse decisions

| Item | Decision | Reason |
|---|---|---|
| Error classes and parser diagnostics | Reuse | Existing intentional non-fallthrough contracts |
| Derived dispatch and generic fallback | Reuse | Fix catch precedence, not API/materialization design |
| Narrow typed re-raise | Extend | Two existing catch sites need categorical exception precedence |
| Test source-fault machinery | Reuse | Already exercised by UnitsUnavailable Country tests |
| New regression file | New focused tests | Existing162 cases pass while these generated catches still mask all three types |
| Cache identity measurement | Reuse existing hash APIs | Read-only metadata before/after in separate frozen sources; no stash |

## 6. Approved scope and remaining verification

Root explicitly approved CategoricalKeyDeclarationError with the other two existing typed errors.
Approved writer scope: country.py typed imports/two re-raise clauses, tests/test_1081_derived_categorical_errors.py (73 cases), task ledger and safe evidence.
Original issue “nothing served silently” is qualified by successful mocked legacy returns; no real healthy Ethiopia legacy cache is claimed.
Read-time empty merge, nonempty value-column fall-through, Feature failure policy and unrelated broad catches remain residuals.
Baseline evidence:166 current-behavior probes plus5 valuation controls;162 existing tests pass; proposed73 tests give42 expected failures/31 passes on f8.
Immutable diagnosis and exact proposal: /local/job40255479/control/isa-backlog-20261021/framework1081-diagnosis/.
Before freeze, validate235 desired-contract/supporting cases, unchanged generic/UnitsUnavailable/label/Feature policies, exact actual549/39 hash scope and bounded healthy ISA public frames with available private references.
No candidate behavior/hash scope is claimed before those checks.

Sue (GPT-6).

## Phase 3 - verification

OK against sections2-5 at runtime commit `ca3e5cc119896983b49f50a6b9dade6a5f825131`.
The ledger-first commit `12346145ab715c28b604f3cc8cdfba7d82db7072` and runtime commit were both filesystem-published by the hook.
The exact production diff is three imports and two bare re-raise clauses (nine added lines); UnitsUnavailableError's complete handler is byte-identical.
All73 new cases plus162 existing cases pass (235 total, zero skips/errors/failures).
The immutable original f8 negative is42fail/31pass; the new test bodies and parametrizations are AST-exact to that actual73-case baseline.
All166 desired generated-handler probes pass, including Python exception context as well as args/cause/notes/custom context; five valuation controls retain their intended behavior.
Six synthetic Feature cases on each source preserve kept healthy data, full frames/attrs and the existing warning/drop policy; only the categorical warning now exposes its original typed cause.
The actual measured hash footprint is0/549 changed API hashes and0/39 changed framework fingerprints in separate exact-source processes.

Sixty manifest-pinned release L2 prerequisites were physically copied into each private root (120 copies, distinct inodes and original SHA256/size/mode/mtime).
Normal f8/candidate reads and fresh-process warm repeats compare17 complete Ethiopia/Uganda public frames per stage/source (68 comparisons), preserving full order, axis metadata, dtypes, null masks and attrs.
All60 parquets per root remain byte/size/mode/mtime exact; no derived parquet is manufactured and no L1/source-cold claim is made.
Feature default currency and explicit currency=None are separate references.
Actual Ethiopia Aggregate/recorded-unit-price refusals remain typed; the older CONTENTS claim that unitprice returns an empty frame is contradicted on f8 as well as candidate and is recorded in the report, not silently repaired here.

All original f8 source/test/fixture/schema/dependency blobs except the approved country.py edit remain exact.
The original diagnosis tree, private evidence and42fail/31pass receipts remain immutable.
No parser/country configuration/global conftest/read-gate/GitHub/shared-cache/acquisition change occurred.
Safe report/validation/reproduction/residual: `slurm_logs/isa_backlog_2026-10-21/framework1081/`.
Full private evidence and final clean filesystem-publication proof: `/local/job40255479/control/isa-backlog-20261021/framework1081-fix/`.
Independent review remains required after the documentation-only freeze. Sue (GPT-6).
