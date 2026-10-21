# Concrete #1081 boundary for root review; no implementation yet

Change only `lsms_library/country.py`, one focused test file and the task ledger/safe evidence. No country, schema, transformation, parser, exception-hierarchy, fixture/golden, dependency, persistence or global-conftest changes.

Import these existing classes from `.local_tools`, which is already imported by country.py:

```python
CategoricalMappingConflictError
EmptyCategoricalTableError
CategoricalKeyDeclarationError
```

Add the same narrow clause immediately before each existing generic catch (`country.py:6292` and `:6362`):

```python
except (EmptyCategoricalTableError, CategoricalMappingConflictError,
        CategoricalKeyDeclarationError):
    raise
```

Keep the food UnitsUnavailableError handler byte-identical and ahead of the generic catch. Bare raise preserves the exact typed object, actionable filename/table message, original args, cause, notes and any structured context. Do not wrap it, reconstruct it, downgrade it, log-and-return, or catch all RuntimeError unconditionally. The roster clause gets the same three types, without changing its unrelated strict/fallback policy.

Including CategoricalKeyDeclarationError is an explicitly proposed extension of the issue's named two-error scope. Root independently found its non-fallthrough contract consistent with this boundary; no code has been applied. All three are existing deliberately distinct RuntimeError types, so no new shared base class or abstraction is needed.

The concrete self-contained private test draft is `test_1081_proposed_contract.py`. It uses existing `Country._aggregate_wave_data` fault-injection machinery, actual categorical readers/private Org trees and generated public methods, with73 cases:

- 24 source-error identity/context cases: three error types x four derived handlers x default/Scalar Aggregate.
- 18 food-derived finalizer-error cases: three types x three food handlers x default/Scalar Aggregate.
- 15 existing strict food controls: three types x three mapping-dict calls/two explicit-unit calls.
- 16 unrelated generic fallback controls: four generic classes x both handler families x successful/exhausted legacy outcomes.

The old baseline gives42 expected failures/31 passes, not errors/skips. Both families and all three error contracts discriminate the old behavior. No test accepts a masked categorical error as a successful candidate result. Existing real-transform UnitsUnavailable tests remain in the required162-case supporting selection. The supplemental five valuation and three healthy Aggregate-absence probes remain required preservation controls.

After root approves implementation:

1. Commit a per-task ledger first, inheriting STANDING and this draft; then make only the scoped source/test change.
2. Run the73-case new file plus the162-case existing selection with image Python, exact cwd/PYTHONPATH/countries-root identity, private data/config/tmp, active offline guards and bounded loadfile workers. Expect235 passes/0skip, with original baseline42fail/31pass retained as evidence. Rerun the generated-handler probe with updated *desired* categorical outcomes; retain generic/UnitsUnavailable/healthy-Aggregate/valuation controls unchanged. Do not run the current-behavior assertions expecting the old mask as a candidate gate.
3. Measure actual all549 API hashes and39 framework fingerprints for immutable f8 and candidate in fresh processes using metadata only. Report the exact changed key set; zero is not assumed. No stash, hash stamp or freshness bypass. If measurement requires scope beyond the approved source/test files, report it before changing anything else.
4. Perform bounded healthy ISA public API comparisons only with available physical private held references and normal hashes/coverage. No acquisition or automatic shared-cache rewarm. Treat a missing held input as a verification limit.
5. Freeze clean/published candidate, record complete source/test/fixture blob identities and exact commands, and stop for independent review. Do not publish a PR or mutate another lane from this task.

The current diagnosis remains read-only. Root must read this concrete proposal/ledger before writer work begins.

Sue.
