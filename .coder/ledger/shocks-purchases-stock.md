# Shocks: purchases and food stocks

Search tier: git + ripgrep; base development 65b1730f0.

## §1 Task
Implement the maintainer's 2026-11-06 ruling: AffectedConsumption records the
qualitative effect of a shock on food purchases; AffectedFoodStock records the
effect on food stocks, following Ethiopia. Preserve separate source answers,
missingness, existing increase/decrease boolean semantics and shock rows.

## §2 Existing machinery
- Ethiopia's five wave data_info.yml files and _/data_scheme.yml already expose
  both columns. Reuse this model and the existing YAML boolean mapping toolkit.
- Existing Mali/Burkina/Senegal 2018-19 mapping.shocks handles five-column
  vague 1 versus six-column vague 2 and refuses unexpected source layouts.
  Tests: test_gh1556_ehcvm_shocks_2018_layout.py.
- EHCVM 2021-22 mappings distinguish d=livestock, e=stock, f=purchases.
  Tests: test_gh1556_ehcvm_shocks_2021.py.
- Malawi hh_u03d asks about stocks. Other source columns must be verified
  rather than inferred from letter order; older EAC-I/ECVMA labels are truncated.
- Existing review: burst/isa-sm-20261103/reports/review-ehcvm-shocks/VERDICT.md
  §2 and source-label diagnostics. Current census: 13 purchase, 17 stock and
  4 unresolved stock/livestock country-wave mappings into AffectedConsumption.
- Canonical schema lsms_library/data_info.yml and Feature assembly must preserve
  the added optional boolean without changing indexes or requiring unavailable
  source questions. See tests/test_schema_consistency.py and STANDING.md §2.

## §3 Definitions
The maintainer's explicit ruling is authoritative:
"let AffectedConsumption refer to food purchases while AffectedFoodStock
applies to food stocks."
Both are reported qualitative responses encoded as nullable booleans; increase
and decrease remain True, unchanged remains False, and existing source-specific
not-applicable/missing mappings remain unchanged. See STANDING.md §3 and the
Ethiopia YAML mappings. No quantities, direction inference, or imputation.

## §4 Invariants
Inherit STANDING.md §4 and CLAUDE.md data-access/cache policies. No shared
harmonized-cache writes; tests and builds use private data roots and exact
checkout import/config identity. Never run DVC CLI or overwrite prior worktrees.
Keep all shock indexes, rows, coping/timing and unrelated effect fields unchanged.
EHCVM split layouts require per-row selection, retaining refusal controls.
No stock question must be manufactured for Uganda or another missing instrument.
No source/fixture/baseline/dependency/lockfile changes, weakened assertions or
unrelated transformations. Explicitly update obsolete semantic test expectations
only when accompanied by independent source-answer controls.

## §5 Reuse decision
Reuse Ethiopia's two-field schema and existing YAML boolean conversion. Extend
existing split-layout hooks to choose both purchase and stock answers. Extend
country declarations and canonical documentation/schema with optional stock
support; do not add a new loader or framework transform. Reuse the source audit
and verify source metadata before editing. Unverified mappings remain explicit
blockers, never guessed from a neighboring survey.

## §6 Open questions
No remaining policy decision on purchases versus stocks. Unexpected source
layouts/ambiguous source metadata require evidence and a reported residual.
New-wave author branches are outside this PR; document the ruling for their
integration rather than editing or merging those unfinished branches.

## Verification
Implemented across 22 existing waves in ten countries using the machinery in
§2, with the two distinct source-answer definitions in §3 and unchanged rows,
indexes, coping answers and other effects under §4. Ethiopia and Uganda remain
controls; Malawi 2004-05 has no manufactured stock answer.

At implementation commit e1cd67958197c126c6a9872127f252b91480a816:
- 432 targeted synthetic/source/schema tests passed; an independent reviewer
  repeated those 432 successfully in a fresh private cache.
- Cold baseline/candidate comparison retained all 271,973 rows across twelve
  countries, exactly matching indexes and unrelated values. Independent
  snapshot verification confirmed it; Ethiopia/Uganda frames were unchanged.
- Feature shocks audit phases 1/2 had no fail/error. All 25 baseline findings
  persisted unchanged; the only three additions report the absent stock
  question in Malawi 2004-05. Direct Feature assembly preserves both nullable
  boolean answers and all rows, with Uganda stock cells missing.
- Country documentation and source-selector review found no blocker. Source
  limits remain explicit: older EAC/ECVMA labels are truncated and supported by
  the prior independent review plus response signatures; CIV Stata .d decoding
  remains #1561. Unfinished 2021/2024 wave branches remain outside this change.

Final independent review, offline CI, exact-head GitHub checks and merge status
are recorded with PR #1596 and the durable sweep report
`reports/shocks-purchases-stock/` under the 2026-11-03 backlog sweep. These are
verification of this implementation, not a claim that existing grain or other
unrelated audit warnings are resolved.
