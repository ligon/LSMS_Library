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
Pending implementation, source-to-output checks, affected regression tests,
cross-country Feature shocks audit and independent review.
