# GH #905 — Mali EACI purchase quantity reducer

## §1 Scope

The two EACI wave scripts, not the shared EHCVM input path. Preserve the
household/input aggregate and its use as an annotation, not a plot allocation.
No source quantity, unit, rule, index, merge policy or EHCVM work changes.

## §2 Reuse

- See STANDING.md §§2–5, CLAUDE.md and data_info.yml for grain, permitted IO
  and cache. Use existing `sum(min_count=1)` and pandas group ordering.
- Tanzania's accepted singleton strategy (GH #1687, `tanzania.py`
  plot_inputs_for_wave) keeps the original scalar sum for every repeated
  group. Reuse that numerical policy, not the country-specific slot code.
- The new #905 equal-width prototype is not needed in these waves.
  `test_gh905_vectorised_min_count_sum.py` and the Tanzania signed witness
  document the four-ULP ceiling. Preserve it on future source layouts.
- The country notes say EACI purchases annotate eligible applied rows only.
  Do not add Farm-level rows or label a grouped quantity as a plot allocation.

## §§3–4 Definitions and invariants

- Phase A read the exact held `EACIS2D_p2.dta` and `eaci17_s07bp2.dta`
  at the two committed sidecars, using independent physical copies in a
  hard-offline root. The actual staged preparation gives float64 in both.
- 2014–15: 16,380 groups, 2,340 repeated, 14,704 all-blank. 2017–18:
  54,708 groups, 4,779 repeated, 49,647 all-blank. The isolated full vector
  result happened to equal the scalar result on both releases. It is not the
  product choice for repeated groups.
- If a future release yields another dtype, keep the original reducer. Keep
  the full scalar order for every repeated key, ignoring no native operand.
  Use grouped output only for singletons. Missing keys and nullable booleans
  have the original pandas semantics. No changes to word decoding or eligibility.

## §5 Implementation

Keep the small reduction local to each already hashed wave script. This avoids
a separately evolving import dependency in the wave-script fingerprint and a
country-module rewarm of the unaffected EHCVM wave parquets. Both small
helpers are tested by loading their actual definitions.

## §6 Remaining verification

Verify all held wave source prerequisites and both APIs, plus both loaders
where possible. Measure end-to-end benefit and exact hash scope independently.
