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

### Phase 3 — verified at development 47df62d49

- 48 targeted source-free tests. The exact historical scalar cancellation
  witness remains 0; repeated-key, NaN, infinity, differently typed and empty
  controls match the prior aggregate.
- Private same-source reducer replay had 0 ULP movement for both waves.
  Three single-thread timing repetitions, best observed old/new: 2014-15
  0.770/0.125 s, 2017-18 2.506/0.253 s. These are reducer timings, not corpus
  or guaranteed end-to-end speedups.
- Default cold/warm Country and Feature match all 204,740 old rows and attrs
  across four independently staged waves. Each tested old-wave subset matches
  its own public baseline. Feature already omits six all-empty optional
  columns from old-wave-only requests. The all-wave EHCVM columns remain.
- The cache estimate was broader than a table-only change. In a 345-key static
  census, 24 Mali country hashes and 48 wave hashes (24 per edited EACI wave)
  changed. All two EHCVM wave hashes and Niger/Burkina controls are unchanged.
  That is the framework's conservative local-module footprint even for a
  wave-script-only change. If the separately reviewed #1772 deploys in the
  same release, it may absorb overlapping re-warm work. No shared cache
  was rebuilt or mutated here.
