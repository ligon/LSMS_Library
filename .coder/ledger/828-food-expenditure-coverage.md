# Prior-art ledger — #828 food expenditure coverage

Search tier: git and ripgrep. Standing baseline: [STANDING.md](STANDING.md).

## §1 Task

Disclose remaining unvalued, physically observed own production and gifts
for explicit total-basis expenditures from six reviewed EHCVM waves.
The 2026-11-27 ruling keeps all current totals and valuation behavior.

## §2 Existing machinery

- Reuse the generated `Country._FOOD_DERIVED` dispatch and
  `food_expenditures_from_acquired` for the exact acquisition input and sum.
- Reuse `food_acquired_valued` for valuation and its positional rung labels.
  That algorithm treats numeric zero as a candidate. Disclosure must read
  the original value separately, without changing the algorithm.
- Reuse Feature's same-call period continuation and shape selection. Coverage
  also describes valid zero-row results, so the ordinary nonempty attrs
  carrier is not enough and coverage must not alter concat's existing attrs.

## §3 Definitions

See STANDING.md §§3–4, CLAUDE.md (Derived Tables), and the food schema in
`lsms_library/data_info.yml`. Use the six `food_acquired` bindings in wave
`_/data_info.yml` and the Burkina Faso, Mali and Niger country
`CONTENTS.org` sections for #876, #1452, #1707 and #1708.

## §4 Invariants

Counts refer to harmonized acquisitions entering the transform, before the
expenditure grouping and after any requested valuation. They are not raw
response rates, population coverage, or evidence about an absent record.
Do not treat explicit zeros, money-only rows, or missing purchases as gaps.

## §5 Reuse decision

Extend the post-read transform with an internal observation point and deliver
a count record after existing output guards only if a true remaining gap
survives anywhere in the public call. If one does, report the accepted
same-call context as well, including reviewed observations without gaps.
No new valuation, build, registry entry, or cache rule.

## §6 Boundaries

Only the six shipped routes and explicit total basis are reviewed. A
standalone transform has no trusted instrument scope. Alternate country
configuration, unchecked cache mode and a legacy derived-table build cannot
inherit that scope. An empty expenditure output can still describe observed
acquisitions. An empty acquisition input or a country dropped before its
post-processing is not successful coverage.

## Verification

- OK (§§2–4) — the observer runs before omission, receives the existing
  valuation result, and uses its rung labels. An input zero can still be
  repriced by that algorithm without becoming an originally missing answer.
- OK (§§2, 4) — Feature transports only counts outside concat, preserves
  valid zero-row scopes, excludes rejected scopes, and uses the existing
  period continuation without a second acquisition read.
- The final public attachment gate leaves no new attr or warning when all
  accepted records are gap-free, including when the only gap was rejected.
  Neutral role counts are still available as context for a surviving gap.
- OK (§§3–5) — no wave input, builder, registry, cache rule or exemption was
  changed. Pinned-base synthetic checks compare full results and scoped
  fingerprints. No survey data or corpus coverage measurement is claimed.
