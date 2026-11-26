# #1751 — Niger EHCVM acquisition position guard

Search tier: git and ripgrep with AST-based source-free tests. Baseline
47df62d49e614c9a56e319def7324145d138c3d9; standing rules in STANDING.md.

## §1 Task, restated

Verify the one-to-one position of Niger's four added acquisition columns
before reattaching them after the EHCVM plot_inputs tail. A row count or an
(i, input) comparison does not prove that native lines retained their order.
This is independent of #1751's unresolved purchase-unit interpretation.

## §2 Existing machinery

- Niger/{2018-19,2021-22}/_/plot_inputs.py:_finish_with_acquisition filters out
  unlabelled inputs, delegates normalization, then attaches four coerced columns
  by position (currently checks only row count).
- Niger/_/niger.py:_finish_plot_inputs sets an input_obs index level, carrying
  a supplied string level through unchanged. Otherwise it uses the public
  unresolved_observation token. It fills crop and u, coerces three payload
  columns, and filters input NA. It also serves the two older ECVMA waves.
- tests/test_ehcvm_plot_inputs_identity.py exercises real producer scripts
  with invented rows and separate requires_s3 native comparisons, and pins
  existing uniqueness/refusal routes. Do not run native cases here.
- tests/test_niger_ecvma_input_observations.py pins older native-slot transport
  on source-shaped frames. Reuse unchanged.

## §3 Definitions in force

Follow Niger/_/CONTENTS.org's #1123 and 2026-11-13 purchase-binding entries.
Multiple slots can share a household and harmonized input. The two EHCVM waves
do not serve a native input_obs token. 2021-22 use, gift and purchase fields
have their own value labels; purchase-card authority is held in #1751. No code
or unit label interpretation changes. Nothing here changes ECVMA slot tokens.

## §4 Invariants

Use exact post-filter row identity, not equality of potentially identical
payload or grain. A same-count permutation or repeated row must refuse before
any positional assignment or parquet write. Healthy outputs remain frame-exact
including index levels, missingness, dtypes, column order and attrs. Never
publish an internal ordinal. Leave tail, mapping, scheme, older scripts,
source IO and row selection unchanged. Errors show wave and reason, not IDs.

## §5 Reuse decision

Reuse the existing supported input_obs transport inside the wave wrapper with
a unique temporary position per retained row. Compare its entire output order.
Restore the prior single unresolved_observation index level in place without
refactorizing any other axis. An exact duplicate would defeat a projected
normalized identity comparison, so do not recompute the tail as an oracle.
Any deliberate future source-grain extension must update this wrapper explicitly.

## §6 Open questions

No additional authority needed for the guard alone. Actual data verification
and publication are separate. Natural hash scope remains the two modern wave
directories and Niger's country hashes, not either ECVMA wave. The cart-unit
authority hold remains open.

### Verification

- OK (§2, §4, §5): all eight new same-count controls fail on the pinned
  pre-change scripts because no error is raised, and pass with the guard.
  Test reversals and replacement even when all shared normalized values agree.
- OK (§3, §4): exact frame, axis-level, dtype, attrs and acquisition-text
  equivalence for an ordinary row order, including a missing unit/crop,
  explicit zero purchase quantity, filtered row, typed empty and many rows.
  The previous count refusal and reserved input_obs refusal are explicit.
- OK (§2, §4): isolated offline existing source-free suite 93 passed,
  4 requires_s3 deselected. Final targeted module including two further
  controls 20 passed. No survey reads, builds from survey data, or remote.
- OK (§4): metadata/sidecar-only 38-country census compares base to the
  candidate script bytes. 25/556 country hashes move (all Niger);
  50/2,323 wave hashes move (all 25 each for 2018-19 and 2021-22);
  0/42 framework build-transform and 0/38 country framework-import hashes
  move. Both ECVMA wave directories and hashes remain unchanged.
