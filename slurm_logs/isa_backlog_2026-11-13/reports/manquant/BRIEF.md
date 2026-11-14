# Stream: manquant -- #1118 residual (Togo, Senegal, Cote d'Ivoire) + Togo gift fix

Read `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/COMMON-RULES.md` first.

- Worktree: `/local/job41201840/worktrees/manquant`, branch `fix/1118-manquant-residual-20261113`, base `f138a3d61`.
- Report dir: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/manquant/`.
- Read the CONTENTS.org of Togo, Senegal and CotedIvoire before editing each.
- Spec: Q3 in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/reconcile-20261112/REPORT.md`
  (lines ~271-280), and `gh issue view 1118`.

## Work, one commit per country
1. **#1118.**  Count the `u == 'Manquant'` rows per wave first.  Then replace
   `fillna('Manquant')` with the missing-unit sentinel `'Unknown'` in the 2018
   `plot_inputs.py` of Togo, Senegal and CotedIvoire.  Where `Manquant` is also
   a raw source label, keep the raw answer in its own column, as Niger does.
   The Benin, Burkina and Guinea-Bissau sites are fixed in other streams.
2. **Togo 2018 gift/purchase mix-up** (ruling 2026-11-13, "Fix all three now").
   Purchased / Quantity_purchased must read s16bq08 / s16bq09a (the purchase),
   not s16bq05 / s16bq07a (the gift question; check the variable labels).  Keep
   the gift answers in their own columns if the schema has them.  CONTENTS
   records the defect; update it.  Measure the change base vs candidate.
3. **Senegal, in the same re-warm** (the reconcile queue bundles these):
   - #1119-SEN: align the maize/cowpea `harmonize_seed_crop` spellings to
     `crop_production`, on the #1203 pattern;
   - #1287's Senegal half: in `Senegal/_/plot_features.py:28`, replace
     `except Exception: continue` with a declared-wave refusal (the #1241
     pattern).  Add a private-root broken-wave repro that must now raise naming
     the wave, and show frame equality to base on a healthy build.
   If either is not straightforward, leave it and report.

## Out of scope
Shocks: the coping stream owns them.

## Verify
- A cold base-vs-candidate build of `plot_inputs` (plus `nitrogen_kg` /
  `seed_kg`) and every other table you touch, per country, in private roots.
  Expected: only the relabelled and rewired rows move.
- Country output equals `Feature` output.
- Each country's tests, plus `tests/test_schema_consistency.py`,
  `tests/test_build_transform_hash.py`, `tests/test_derivations.py`.
- A hash census.  Expected: Togo, Senegal and CotedIvoire only.

## Deliver
Return `REPORT.md` and `PR_BODY.md` in your final message.  Commit as you go.
