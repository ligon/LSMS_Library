# Stream: burkina (one Burkina Faso re-warm)

Read `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/COMMON-RULES.md` first.

- Worktree: `/local/job41201840/worktrees/burkina`, branch `feature/burkina-bundle-20261113`, base `f138a3d61`.
- Report dir: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/burkina/`.
- Read before editing: `lsms_library/countries/Burkina_Faso/_/CONTENTS.org` (all of it).
- The spec for items 1-3 is the reconcile queue: Q3, Q6 and Q7 in
  `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/reconcile-20261112/REPORT.md`
  (lines ~271-325).  The issues are #1436 (second residual), #1437 (Burkina
  half), #824, #1045, #1118 (Burkina site) and #1442 (read with `gh issue view`).

## Work
1. **Q3, #1118.**  Count the `u == 'Manquant'` rows per wave first.  Then
   replace `fillna('Manquant')` in `Burkina_Faso/_/s16b_2021.py:133` with the
   missing-unit sentinel `'Unknown'`.  If `Manquant` is also a raw source label,
   keep the raw answer in its own column, as Niger does.
2. **Q6, the 2021-22 `crop_production` driver.**  Build it from the panel
   component's s16c, plus s16d sales, on Mali 2021-22's #1466 pattern: a single
   harvest row where one exists, else Farm-level, per the #843 option-d ruling.
   Read Mali's implementation and its tests before writing Burkina's.
   - Harvest-from-dispositions is NOT ruled for Burkina; the Benin/CIV ruling
     covers only those two.  If the driver needs it, stop and bring it back as
     a question with counts.
   - #1437 (blank s16cq16a on finished harvests): diagnose Burkina's half.  Do
     not impute; report.
   - Verify: a cold build; `Value_sold` equals the raw s16d sum; report the
     `harvest_kg` / `yield_kg` row counts.
3. **Q7, #1045.**  Move Burkina's `interview_date` to a declarative `datetime:`
   spec, as Ethiopia W4/W5 did in #1413, 2021-22 first.  Do the other waves too
   if each one is straightforward.  Add a test beside
   `tests/test_gh1045_parse_datetime.py`.  `datetime_reports(country='Burkina_Faso')`
   must show no new `unparsed`.
4. **#1442: Burkina 2014 C05/C06 as Expenditure.**  The implementation is
   archived as commit `d81358091` (on `refs/archive/2026-11-11/isa/burkina-bundle-20261104`;
   the object is in this clone).  Of that archive's other commits, the #1464,
   #1057 and #1226 work has already landed through #1522, #1583 and #1505.
   Bring in `d81358091` only (cherry-pick it in your worktree, or re-apply it),
   review it fresh against current development, and re-measure it.  #1509 has
   since made the kg-factor evidence purchases only, so the effect of C05/C06
   on kg factors differs from what the archive measured.  Use #1442's own
   ruling text.

## Verify
- A cold base-vs-candidate build of every Burkina table you touch, in private
  roots, with row counts and frame diffs per wave.
- `tests/test_*burkina*.py`, `tests/test_gh1045_parse_datetime.py`,
  `tests/test_derivations.py`, `tests/test_schema_consistency.py`,
  `tests/test_build_transform_hash.py`, the Mali #1466 crop tests, and any test
  that pins Burkina numbers you move.
- A hash census.  Expected: Burkina only.

## Deliver
`REPORT.md` and `PR_BODY.md` (a section per issue, plus "For maintainer
review").  Commit as you go.
