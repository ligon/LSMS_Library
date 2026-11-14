# Stream: nigeria (one Nigeria re-warm)

Read `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/COMMON-RULES.md` first.

- Worktree: `/local/job41201840/worktrees/nigeria`, branch `feature/nigeria-tables-20261113`, base `f138a3d61`.
- Report dir: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/nigeria/`.
- Read before editing: `lsms_library/countries/Nigeria/_/CONTENTS.org` (all of it),
  `.claude/skills/add-feature/pp-ph/SKILL.md` (every Nigeria wave dir holds TWO
  rounds needing distinct `t`), `.claude/skills/add-feature/food-acquired/units/SKILL.md`,
  and CLAUDE.md "Derived Tables" (the kg ladder; `survey_kg` is its top rung).
- Issues (read with `gh issue view N -R ligon/LSMS_Library`; never comment):
  #1502, #1518, #1537, #1713, and #587's post-harvest follow-ups.
- Rulings (`isa-decisions.org`): "Nigeria shipped conversion tables, GH1502 +
  GH1518", "Nigeria generic Pepper, GH1537", "GH587 label judgements and two
  follow-ups" (PH W3 Kg/l bullet), "GH587 W1 division after red team" (the
  unit-tokens bullet: aligning PH's blank-unit drop is a follow-up).

## Work, in order.  Measure first; implement only what the measurement supports.

1. **#1537 Pepper (check, then map).**  Confirm every served `j == 'Pepper'` row
   comes from codes 76 / 2140 (Preferred 'Fresh Pepper').  If so, map it to the
   Fresh Pepper Aggregate.  If not, leave it passing through and report which
   codes it comes from.

2. **#1502 + #1518 shipped conversion tables.**
   a. Bounded measurement, before wiring.  Measure W2's PP and PH tables and
      the W3 table: how many served `food_acquired` (t, j, u) rows each covers,
      agreement with W4's own `cvn` and with today's inferred factors, and the
      effect on `food_quantities(units='kgs')` and `food_prices()` (kgvalue),
      base vs candidate, cold, in private roots.  The ruling's evidence: W3
      inference serves 164,823 kg against 330,793 kg at the table on 106,152
      rows (0.556x), and W4's cvn matches the W3 table at median ratio 1.000.
      If W2's table looks unusable (it covers few rows, or it disagrees with W4
      by more than 2x on most cells), do not wire W2.  Report it and continue
      with W3.
   b. Wire the tables as a per-row `Quantity_kg` (the survey_kg rung, on the
      #1452 EHCVM precedent), by round: W2 PP, W2 PH and W3.  Include the
      harvest-side W3 table `ag_conv_w3` for #1518 (`crop_production`
      `Quantity_kg`).  W1/W2 harvest coverage is the #1518 residual; draft it.
   c. State the movement per wave: kg, `food_prices` rows and Price, and the
      `KgFactorSource` partition before and after.

3. **#587 post-harvest follow-ups.**
   a. PH W3: test the 3,433 dropped blank-size Kg/l rows against household
      unit values.  If they behave per-unit, serve them as PP's are served.
      Otherwise keep them dropped, with the evidence.
   b. Align PH's blank-unit rows with PP: serve them as `u='Unknown'`, per
      `data_info.yml`.  Count them per wave first.

4. **#1713 `individual_education`.**  Remove the post-planting quarters that are
   still declared after #1318 dated the table post-harvest only (5 cells
   'dropped').  Confirm what the coverage grader will make of the change; do
   not regenerate the coverage snapshot.

## Verify
- A cold base-vs-candidate build of every Nigeria table you touch, in private
  roots, row counts and frame diffs per wave.
- `tests/test_*nigeria*.py`, `tests/test_quantity_kg.py`,
  `tests/test_build_transform_hash.py`, `tests/test_schema_consistency.py`,
  `tests/test_derivations.py`, plus any test that pins Nigeria numbers you move.
  Update pinned numbers only with the measured value and a note citing the ruling.
- A hash census (COMMON-RULES).  Expected: Nigeria only.

## Deliver
`REPORT.md` (COMMON-RULES list) and `PR_BODY.md`: a section per issue, with a
"For maintainer review" list of every judgement you made.  Commit as you go.
