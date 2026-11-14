# Stream: malawi (one Malawi re-warm)

Read `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/COMMON-RULES.md` first.

- Worktree: `/local/job41201840/worktrees/malawi`, branch `fix/malawi-1228-1712-20261113`, base `f138a3d61`.
- Report dir: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/malawi/`.
- Read before editing: `lsms_library/countries/Malawi/_/CONTENTS.org` (all of it).
  Malawi stacks cross-section and IHPS panel samples, and now has a 2024-25
  wave (#1604).  Do not touch any weight (#1704 is open and unruled).
- Issues: #1228 and #1712 (read with `gh issue view`).

## Work
1. **#1228 (ruling "Malawi later-wave cowpea, GH1228": "Join D, single-text only").**
   a. Read-only check first: do Modules D and G share the season reference in
      2016-17 and 2019-20?  If they do not, stop and report.
   b. Relabel the 2016-17 / 2019-20 'Other' rows of Module G to Cowpea (Khobwe)
      from Module D's text, and only where the plot has exactly one
      'Other Crop' text.  Move Module I code-48 sales by the same rule.  Local
      names stay unmapped.  Add the harmonize_crop label the issue says is
      missing.
   c. 2024-25 is not in the ruling.  Measure whether the same pattern holds
      there and report it; do not extend the rule to it.
   d. Count: rows relabelled per wave, rows left 'Other' and why (multi-text,
      no text, local name), and sales moved.
2. **#1712, plot_inputs.**  Six native Module H columns are all null and grade
   the table 'builds' (since #1190).  Diagnose, per column and wave, why: a
   wrong source variable, a question not fielded, or a filter.  Fix a wiring
   error.  If the column is genuinely absent from the instrument or the
   release, do not choose between `optional:`, removal or a coverage verdict.
   Bring that back as a question with the evidence.  `optional:` is
   country-grain; see CLAUDE.md "Silent All-Null Read".

## Verify
- A cold base-vs-candidate build of `crop_production` and `plot_inputs` (plus
  anything else you touch), all waves, in private roots: row counts and frame
  diffs per wave.  Any row moving outside the ruled scope is a finding.
- `tests/test_*malawi*.py`, `tests/test_derivations.py`,
  `tests/test_schema_consistency.py`, `tests/test_build_transform_hash.py`, the
  `harvest_kg` / `crop_production` tests that cover Malawi.
- A hash census.  Expected: Malawi only.

## Deliver
`REPORT.md` and `PR_BODY.md`.  Commit as you go.
