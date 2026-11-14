# Stream: cropkg -- #1559 price-implied crop kg: report-only screen + a rung DESIGN MEMO

Read `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/COMMON-RULES.md` first.

- Worktree: `/local/job41201840/worktrees/cropkg`, branch `feature/1559-crop-kg-screen-20261113`, base `f138a3d61`.
- Report dir: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/cropkg/`.
- Issue: #1559.
- Ruling, "GH1559 price-implied kg per unit for crops" ("Also build a rung"):
  (1) a report-only screen flagging a survey kg outside 0.1x-10x of the
  price-implied kg per unit (baseline: kg sales plus sacks at their stated
  mass); and (2) the design of a price-implied INFERRED rung for crop units in
  `harvest_kg_factors`, as its own design and never merged with the food ladder.
- Prior measurement:
  `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/sold-kg-1560/REPORT.md`
  (Part 2) and its scripts.
- Read CLAUDE.md "Site Q", `lsms_library/quantity_audit.py`, and the
  `harvest_kg_factors` ladder in `transformations.py`.  Read the CONTENTS.org of
  every country the screen fires on.

## Work
1. **The screen (code).**  It is report-only, like Site Q: it never clips,
   drops or NaNs, returns its input unchanged, and names every offender.
   - Choose where it runs so it costs NO cache hashes:
     - a read-path module listed in `_EXCLUDED_CALLABLES`, pinned by a test; or
     - a function in `transformations.py` that is not in the closure of any
       country's imported transform.  Since #1509, Ethiopia, GhanaLSS and
       Uganda import `food_quantities_from_acquired`.
   - Measure the hashes; do not assume them.
   - Decide the lever and accessor names to fit the family
     (`quantity_reports` style), and justify the choice.
   - Report the firing rate per country and wave, and the 0.1x / 10x tails.
2. **The rung (design memo only, NO code).**  `out/RUNG_DESIGN.md` covers:
   - the estimator;
   - its evidence rows;
   - gates, as the food ladder has (`min_reports`, `min_baseline`, the
     dispersion gate);
   - where it sits in `harvest_kg_factors`' ladder;
   - its disclosure;
   - the measured caveats the ruling names:
     - product state (shelled or unshelled, about a 2.4x price gap; only
       2021-22 records it);
     - selection (21-24% of rows come from sellers);
     - seasonality (about 1.3x, Mali);
     - survey kg at 1.1-1.4x the price-implied value at the median;
   - coverage and agreement, measured on Niger and Mali at least, against the
     stated-mass rows as an answer key;
   - the cache cost of landing it.
   The user rules on the memo before anyone builds the rung.

## Verify
- Tests for the screen: synthetic, plus cache-gated real data.
- `tests/test_build_transform_hash.py`.
- A hash census, expected 0 moved (or explained).

## Deliver
Return `REPORT.md` and `PR_BODY.md` in your final message; the memo goes in
`out/`.  Commit as you go.
