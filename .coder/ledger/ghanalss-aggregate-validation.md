# Prior-Art Ledger -- GhanaLSS Aggregate validation and publication

Search tier: git, ripgrep, Country API, and local Org source. Inherit
`STANDING.md` and `ghanalss-aggregate-curation.md`; the previous goal turn
made verified progress by committing the all-wave inventory and draft.

## 1. Task

Use the existing preparation, scoring, and aggregation machinery to select
defensible GhanaLSS groups and publish a complete country Aggregate column.
The inventory/draft is an input, not completion. Preserve exact delivered
Preferred labels, nutritional crosswalks, and expenditure accounting.

## 2. Existing machinery

| Machinery | Source / evidence | Decision |
|-----------|-------------------|----------|
| Country extraction, sample, Region markets, demographics | `country.py`, `.claude/skills/demand-estimation.md`, Ghana `CONTENTS.org` | Reuse, retain full sample denominators and disclose missing controls/markets. |
| Level aggregation and partition evaluator | `SkunkWorks/aggregation/evaluation.py`, `test_evaluation.py` | Reuse; already enforces full maps, household splits, common normalization, fixed comparison population and market identification. |
| `Regression.prepare_data`, `score_w` | CFEDemands `Empirics/regression.org`, feature `prepare-data-scoring`, code `3cdfae9` | Reuse production branch; no alternate estimator/selector. |
| `Regression.preparation`, `beta`, `w_cov`, `gamma`, `e3` | same source; preparation/scoring tests | Reuse support reasons, loadings, identification, and conditional residual diagnostics. |
| `Regression.get_beta(compute_se=False)` | same source, estimate_beta | Point estimates in resampling; avoid nested or unbounded inner bootstrap. |
| `SurveyTemplate`, `survey_inputs` | `survey_simulation.py`, test_survey_simulation.py | Reuse empirical-mask DGP for known-welfare sensitivity. Ghana template needs explicit Region and denominator handling. |
| Draft partition and provenance | `ghana_inventory.py`, `ghanalss/proposed_groups.yml`, item_review.csv | Reuse; source-based semantic additions separately reviewed. |
| Country relabel contract and column writer | `country.py:_relabel_j`, `lsms_library/util/orgtbl.py`, aggregate-labels skill | Reuse for live country column and sum-preserving tests. Wave Aggregate columns are unused. |

No existing supplied-partition real-data validation driver was found in the
aggregation directory. New analysis should adapt inputs and report outcomes;
it must not replace CFE fitting or uncertainty machinery.

## 3. Definitions

Follow `SkunkWorks/cfe_aggregation.org` and the curation ledger. Same-beta
groups can introduce unequal error variances; non-rejection is not
equivalence. Missing constituent betas need a disclosed semantic assumption
and validation, never automatic acceptance. The older food-acquired skill's
50% non-rejection heuristic and its claim that better nonfood prediction
identifies better MUEs are superseded by these explicit user-reviewed notes.

Primary real-data experiment: 2016-17 purchased expenditures, summed over
the six food recalls as the current derived API does; Region markets;
the existing 14 sex-age counts and log household size. Salt remains a
singleton normalization anchor. Onion and Egg are reserved food prediction
targets, removed from every fitted/scored expenditure vector in the
prediction experiment. Total recorded acquisition and earlier applicable
waves provide sensitivity checks. No additional own-production valuation.

## 4. Invariants and evaluation design

- Use a reproducible household split into training, validation, and final
  test sets (seed 20260916), stratified by Region and Rural. All observations
  of a household remain on one side. Do not tune on the final test set.
- Coverage uses the original sample weights and household denominators.
  Missing controls prevent scoring but do not erase households from reports.
  Preflight: 14,009 sample households, complete Region/positive weights;
  13,990 households with derived characteristics, missing 19 in Central.
- Common score origin uses complete-control Salt-observed training cells;
  comparison uses complete-control Salt-observed evaluation cells fixed
  before fitting. No post-fit intersection of survivors.
- Reserved-food prediction uses training-only calibration on scored
  households and observed positive target amounts. Evaluation targets and
  populations are fixed across partitions. Missing predictions invalidate
  the corresponding complete-population error comparison.
- Keep nonpositive loadings, failed fits, unsupported members, and loss of
  support visible. Real-data prediction/stability is a diagnostic, not MSE
  against known true w. Simulations supply explicitly conditional evidence.
- Economic curvature is reported on the common anchor scale over a fixed
  training welfare range, with joint resampling of constituent loadings.
  An initial 0.10 log-expenditure tangent-error screen is a disclosed
  analysis tolerance, not a theorem about w error or an optimality claim.
- Only aggregate results are tracked. No household identifiers, masks,
  scores or model pickles in git. Existing cache/data IO constraints apply.
- A published map must cover every delivered Preferred label plus existing
  country rows, be unambiguous, preserve level totals, and keep each
  unresolved item as a named singleton. It must make real, supported
  aggregations; a token identity map is not a substitute for the objective.
- Adding missing country Preferred rows must not introduce nutrient matches
  that were absent before. Inspect the nutrition lookup before choosing
  how to extend the table and verify its existing behavior is preserved.

## 5. Reuse and implementation

Add a thin Ghana empirical-evaluation driver and targeted tests for its
input alignment, split, and validation leakage/population contracts. Reuse
the evaluator for fitting/scoring, standard least squares for diagnostic
prediction calibration, and the existing CFE DGP for known-welfare checks.
Use the Org column writer for the final country map after handling its
incomplete Preferred axis explicitly. Add contract tests and a dated
results/decision note with reproduction commands.

## 6. Verification

Pending empirical results, selection, publication, and end-to-end API tests.

--Sue, 2026-09-16
