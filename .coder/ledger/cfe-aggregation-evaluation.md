# Prior-Art Ledger -- CFE aggregation evaluation

**Search tier:** git and ripgrep over LSMS and the authoritative CFEDemands
Org files; no configured GitNexus or memory tool. Inherits `STANDING.md`.

## §1 Task, restated

Implement the supplied-partition evaluator in
`SkunkWorks/cfe_aggregation.org`, "Statistical evaluation and search".
Aggregate nonnegative food expenditures in levels, fit the revised CFE
preparation/estimation pipeline on training households, and score held-out
households. Report coverage separately from error on a fixed comparison
population. Establish the comparison using known-welfare simulations before
adding an automatic partition search. Work is an isolated SkunkWorks analysis;
country extraction and production estimation remain in their existing packages.
Extend the first toy experiment with complete household observation patterns
from Uganda food expenditures. Measure the actual sparsity before simulation;
do not equate item-level density with household fitting or scoring coverage.

## §2 Existing machinery

| symbol | source | role and tests | decision |
|--------|--------|----------------|----------|
| `Country._relabel_j` | `lsms_library/country.py:3031` | published label-column selection and level sums; label contract tests | Preserve; arbitrary supplied membership maps are not a `labels=` argument. |
| `food_expenditures_from_acquired` | `lsms_library/transformations.py:2556` | expenditure basis, source handling, valuation; transformation tests | Reuse upstream for empirical inputs; evaluator accepts already-declared monetary amounts. |
| `Regression`, `prepare_data` | `../CFEDemands/Empirics/regression.org:1285,1070` | parameter fitting and selection diagnostics; `test_preparation_scoring.py` | Reuse unchanged from `feature/prepare-data-scoring`. |
| `Regression.score_w` | same file:1563 | frozen controls, loadings, centering and market intercept; training agreement/batch invariance tests | Reuse; never refit on evaluation households. |
| `Regression.w_cov` | same file:1494 | fitted conditional covariance under its stated homoskedastic model; `test_w_var.py` | Reuse for raw fitting diagnostics only, not validation MSE or post-selection coverage. |
| `dgp.expenditures` | `../CFEDemands/Empirics/cfe_estimation.org:2434` | known lambdas, prices, characteristics and measurement error; `test_dgp.py` | Reuse with index-name adapter and additional zero-pattern regimes. |
| `lsms_library.demands.main` | `lsms_library/demands.py:31` | one Country API fit | Leave unchanged; lacks partitions, held-out scoring, common normalization, or validation loss. |
| `Country.sample`, `Country.food_expenditures` | `lsms_library/country.py`; `countries/Uganda/_/CONTENTS.org:209`, `_/recall.yml` | sample universe, household Region, cross-sectional weights, and declared expenditure basis; existing sample and food transformation tests | Reuse for survey masks. Keep zero-food sample households in coverage denominators. |
| `harmonize_food` | `countries/Uganda/_/categorical_mapping.org` | existing Preferred/Aggregate labels; API contract in aggregate-labels skill | Trial grouping only. Conflicting Preferred-to-Aggregate assignments remain singleton foods, explicitly reported. |

The API audit passes (15 skills); no existing partition evaluator or grouped
validation split was found. Native DGP indices reverse the modern household
`i` / good `j` names: adapt labels, not numerical meaning.

## §3 Definitions and conventions

Use `STANDING.md §3` and `../CFEDemands/.coder/ledger.md §3` for index, weight,
and welfare conventions. In particular, `w=-log(lambda)` and beta denotes
Frisch elasticity. Follow `SkunkWorks/cfe_aggregation.org`, "Notation and
support": every fine food belongs to exactly one nonempty group; expenditure
is nonnegative; unreported amounts and recorded zeros have the same treatment
inside the declared questionnaire universe. Sum amounts before logging.

## §4 Invariants and assumptions

- Inherit IO/cache constraints from `STANDING.md §4`; synthetic experiments
  require no country data, credentials, cache purge, or external service.
- A caller supplies a complete item-to-group partition, training and evaluation
  households, population weights, fixed comparison cells, and normalization
  reference cells. A household cannot appear on both sides of the split.
- Keep the anchor good a singleton and require its fitted loading to be positive.
  For declared anchor loading b0, multiply scores by fitted beta_anchor/b0.
  Subtract the weighted mean on the same declared training reference cells for
  every candidate. Neither held-out truth nor held-out expenditures chooses
  this normalization. This is a unit convention, not known loading inference.
- Check identification separately for each fitted market using the existing
  `Regression.w_cov(min_goods=1).V22` rank signal. A market with only single-good
  households does not identify its intercept even when loadings vary globally.
  Exclude its scores from coverage/error and fail any reference that relies on
  it. The scorer alone does not impose this additional validation check.
- All positive-weight comparison cells must be scored to report comparable
  MSE. Never silently intersect survivor sets or average validation error over
  a candidate-specific population. Coverage uses the entire declared evaluation
  population. Scoring outside the comparison set is reported separately.
- Simulation truth may assess error; it must never enter fitting or score
  normalization. Recenter truth on the same training reference cells and put
  it in the declared anchor units before calling the evaluator.
- Conditional fitted variances omit loading, centering, and selection uncertainty.
  Label them as diagnostics in the model's raw units; do not advertise intervals
  for normalized held-out scores without a separate coverage experiment.
- Candidate maps are supplied, not learned using held-out data. Evaluating a
  collection does not provide unbiased performance for an adaptively selected
  winner. Search and a final untouched test sample remain subsequent work.
- An empirical template preserves entire household masks, markets and survey
  weights. Synthetic welfare and errors are generated independently of those
  masks. This does not identify why the survey reports zeros. Report purchased
  and total recorded acquisition separately; do not impute own-production values.
- Use the observed, wave-specific Preferred universe, excluding Cigarettes and
  Other Tobacco. Reindex food observations to sample households before counting.
  Distinguish absent food records and unknown Region from preparation exclusions.
  The 2019-20 API currently has three missing Regions despite the older blanket
  completeness statement in CONTENTS; exclude them explicitly from simulations.
- Empirical masks and identifiers remain local, never committed. Reproduction
  reloads through Country. Only aggregate coverage and simulation summaries are
  tracked. Preserve fine-good pair counts, not just marginal observation rates.
- Equal-within-group loadings and homoskedastic fine-good log errors do not
  ensure homoskedastic grouped errors: the number of positive components varies
  across households. Keep this effect in the primary masked-DGP experiment;
  a substitution experiment may hold the group's baseline amount fixed by
  dividing by the number of positive members, with that assumption disclosed.

## §5 Reuse decisions

- **Reuse** `Regression` and `score_w`; no alternative beta or welfare estimator.
- **New** pure summation over explicit membership maps: `_relabel_j` requires a
  country's stored mapping column and cannot accept arbitrary trial partitions.
  Follow its summation convention without changing Country or `labels=`.
- **New** evaluator/report and fixed normalization: existing demand entry point
  fits one sample; it does not implement the agreed comparison.
- **Extend** the tested DGP externally with complete, substitutable-input, and
  unreported-positive regimes. The two latter mechanisms share the observed
  zero convention but have different latent interpretations.
- **Extend** the same DGP and evaluator with survey masks, known welfare, and
  existing curated groups. Reuse `prepare_data` diagnostics to separate fitting
  recovery from subsequent frozen scoring, rather than duplicating selection.

## §6 Remaining choices

Empirical country/wave population weights, a defensible anchor or anchor basket,
an approximation tolerance, and the cost of leaving welfare unscored remain
research choices. They do not block a configurable evaluator or known-truth
checks. No automatic search or empirically optimal Uganda grouping is claimed.

### Verification

- `aggregate_expenditures` (`SkunkWorks/aggregation/evaluation.py:72`) --
  **OK (anchored on §2, §3, §5)**: exhaustive maps, nonnegative level sums,
  union of positive support, and shared zero/unreported convention; no Country
  API or extraction changes.
- `evaluate_partitions` (same file:101) -- **OK (anchored on §2, §4, §5)**:
  reuses fitting, frozen scoring, and the per-market covariance rank signal;
  separates coverage from fixed-population MSE; holds normalization to training
  data; retains failures. No pre-existing evaluator was found on re-search.
- `simulation_inputs` (`SkunkWorks/aggregation/simulation.py:20`) --
  **OK (anchored on §2, §4, §5)**: reuses `dgp.expenditures`, adapts old index
  names, and restores NumPy global random state. Truth only enters reports.
- Tests: **22 passed**, including identified versus unidentified markets,
  household splits, truth/held-out-data independence, weighted normalization,
  and missing comparison/reference scores. Reviewer reproduced the market
  defect before the fix and found no remaining defects after the fix.
- Experiment: 36 fits (seeds 11, 12, 13; four zero/loading regimes; three
  supplied partitions), all processed successfully. No country-data access or
  empirical optimality claim. Commands and results are recorded in
  `SkunkWorks/aggregation/evaluation.org`; checks used the CFE checkout at
  `be42621` / code commit `3cdfae9`. Import provenance was asserted.
- Org lint and ASCII checks pass. Full LSMS country tests were not run: this
  isolated analysis does not change library or extraction code, and its test
  command bypasses country-cache purge hooks.

--Sue, 2026-09-16
