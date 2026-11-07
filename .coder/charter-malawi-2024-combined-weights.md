# Investigation charter — Malawi 2024–25 combined survey weights

Status: draft for maintainer sign-off, 2026-11-07. Requested investigation;
production weighting changes remain on hold under decision 48.

## §0 Question, restated

Can the library support a defensible cross-sectional estimator using the union
of IHS6 interviews and retained IHPS 2024 households, counting each interview
once? What design information, assumptions and variance procedure does it need,
and what can be recovered from the released weights and documentation?

The motivating question is whether `wt_cross` and `wt_long` can construct a
retained household's probability of inclusion in the representative cross-section.
Distinguish three questions: membership in the already representative IHPS
cross-section; inclusion in IHS6; and weighting the combined library frame.

Do not assume a ratio of final calibrated weights is an inclusion probability.
Conversely, do not assume that inability to recover that probability rules out a
valid composite estimator. Point identification, choice of estimator and variance
identification are separate questions. Pooling is a candidate, not a required result.

## §1 Scope and boundaries

In scope:
- Define population, household/person unit, geographic coverage and reference
  period before comparing estimators. Treat IHS6-only domains such as Dzaleka
  separately; verify Likoma and other coverage differences. Account for the
  refreshment sample's restriction to IHS6 quarter 2 and possible seasonality.
- Reconstruct selection and weighting for IHS6, retained IHPS, and IHPS
  refreshment. Distinguish design probabilities, composition/weight sharing,
  nonresponse, pooling coefficients, calibration, trimming and API normalization.
- Verify the 815 shared interviews, 1,948 retained households and 12,888 IHS6
  households against released identifiers and documentation. A known overlap is
  not proof that other overlap is impossible. State what linkage can establish.
- Establish which intermediate weights, sampling frames, strata/PSUs, selection
  rates, response adjustments and construction scripts are actually available.
- Compare IHS6-only, IHPS-only and combined estimation. Candidate combined
  approaches include design-based union/multiple-frame weighting where inputs
  permit it, and composite estimation with explicit component contributions.
  The existing mixed `weight` column is a diagnostic counterexample, not a baseline
  to preserve. Assess point and variance estimation for every retained candidate.
- Use small reproducible analysis prototypes and simulations in isolated research
  files. A proof that released information is insufficient is a successful result
  if it names the missing quantities and demonstrates the non-identification.

Outside this charter:
- Changing `sample.weight`, `panel_weight`, `ihps_weight`, their normalization,
  the public API, production builders, baseline fixtures or shared caches.
- Choosing a final production policy, merging an estimator implementation, or
  reopening unrelated Malawi wave-wiring, food-unit or shock decisions.
- Generalizing a solution to other countries/waves without a separate scope.
- Contacting survey producers; prepare precise questions if documentation or
  released intermediate quantities are missing, but do not send them.

## §2 Definition of done

- [ ] **Source and design map.** A table traces each sample component through
  selection, observed membership and weight construction, with document/page,
  code or released-variable citations. Every required input is marked available,
  derivable under stated assumptions, or unavailable. Verify document DVC hashes.
- [ ] **Estimand and overlap.** The report states a primary common-population
  estimand, time reference and treatment of exclusive domains. Counts reproduce
  the released one-interview spine; identifier and household-composition limits
  are explicit. Population totals are compared on original expansion scales.
- [ ] **Identification answer.** Derive what the released final weight pair does
  and does not identify. Show the role of the C6 pooling factor and the separate
  cross-sectional/longitudinal calibration and trimming. For any non-identification
  claim, supply a constructive counterexample or a clearly missing independent
  quantity, rather than merely saying the weights differ.
- [ ] **Candidate comparison.** For each baseline and feasible combined approach,
  write its total/mean estimators, component/overlap treatment, assumptions and
  variance procedure. Separate design-based claims from model-assisted or
  assumption-dependent claims. Reject infeasible approaches with named reasons.
  Assess dependence from shared interviews, estimated pooling coefficients and
  shared estimated calibration benchmarks. The BID's retained/refreshment sampling
  independence assertion does not establish independence of final estimators:
  IHPS C7 uses IHS6-derived household totals. Distinguish inference conditional on
  fixed benchmarks from inference accounting for their sampling uncertainty.
- [ ] **Checks that can fail.** Reproduce relevant published/released weight
  totals within their documented rounding precision. Test unique-interview
  accounting, common support, exclusive domains, missing weights, scale recovery,
  and limiting cases. For candidates advanced for use, simulated finite populations
  must exercise overlapping/nested sampling and the proposed uncertainty calculation;
  evaluate bias, empirical
  variance and interval coverage against declared Monte Carlo error, with criteria
  stated before judging results. Check attrition/coverage assumptions explicitly.
  If no combined estimator is identifiable from the available inputs, a constructive
  insufficiency demonstration replaces combined-estimator performance tests;
  mark inapplicable checks explicitly and explain why. Require uncertainty
  simulations only for proposals claiming an implementable uncertainty procedure.
- [ ] **Real-data sensitivity.** On the same eligible population, compare household
  totals, household size, rural/region shares and at least one consumption outcome
  across baselines and feasible candidates. Report weight concentration/effective
  sample sizes, domain/time sensitivity, and uncertainty where identifiable.
  Matching calibration totals alone cannot validate representativeness or precision.
  A comparison blocked by documented identification or input insufficiency may be
  marked not applicable, with that argument checked by the independent reviewer.
- [ ] **Independent challenge.** A reviewer checks the design assumptions,
  overlap/covariance treatment, normalized-versus-expansion scale and evidence
  claims against primary sources and attempts to refute the recommendation.
  Surviving objections are explicit; no synthetic success is presented as proof
  that an unverified real-world sampling assumption holds.
- [ ] **Decision-ready result.** Deliver one of: a supported estimator proposal;
  a conditional proposal with named assumptions/additional inputs; or a documented
  insufficiency result and a precise data/document request. Include the resulting
  choices for decision 48 and API implications. Ethan decides production policy.

## §3 Execution and constraints

After charter sign-off, delegate a source/design investigator and an independent
statistical reviewer. Ground first with the linked prior-art ledger; update it
before adding analysis. Present the design/input map before substantive estimator
prototyping. If it reveals a different estimand or a material missing dependency,
surface that checkpoint instead of silently changing the charter.

Use one worktree based on current development for research and a separately pinned
config tree for the unfinished Malawi 2024–25 wiring (`83a0ec252` at charter time).
Record code/config revisions and import identity. Research scripts and outputs
must distinguish reported quantities from constructed ones. Preserve source weights
and metadata; independently normalized API columns do not preserve their relative
scales. Reuse existing read/ID/normalization machinery; no new dependencies.

Use sanctioned readers and private data roots with BLAS/build workers pinned.
No raw household identifiers in public artifacts. Small single-core probes first;
batch meaningful simulations on the free Slurm account only when needed. No
recurring automation, paid compute or open-ended optimization campaign is chartered.
One research pass and one adversarial revision are the initial scope; report any
remaining blocker rather than turning it into an unbounded implementation task.

## §4 Sources and prior art

- Ledger: `.coder/ledger/malawi-2024-combined-weights.md` and its inherited
  `.coder/ledger/STANDING.md`; existing `.coder/ledger/weight-normalisation.md`.
- Primary document: `lsms_library/countries/Malawi/2024-25/Documentation/ihps_bid.pdf.dvc`
  on development; MD5 `2be2a0b314b01f6c3791b7679bff88df`, 538,484 bytes.
  Catalog 8520, distinct from IHS6 catalog 8507. PDF viewer pp.16–19 (§3.1),
  pp.20–27 (§3.2.1, especially C6–C8), pp.29–30 (§3.2.3, L1–L5).
- Verified named PDF and provenance:
  `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261103/reports/malawi-weights/`.
- IHS6 cross-section report and other documentation sidecars in the same wave's
  Documentation directory; source records in its `SOURCE.org`. Locate and verify
  the IHS6 design and weight-construction documentation, not just the IHPS BID.
- Malawi `_/CONTENTS.org`, including LOGBOOK/WAITING/TODO and the 2024–25
  overlap/weight notes; quote and investigate contradictions rather than declaring
  these notes stale. Read this country record before technical investigation.
- Unfinished wiring branch `isa/wire-malawi-2024-20261104`, wave `_/data_info.yml`
  and `_/mapping.py`; `tests/test_gh1275_malawi_2024.py`, especially shared
  interviews, the IHPS cross-section and source weight checks.
- Existing `country.py::_normalise_sample_weights`, `_SAMPLE_WEIGHT_COLUMNS` and
  `tests/test_weight_normalisation.py`; `tests/test_sample.py` raw-weight checks.
- Historical findings: sweep `reports/wire-malawi-2024/REPORT.md` and
  `reports/review-wire-malawi/VERDICT.md`, Memo A. These are evidence to reproduce,
  not a ruling that the proposed NA assignment is settled.
- Primary methodological references and NSO/WB construction scripts found during
  research must be read and cited; identify which statements are assumptions.

## §5 Open questions and decision gate

The proposed default estimand is the current-wave household population on the
common non-camp survey support, with exclusive domains reported separately.
Confirm or revise it from the design documentation before analysis. Clarify
annual/quarter-specific inference where interview schedules differ.

The investigation must not require pure design probabilities when a justified
composite estimator suffices, or claim such an estimator establishes those
probabilities. Released final weights may support estimation without uniquely
recovering all underlying selection mechanisms.

Decision 48 remains on hold. Approval of this charter authorizes the bounded
investigation and research prototypes; it does not select combined weighting,
NA assignment, independent normalization of `ihps_weight`, or a public API change.
