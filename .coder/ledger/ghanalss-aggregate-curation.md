# Prior-Art Ledger -- GhanaLSS Aggregate curation

Search tier: git and ripgrep, Country API and local Org tables. Inherits
`STANDING.md`. API audit passes (15 skills). No configured graph/memory tool.

## §1 Task, restated

Produce a reproducible inventory of the Preferred food labels actually
delivered for GhanaLSS and a concrete draft country-level aggregation map.
Attach observed support and reasons to proposed groups. This step prepares
the semantic candidates for statistical evaluation; it does not publish an
unevaluated Aggregate column or require the historical wave Aggregate columns
to agree. The user explicitly corrected their non-operational status.

## §2 Existing machinery

| machinery | source / test surface | decision |
|-----------|-----------------------|----------|
| `Country.food_acquired`, `sample` | `country.py`; country tests | Reuse API-delivered labels, visit/source grain and full sample denominators. |
| `food_expenditures_from_acquired` | `transformations.py:2556`; food transformation tests | Reuse for wave totals with explicit purchased/total basis, no new valuation. |
| `df_from_orgfile`, `countries_root` | `local_tools.py`, `paths.py`; `test_ghanalss_food_label_canonical.py` | Reuse for declared source vocabularies and provenance. |
| `Country._relabel_j` | `country.py:3031`; `test_label_selection.py` | Future publication reads the COUNTRY table only. Wave builders use Preferred labels. |
| `aggregate_expenditures` | `SkunkWorks/aggregation/evaluation.py:72`; `test_evaluation.py` | Reuse exhaustive level summation for candidate support comparisons. |
| `template_from_frames` | `SkunkWorks/aggregation/survey_simulation.py:54`; `test_survey_simulation.py` | Pattern for sample denominators; do not call Uganda-specific Region/curated-map requirements. |
| `orgtbl.py` | `lsms_library/util/orgtbl.py`; parent food-acquired skill | Existing future column writer; no new production writer needed in this draft step. |
| `interview_date` | GhanaLSS `data_scheme.yml`, `.coder/ledger/ghanalss-visit-dates.md` | Dates are incomplete and a missing date is not an unasked visit; do not infer universal exposure from them. |

No existing item-by-wave aggregation review inventory was found in SkunkWorks
or bench. The general coverage matrix concerns table readiness, not positive
food observations or semantic membership.

## §3 Definitions and conventions

Use `STANDING.md §3` and GhanaLSS `_/CONTENTS.org`, especially "The
repeated-visit design", "Section 8H own production", "GLSS3 is stratified",
and "One food vocabulary, not seven". Use `SkunkWorks/cfe_aggregation.org`
and `semantic_curation_proposal.org` for whole-group admissibility and the
distinction between a semantic proposal and statistical certification.

- A fine item is the exact Preferred `j` delivered by the API. A raw code,
  a country-table row, a country Preferred label, and a delivered wave label
  are not interchangeable; preserve their correspondence explicitly.
- Positive support means positive monetary Expenditure, not row presence or
  a reported price. Zero and unavailable amounts contribute zero to sums.
- `visit` is a calendar contact for GLSS3--7. GLSS1/2's food `visit=1` is a
  consumption occasion recorded at contact 2; do not join them mechanically.
- Total expenditure includes existing derived own-production values. Modern
  8H uses quantity times farmgate price; the early 12B construction differs;
  GLSS3 own-production expenditure is not wired. Keep sources/bases distinct.

## §4 Invariants and assumptions

- Inherit sanctioned IO and cache constraints from `STANDING.md §4`.
  No DVC CLI, cache purge, or production extractor changes.
- Preserve sample households with no food records in wave-level denominators.
  Report food households outside sample separately, and do not silently lose
  them from label discovery or expenditure accounting.
- Per-visit counts do not establish that non-reporting households were asked.
  GLSS3 has seven rural / ten urban asks, and its fieldwork schedule is not
  perfectly recoverable from analysis Rural. Report per-visit positive counts
  without manufacturing a universal visit-completion denominator.
- Inherit existing source amounts, including derived values. Do not infer
  monetary values from quantity or prices in this analysis.
- Draft membership covers every delivered label exactly once. Uncertain items
  remain named singletons; no residual Other bucket, automatic string-matching
  merges, or certification from missing/unidentified betas.
- Each proposed group records exact members, reasons, source evidence and
  draft status. Historical wave Aggregate columns are optional provenance,
  never constraints, live consumers, or the source of an automatic mapping.
- No household records/identifiers are committed. Outputs are item/group/wave
  summaries. The scope is within GhanaLSS; shared display names do not certify
  cross-country equivalence.

## §5 Reuse decisions

- **Reuse** Country extraction, expenditure derivation, source table readers,
  and exhaustive candidate level aggregation. No new estimator or selector.
- **New** a thin SkunkWorks inventory/report driver: the survey simulator is
  Uganda-specific and the general coverage matrix lacks item/visit support.
- **New** an explicit GhanaLSS draft group specification and generated review
  table. These are concrete inputs to the already implemented evaluator, not
  an implementation of a generic curation service or an automatic search.

## §6 Remaining choices

Statistical selection and publication follow the inventory/draft stage. The
precise acceptable approximation error, real-data validation targets, and
handling of unresolved semantic cases remain choices for that stage. They
do not block a complete, conservative draft or support measurements now.

## Verification

Pending implementation and review against the invariants above.

--Sue, 2026-09-16
