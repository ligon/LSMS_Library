# Prior-art ledger — Malawi 2024–25 combined weights

Search tier: git + ripgrep; charter-only grounding, 2026-11-07.
Inherits `STANDING.md`. Code baseline 65b1730f0; unfinished 2024–25 wiring
83a0ec252. Re-pin current development before research; record any source changes.

## §1 Task
Determine whether and how IHS6 and retained IHPS interviews can support a valid
combined cross-sectional estimator. Deliver the identification result, candidate
estimators, evidence and variance implications under the linked charter. No
production weighting policy is selected.

## §2 Existing machinery
| Source | Existing role | Verification / reuse |
|---|---|---|
| Malawi 2024–25 `_/data_info.yml` and `_/mapping.py` on the wiring branch | Three sample-membership patterns, source weight columns, transfer of the 815 shared interviews' IHPS identifiers/weights onto their IHS6 rows | `test_gh1275_malawi_2024.py` contains source/spine/weight tests; reuse and revalidate |
| Malawi `_/malawi.py` and committed panel crosswalks | Existing ID conversion and refreshment-copy removal | Existing wiring tests; do not reinvent linkage |
| `country.py::_normalise_sample_weights`, `_SAMPLE_WEIGHT_COLUMNS` | API-time per-wave mean-one normalization of weight/panel_weight, independently | `test_weight_normalisation.py`, `test_sample.py`; read raw scales for research |
| `.coder/ledger/weight-normalisation.md` | Existing scale convention, raw cache/API distinction and test history | Reuse definitions; no change in this investigation |
| IHPS BID §3.2.1 C1–C8 and §3.2.3 L1–L5 | Documented component weights, pooling, calibration, trimming | Verified DVC-backed PDF; primary definition source |
| `local_tools.get_dataframe`, `data_access.get_data_file` | Sanctioned data/document access | Inherit STANDING.md IO rules; private roots |

No combined estimator implementation has been established by this charter scan.
The research phase must search existing sampling/weighting analyses and primary
methods before classifying any estimator prototype as new.

## §3 Definitions
Use charter §0–§1 and IHPS BID as the question/design anchors. A final estimation
weight is not automatically an inverse selection probability. A pooling
coefficient is not automatically a selection probability. Keep reported final,
intermediate, model-based and normalized quantities distinct. Retained membership
in the IHPS cross-section is different from membership in IHS6.

## §4 Invariants
Inherit STANDING.md §4; preserve one row per interview and original source
weights. Do not pool independently normalized API weights as expansion weights.
Do not assume independent or disjoint sample components without design evidence.
No production source, API, fixture, dependency or shared-cache changes are covered.
Read full Malawi CONTENTS including live caveats before investigation; report
contradictions with their source passages.

## §5 Reuse decision
Reuse the existing readers, identity spine, source weight columns, normalization
definitions, source oracles and DVC documentation. Extend research diagnostics
only after completing the design/input inventory. New estimator prototypes need
a formula, named assumptions and a search demonstrating why existing machinery
does not already supply them. Candidate failure or non-identification is a result.

## §6 Open questions
Primary population/time estimand; frame support and actual overlap; intermediate
weight/design metadata availability; covariance and variance identification.
See charter §5. Decision 48 and every production weighting edit remain held.

## Verification
Pending research. This ledger records existing machinery and constraints only;
historical weight totals are not newly reproduced findings.
