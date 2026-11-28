# Prior-art ledger — #1744 typed Site Q scan findings

**Base:** 47df62d49e614c9a56e319def7324145d138c3d9. **Search:** git/ripgrep
and source inspection. Inherits [STANDING](STANDING.md).

## §1 Task

In the Feature audit harness classify the existing Site Q captured warning
types, keeping them advisory and avoiding offender identities in output.
No change to production screens, thresholds, strict mode, grader or data.

## §2 Existing machinery

| Symbol | Base anchor | Contract |
|---|---|---|
| `scan._build`, `_warnings_to_findings`, and `WARNING_TAXONOMY` | `bench/feature_audit/scan.py:142–231` | Capture warnings in all phases, currently coerce to text, preserve legacy substrings for unrelated warnings. |
| `Finding` and `to_record`, `run` | `scan.py:168–190,785` | JSONL fields and `feature\|country\|kwargs\|check` fingerprint. Worker results transport findings, not their captured warnings. |
| `QuantityImplausibleWarning` and `SurveyKgPriceWarning` | `lsms_library/quantity_audit.py:597–614,1733–1740,1814–1831` | Price comparison is the subclass. Both classes may also report that a screen failed to run. |
| `_format_offender`, `_format_survey_kg_price_report`, `quantity_reports` | `quantity_audit.py:1570–1648,1743` | Warn text can embed offender identities and values, and the ledger carries all offenders. Neither belongs in the new scanner findings. |
| `cluster`, filing payload | `bench/feature_audit/cluster.py:47–83`, `audit.workflow.js:166–201` | Group on check, severity, status and normalized detail. The workflow also forwards raw `example_detail` and uses the example fingerprint as an audit key. |
| Coverage grader, Tier C semantics | `lsms_library/coverage_matrix.py`, harness README and cropkg red-team F8 | Current grader ignores warnings. A runtime warning is not a defect verdict. |

## §3 Definitions

Site Q remains report-only by default (GH #857 ledger and the #1559 warning
subclass). Severity C stays a triage candidate, not an assertion that a result
is wrong or even that offending rows exist. The phase 1a country is the audited
call's scope, and assembly phases keep country unnamed rather than parse a
source-country identity. Feature denotes what was audited, not a possibly
different source table named in the warning.

## §4 Invariants

Each existing emission yields one finding. Match the actual captured class
and test the price subtype before the ordinary superclass. Use fixed,
class-level summaries for the two typed findings, with no formatter parsing,
ledger intake, raw warning copies, keys, values or exception text outside the
worker's warning capture. Unrelated warnings keep their old detail, severity,
expected, check and unnamed-country behavior. Keep record shape, phase labels,
kwargs and fingerprint composition; typed checks intentionally have new,
separate fingerprints instead of legacy generic identities. Repeated texts
of the same class group by class safely. No grader, strict policy, producer,
cluster or Github workflow edits.

## §5 Reuse

Preserve the existing capture point, record type, JSONL and cluster routes.
Use just the warning types for classification, and keep the existing
substring classifier for all other warnings. Use synthetic test producers
and the real warning/capture/finding/cluster interfaces.

## §6 Scope

No source or real survey frames are needed, and no remeasurement of the six
historical findings is claimed. Phase 1b/2 have no sound country attribution.
The legacy raw-detail behavior of unrelated warnings is out of scope.

## Verification against the ledger

- OK (§§2–4): scanner keeps the warning class in local capture. Three phase
  tests prove subtype priority, scoped attribution, kwargs and legacy
  warning behavior. Spoofed text and a different class with the same name
  remain generic. A typed warning stays advisory if the build later raises.
- OK (§4): worker-compatible Finding pickle and JSON records retain the
  original schema. Distinct typed check keys yield distinct stable
  fingerprints. Repeated warnings with different row text still have
  identical dedup fields. Neither records nor clustered examples contain
  the invented offender identifiers. Existing phase-2-only warmup remains
  unreported. Strict/error reporting and unrelated warning details are
  outside this reclassification and unchanged.
- OK (§5): 56 offline source-free passes in the final selection, including
  the three scanner test files, two ordinary quantity controls, and three
  survey-kg producer/strictness controls. The new suite was red on the
  base (five targeted failures). Package realpath and one-CPU affinity
  were asserted before tests; state, cache, temp and config stayed private.
- The tracked runtime library, coverage tooling, clusterer and filing
  workflow remain byte-identical to the base. No survey build or real
  audit sweep ran, and no publication took place.
