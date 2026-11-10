# Prior-art ledger — #932 ready development PR data validation

## §1 Selected contract
User selected option A: full data validation on every ready development PR. Source-only implementation on actual4381d701ffb06ab447a37c66362119d1c1e23009; no workflow trigger, data execution, admin-setting mutation or GitHub publication is authorized here. A required context must never pass by being skipped. Draft/fork refusal precedes checkout/install/credentials. Title/body-only edits must neither restart nor cancel credentialed validation.

## §2 Existing machinery
Reuse ci.yml data-tests credential gate, private_ci_check.py output confinement, Uganda prebuild and full sequential pytest body unchanged. Existing nightly/manual/release jobs remain. Prior proposal reports/932-compatible-pr-data-gate-policy-20261109 and .sucoder/932-platform-check-20261109.json establish User-owned public repo/no merge queue, required unit-tests only, strict=false, existing bypass. Do not apply the former context-only proposal.

## §3 Definitions
Inherit STANDING.md and CLAUDE.md sanctioned data/private-output rules. PR validation certifies an exact synthetic merge and event head/base tuple, not a bare head or moving branch. Strict base freshness plus fresh merge/review checks is needed at release. Skipped/cancelled/dispatch-only jobs are not data validation; existing administrator bypass remains an explicit exception.

## §4 Design and source references
One PR-only workflow uses a permissionless/no-checkout classifier. A dependent always-run job defaults to required name pr-data-validation and explicit failure on classifier/eligibility errors. Only known title/body-only edits use nonrequired pr-data-metadata-edit and a separate job concurrency group; this does not emit a skipped required success or interfere with pending/live data work. GitHub official contexts reference permits needs in job.name and job.concurrency; concurrency documentation warns pending jobs otherwise replace one another. Unknown edited payload changes fail closed. Actual GitHub event/check wiring remains unproven until a separately authorized acceptance.

## §5 Reuse decision
Reuse existing data recipe verbatim. Add only event classification and exact checkout/parents guards plus pure invented-event/source controls; no CI framework/App/API service. Ruleset proposal adds one context and strict freshness while preserving other rules/bypass, but is not applied.

## §6 Verification boundary
Freeze source and finite pure control selection before execution. Controls exercise actual inline event function and checkout predicate with invented JSON/SHA tuples; source controls pin routing/concurrency/fail-closed placement and existing recipe preservation. No imports/tests/collection/data/providers/credentials/workflow triggers yet. ROOT reviews source and execution plan first.
