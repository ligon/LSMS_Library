# Independent review instructions - GH1091 Niger

Review the final source head named in private READY.json, with runtime/tests
exactly at 6be6576607f1862b603d123acfb74ace4026fd3f and reviewed GH1088 parent
d602f8d63520e521a730f85551b366c4d45d9a35. Read complete Niger CONTENTS including
tasks and the ledger first. The author status is ready/pending review, not PASS.

Two qualifications require explicit assessment:

- The original auxiliary direct-Wave collector failed its zero-write assertion
  before recording the exact delta. Later zero-write observations/full native
  equality do not identify or reconstruct that delta. Keep it separate from
  the completed normal Country/Make matrix.
- First-freeze packaging replaced the private detailed aggregate validation.json
  with the committed safe export before creating its payload manifest. The
  original final aggregate size/SHA were not recorded. The earlier 32,213-byte
  listing preceded metadata enrichment. Retain the replacement safe file and
  both freeze attempts. validation-reconstructed-private.json is a new
  reconstruction from unchanged frames/per-probe JSON/receipts, not original
  bytes. No author API/source/test replay was used for recovery. Verify the
  packaging-incident receipt and complete first-to-final changed-path inventory.

Use the original immutable per-probe frames, warnings, source/copy receipts and
logs as evidence, with exact float comparisons, explicit MultiIndex internals
and recursively typed full attrs. The first freeze performed complete byte
receipts; corrected packaging reuses them with raw-copy metadata identity
checks rather than rereading source data. Safe exports are isolated in
safe-exports/. Do not overwrite either freeze or infer broader coverage from
the reconstructed aggregate.

Challenge actual no-country-builder/Make DAG behavior, both supported singletons,
unsupported requests and both backends at cold/populated/fresh warm stages.
Parent Make wrong-wave results are not acceptance references. Required columns,
46043 observations,30late obs,76exact masks, food/derived controls, matched
Feature default/currency=None and healthy Mali40218 must survive. Empty dtype,
native serialization, CLI PopulationRecord-to-dict and natural hashless Make
rewrites are existing explicit route consequences, not normalization targets.
Food cold/warm u dtype remains GH1196. Reconcile the actual three scanner records;
there is no community_prices phase-2 kwargs grid.

No whole-corpus build or broad suite is requested for this narrow country
removal. Respect the original offline/private-source constraints; use a new
immutable review checkout/root for any independent probes. Preserve the original
11-source/44-copy gates and distinguish author tests from independent results.
No GitHub writes, issue closure or public merge is authorized here. Eventual
stack parent is PR1176's actual fix/1088-niger-quantity-sentinel-20261021 branch.

Sue.
