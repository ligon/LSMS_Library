# #1288 - Uganda income's managed Make inputs

## Prior art and boundary

Uganda CONTENTS.org and the #1062 Uganda sibling-writes ledger establish
framework authority for earnings and enterprise_income, even when its Make
wrapper is invoked. Income materializes each through DVC, restores the exact
backend before reading the matching raw country parquet, ignores finalized
source returns, preserves the household union and writes only income.
The historical 1,810-row partial-cache failure is not the current witness.

The two raw sibling paths in income.py are currently f-string reads. The
existing scanner only accepts literal strings. The explicit private Make
route therefore only knows sources of siblings it happens to copy from a
rich current outer cache. An empty outer cache or a source change can
remove those eligible copies before a private income build. Bind the
unchanged source method, full raw path and columns together in literal
tuples. Reuse _make_other_references, _make_declared_inputs and the existing
Make receipt rather than adding a framework dependency evaluator. Retain
the #1062 DVC pin/finally, raw selection, arithmetic and sibling boundaries.

## Verification

Check managed paths and hashes on actual Uganda metadata. Run the actual
income/Make/public APIs and existing test fixtures with invented native
sources, including a DVC closure witness with absent or partial live sibling
cache, unchanged live bytes, receipt-based warm hit and one sibling-only
source drift invisible to the selected hash. Verify actual held source
parity where finite source custody permits, plus Uganda hash footprint.

No new default/DVC warm receipt policy and no change to direct CLI Make
mtime. Document those conditions honestly. No survey data acquisition,
upstream input policy change, other countries or regressions inferred.

## Observed structural and synthetic checks

The new real-configuration reader check fails before the patch and passes
after it. Actual GNU Make and nested Country APIs with invented
sidecar-addressed CSVs reproduce a cold failure without live siblings.
With the binding, absent and one-wave sibling cases work, standard warm
reuse keeps files unchanged, and a single changed earnings sidecar causes
Make to rebuild income without moving its selected-index source hash or
publishing the private siblings. The previous valid-subset control remains
green. The 32-test focused selection and final metadata cleanup pass.
All 28 Uganda country and 224 wave fingerprints move under the existing
country-script hashing rule. No framework file was changed.

An independent Make-authored warm cache replay found default/DVC still
returns the old values under the same selected hash after that change.
Make refreshes the prerequisite proof and the output at that same hash.
This explicit backend distinction is a residual, not a supported stale
output property to lock in with a tracked test.
