# Final-source cache validation and promotion preparation

Scope: prepare one physical copy of the completed cold corpus; await the
coordinator's final development SHA before any library validation. Shared L2
and the original corpus remain read-only. No source or baseline edits.

Prior art: reuse ../feature-audit/prepare_copy.py and verify_original.py for
physical copy and preservation checks; reuse ../corpus/run_corpus.py for exact
Country.data_scheme enumeration, diagnostics.load_feature dispatch, existing
tests/generate_baseline.fingerprint, hash/footer coverage, and serial tables in
at most two country subprocesses. These are release-control artifacts, not new
product machinery. STANDING.md sections 2–4, CLAUDE.md cache rules, the workplan,
and Uganda CONTENTS define the applicable contracts.

The original cold run at 7fa05179c8519f44ea329ba6387f5d6d6441a8ea covered all
549 declarations (530 success, 19 documented errors). Reuse requires final
source/hash and output comparison, not assuming old cache contents are truth.
The final nutrition fixture decision is now explicitly approved by the user;
older handoff/report statements that it remains held are historical.

Country idiosyncrasy: Uganda has separate wave-concatenation and country-script
builders. CONTENTS says "assets still disagrees with its own script" and
describes a country script output with a different column/index. The final
assets correction must close the affected route, not merely read the existing
canonical warm parquet. CONTENTS also records income/nutrition sibling writes
leaving country earnings/FCT hashless. Generic footer classification 'legacy'
does not override the actual country read policy: stale/rebuild under #809.
No manual restamping is allowed.

Reuse decision: thin control wrappers only. No new cache evaluator, scientific
transform, or baseline generator. Promotion is a future coordinator action,
after final-source validation and an explicit reviewed file manifest.
