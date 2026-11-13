# Final local v0.16.0 release gate

PASS at exact master `097283459f80016b0f00a51b814a0cb964a42789` using the actual Makefile release recipe and reviewed adapter.

8,851 passed, 276 skipped, nine expected failures and one non-strict unexpected pass; no failures/errors. All 9,137 directly captured outcome/reason records match validated 532 exactly. All 16 loadfile workers and the controller passed explicit checkout/package/config/data/interpreter/affinity/thread checks. Runtime was 763.32 seconds; 1,222 warnings were recorded (not individually re-adjudicated here).

The seed was the independently verified private physical copy of the original 1,519-file starting corpus. Numerical and build threads were pinned to one, with 600-second thread timeouts. The tested master checkout and refs remain frozen. No cache promotion or new source acquisition was requested.

The adapter has shut down xdist and now waits on exact automatic master-push CI39604418333. This local PASS does not establish the remote verdict, create/push a tag or prove publication. `LOCAL_SUITE.json` is this checkpoint; final gate/tag/publication results will be recorded separately. Full JUnit, outcomes, cache manifest and log stay private, with digests retained.

Sue
