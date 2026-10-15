Corrected CI completed on October 15, 2026. It is a failed run with a complete
pytest summary, superseding the earlier in-progress status.

[Run 38697245468](https://github.com/ligon/LSMS_Library/actions/runs/38697245468)
used source `9d5bf200f170f322076626386689b498bebb1c06` and retained the full
sequential credentialed selection, 600-second timeout and pinned build/BLAS
thread counts. The aggregate record in `completed-1010-ci.json` includes job
identities, timestamps, source verification, counts and private-log hashes.

| Job | Result |
| --- | --- |
| unit-tests / 116296632079 | 3,288 passed, 4,407 skipped; success |
| windows-smoke / 116296632418 | 327 passed, 1 skipped; success |
| data-tests / 116296632216 | 3 failed, 4,919 passed, 2,768 skipped, 4 xfailed, 1 xpassed; failure |

Data pytest completed in 3,140.09 seconds. Its three failures are the Uganda
assets duplicate-index and feature-sanity checks, and the historical nutrition
fixture comparison. The assets compiler expanded 116,808 real observations to
790,688 rows, with 673,841 duplicate raw keys. The nutrition output was
23,777 × 15 versus the then-held 23,801 × 15 fixture. Subsequent repairs and
maintainer approval are separate evidence; they do not change this run's result.

All 163 outcomes in the EHCVM test module and all 20 currency-unit outcomes
passed. The previously timed-out GhanaLSS unitprice test passed in 57.85 seconds.
No historical EHCVM Derivation/inkind failure recurred. The #1010 test flag leak
and its local effect are reproduced and fixed, but the historical #941 cause
remains unresolved; #941 is open.

The older unpinned run 38687547945 ended at 08:54:10 UTC with the annotation
“The hosted runner lost communication with the server.” Its data log returned
HTTP 404 after termination, and no final pytest summary was captured. There
are no inferred totals or specific resource/network attribution. The owned
monitor stopped after all tracked jobs were terminal; no CI run was cancelled
at that step.

The data log remains private (461,456 bytes, SHA-256
`e1895513df52c2c99b7c2cf8b0840b60890f04a469fe6f6afd64992627fe621e`).
This archive contains aggregate evidence only. Final release CI must include
the approved #967 nutrition fixture and reviewed assets compiler correction.
