# Master release CI: platform jobs

PASS for the owned platform scope of [run 39604418333](https://github.com/ligon/LSMS_Library/actions/runs/39604418333), automatic master push at `097283459f80016b0f00a51b814a0cb964a42789`.
All six executed platform jobs independently expose that exact checkout in their completed logs.
Linux data-suite completion and the whole workflow verdict are reported separately; neither is inferred from this platform PASS.

| Job | Result |
| --- | --- |
| unit-tests | success; ========= 4685 passed, 4452 skipped, 70 warnings in 266.86s (0:04:26) ========== |
| windows-smoke | success; ======================= 327 passed, 1 skipped in 9.03s ======================== |
| windows-script-build | success; installed code/config checks and real cold tables below |
| dvc-remote-reachability | success; credentials configured and remote sweep step passed |
| dvc-lock-check | success; per-step outcomes retained in verification.json |
| docs | success; documentation build/upload/deployment steps passed |
| refresh-dvc-locks | skipped; no automatic refresh was scheduled |

The actual native Windows installed-library run produced:

| Table | Rows | Columns | Waves | Digest |
| --- | ---: | ---: | ---: | --- |
| Uganda:shocks | 14457 | 10 | 8 | `0ab2ea4c79dd` |
| Uganda:assets | 116808 | 1 | 8 | `9202ed542a14` |

Both match the approved expectations. The native job's installed-code/package-config assertions pass;
its Make variable expansion has a nonempty parquet list and zero find errors. S3 credential configuration
and the actual cold-build step both succeed. This is actual 097 master evidence, not reuse of historical
native results. It exercises two installed data paths, not all installed microdata or scientific policies.

The exact run's Linux `requires_s3` gate is already successful in job metadata; the data monitor owns
its raw log, complete suite and credential reconciliation. Platform raw logs contain zero literal
`NoCredentialsError`, `PartialCredentialsError`, `UnmarkedS3TestWarning` or runtime credential-skip
marker witnesses. Unit/smoke authentication is intentionally disabled and their skips remain skips;
this report does not individually adjudicate the entire skip population.

`verification.json` retains each job/step result, actual checkout SHA, log byte count/SHA256, native
dimensions/digests and literal credential-indicator counts. Full logs and raw API metadata stay private.
Logs were fetched only after their jobs completed. Polling used read-only GETs every 45 seconds;
no workflow dispatch/rerun, Git/GitHub mutation, package import, local test or cache operation occurred.

All owned monitoring/log-fetch processes are stopped. Release owner still owns the remaining Linux
gate, local Makefile gate, tag and publication.

Sue
