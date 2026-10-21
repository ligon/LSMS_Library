SOURCE round-trips now retain unknown, repeated and empty metadata occurrences and keyword-looking notes. Primary acquisition URLs come only from explicit `CATALOG_URL` or the pre-metadata body. Contradictory source/holding metadata and unsafe scalar rewrites refuse explicitly before changing destination bytes.

Backfill preserves the held release, records separate latest-release/first-seen observations, and never fills an absent held IDNO from catalog metadata. Stamped primary-entry corrections require acquisition or explicit curation. Failed cross-entry downloads leave existing SOURCE bytes intact. A requested push must confirm every downloaded path before release stamping; explicit `push=False` still supports a complete local download.

Independent review passed 370 repository tests and 30 reviewer controls. Its first round caught a catalog-identity loss; the narrow refusal guard and destination-byte regressions repair it. A fresh 130-file SOURCE census shows unchanged inputs and zero information loss/drift; all 549 country, 2,139 wave and 40 table-plus-aggregate fingerprints remain unchanged. Producer evidence is in `slurm_logs/framework_campaign_2026-10-20/1115-1128/`, including `review-fix/`.

Fixes #1115.
Fixes #1128.

Code only: no shipped SOURCE/CONTENTS backfill, catalog acquisition or queue stamping. Nested replacement, staging, retirement and raw-file/sidecar rollback remain #1149; these metadata guards do not prove served country configurations switched vintages.

Dependency: this draft targets `integration/framework-v016-40255479` at `852ac2bfc` to keep #1154–#1157 out of the diff. After those land, update this publication branch from development and retarget the PR. Do not merge into the temporary comparison base.

Final combined source including the separate #1160 fix is `532d58700fa46e52a0b390198604eca1dc4de00e`: **8,851 passed, 276 skipped, nine expected failures, one non-strict unexpected pass; zero failures/errors or credential skips**, in 736.35 seconds. All 87 new cases pass and every existing outcome/reason matches the prior 4cab run. Unit, Windows smoke and credentialed native Windows also pass at that exact head. Credentialed Linux [run 39533863014](https://github.com/ligon/LSMS_Library/actions/runs/39533863014) remains **PENDING**; the earlier 4cab run is preserved separately. Final-source packaging and installed config smoke pass within their documented limits. No pending/skipped data job is represented as a pass, and no release is proposed.
