SCOPE DEVIATIONS

None. Read-only Actions/master-ref monitoring and local source identity inspection. No dispatch, cancellation, GitHub/ref/source/cache mutation or publication action.

Master push CI [38759333750](https://github.com/ligon/LSMS_Library/actions/runs/38759333750) at exact source `cb44e3522a2426ba03431ca9a218cc63fe03f6c9` is **success** as observed 2026-10-15T16:18:36.130941+00:00. Pending/skipped jobs are not passes.

Live GitHub master at 2026-10-15T16:18:38.254473+00:00: `cb44e3522a2426ba03431ca9a218cc63fe03f6c9`. Matches expected merge source: **True**. Master tip ever changed during this monitor: **False**. A change is retained in master-tip-change.json/history and must be reviewed before release publication; success for an older CI source does not silently approve a new tip.

| Job | State | Actual terminal result |
|---|---|---|
| [data-tests](https://github.com/ligon/LSMS_Library/actions/runs/38759333750/job/116506129612) | success | 4927 passed, 2768 skipped, 4 xfailed, 1 xpassed, 1004 warnings in 3137.91s (0:52:17) |
| [unit-tests](https://github.com/ligon/LSMS_Library/actions/runs/38759333750/job/116506130011) | success | 3293 passed, 4407 skipped, 60 warnings in 132.36s (0:02:12) |
| [dvc-remote-reachability](https://github.com/ligon/LSMS_Library/actions/runs/38759333750/job/116506130068) | success | No pytest summary |
| [windows-script-build](https://github.com/ligon/LSMS_Library/actions/runs/38759333750/job/116506130111) | success | Uganda:shocks rows=14457 cols=10 waves=8 digest=64a53fa7af55 (65s); Uganda:assets rows=116808 cols=1 waves=8 digest=9202ed542a14 (56s) |
| [docs](https://github.com/ligon/LSMS_Library/actions/runs/38759333750/job/116506130114) | success | No pytest summary |
| [windows-smoke](https://github.com/ligon/LSMS_Library/actions/runs/38759333750/job/116506130148) | success | 327 passed, 1 skipped in 8.69s |
| [dvc-lock-check](https://github.com/ligon/LSMS_Library/actions/runs/38759333750/job/116506130200) | success | No pytest summary |
| [refresh-dvc-locks](https://github.com/ligon/LSMS_Library/actions/runs/38759333750/job/116507798258) | skipped | Skipped by condition |

The source is the two-parent PR999 merge commit. Its full tree equals reviewed final-docs2e41feda3; runtime/config excluding CONTENTS, tests, workflow, dependencies and release recipe equal successfully validated3914d8804. See source-identity.json for exact parents/tree.

Master push additionally enables documentation and DVC-lock checks. Any lock-refresh/master advance is monitored explicitly. The local make-release process is owned by the parent and is separate; this CI report does not claim its result or a tag/publication.

Completed-job raw logs remain private and may contain survey warnings. Aggregate metadata records exact job/source identities, log sizes/SHA256 and actual summaries. No in-progress log fetches. The monitor waits for two terminal metadata snapshots, captures each completed log or records three failed attempts, then stops; inspect monitor-finished.json when present. Native expected-output comparison is recorded in native-validation.json when its terminal log is available.

## Terminal data evidence

4927 passed, 2768 skipped, 4 xfailed, 1 xpassed, 1004 warnings in 3137.91s (0:52:17). The marker stream reconciles exactly to the terminal counts.

The preceding implementation manual run38746123119 is compared by actual module outcome counts, without re-executing tests. Module-count differences: 0. See semantic-review.json. Same totals do not establish individual skipped reasons, which were not printed.

| Previously failed Uganda case | Actual master outcome |
|---|---|
| `tests/test_table_structure.py::TestTableStructure::test_no_duplicate_rows[Uganda/assets]` | passed |
| `tests/test_table_structure.py::TestFeatureSanity::test_feature_is_sane[Uganda/assets]` | passed |
| `tests/test_uganda_invariance.py::test_parquet_matches_baseline[var/nutrition.parquet]` | passed |

These node positions reuse the prior unchanged-source collection proof, calibrated against the original three failure positions. No pending/skipped test is relabeled as passed. Four expected failures and the existing unexpected pass, if present in the actual counts, retain their pytest categories.

| Completed job | Bytes | SHA256 |
|---|---:|---|
| data-tests / 116506129612 | 457010 | `b049a9fc8cace05fe2b03598c8cd706e6d6c58a52e5f62bb2d31a4d613934b03` |
| unit-tests / 116506130011 | 85161 | `c7d8d833407d60e8a06c46ee5b9cce66d37aa7cc76b5a7b5d522adc56a563d21` |
| dvc-remote-reachability / 116506130068 | 20779 | `ce29b614395accfe5e011acb0078da370c7b0d8f96bdb1c840c8b06dd5aca30a` |
| windows-script-build / 116506130111 | 138135 | `b4c0fc74885d8e96e4ad669ff5b480f1bbb7cce4b6cc82d13faa4d027c04253a` |
| docs / 116506130114 | 45353 | `598277011dfc22ed933fe9198db0bfa852294ff53e247a4a7b4db5a8e6b6c7b7` |
| windows-smoke / 116506130148 | 94010 | `03ec4ec390e6fe35608f5c3d02182f913bbb64cbcb5a0aa207ac301b06cd95c0` |
| dvc-lock-check / 116506130200 | 44780 | `a24780d2cbb720cac72019647757dd1aa3c28b9d994fbb2ec8bbc63893e8f791` |
