# Narrow documentation descendant confirmation

**PASS** at `780f6798683c88a9a5f6844dd355da96c7eaace1`.

Compared with fully reviewed `3f520e09ff2d21501e30d8ac657f7f8ea571152d`, exactly one paragraph in `lsms_library/countries/Uganda/_/CONTENTS.org:833` changes: +3/-1, exactly the reviewer-proposed wording. All other 14,883 Git tree entries, including runtime, tests, configuration, ledger and evidence blobs, are identical. Diff check is clean.

The replacement correctly distinguishes the Code-to-Preferred build decoder from query-time categorical source/target selection. This resolves the documentation qualification in the full review without changing the resolver.

Independently recomputed all 549 country/table hashes and all 39 tagged framework fingerprints under the descendant using matching import/config realpaths, private empty data root, offline namespace and single numerical/build threads. Every hash matches the reviewed head: **zero additional invalidation**. `country.py:986` explicitly excludes CONTENTS.org from Org build inputs, with the same exclusion used by country hashing. No data cohort or tests were rerun; no parquet was written.

Both the frozen reviewed worktree and descendant worktree remain clean at their pinned heads. Every tracked-file SHA256 remains unchanged through this narrow review (14,884 per worktree). No review process remains after the final check. Full source/runtime approval remains pinned to the original reviewed head; this confirmation covers only the documentation descendant.

Reproduction:

```sh
/local/job40255479/venv_img/bin/python /local/job40255479/control/isa-backlog-20261021/review-1077/doc-descendant/check.py
bash /local/job40255479/control/isa-backlog-20261021/review-1077/doc-descendant/run-hashes.sh > /local/job40255479/control/isa-backlog-20261021/review-1077/doc-descendant/hashes.log 2>&1
/local/job40255479/venv_img/bin/python /local/job40255479/control/isa-backlog-20261021/review-1077/doc-descendant/finish.py
```

Sue
