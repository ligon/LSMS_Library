# GH #1006 bounded reproduction

Baseline: `f8d236694c26fe9e709c5548f02c70f74ea72c33`.
Runtime test commit: `1eb9bb377157c78f74962c7a9de59807d446b239`.
Writer checkout: `/local/job40255479/worktrees/isa-Uganda-1006`.
Private evidence: `/local/job40255479/control/isa-backlog-20261021/Uganda/1006`.
Interpreter: `/local/job40255479/venv_img/bin/python`.

The exact completed commands, from the writer checkout, are below. Runners and full logs are private because API warnings/frames can contain source keys. `writer/run.sh` sets the exact checkout/config/data roots, numerical/build/make threads one, no cache-bypass/backend override, active method guards, and `unshare -rn`. The pytest runner pins three workers with `--dist=loadfile` for scoped/supporting selections; absence controls run serially.

```sh
bash /local/job40255479/control/isa-backlog-20261021/Uganda/1006/writer/run.sh writer/run_tests.py scoped
bash /local/job40255479/control/isa-backlog-20261021/Uganda/1006/writer/run.sh writer/run_tests.py supporting
bash /local/job40255479/control/isa-backlog-20261021/Uganda/1006/writer/run.sh writer/run_tests.py empty
bash /local/job40255479/control/isa-backlog-20261021/Uganda/1006/writer/run.sh writer/run_tests.py credentialless
bash /local/job40255479/control/isa-backlog-20261021/Uganda/1006/writer/run.sh writer/check_controls.py
bash /local/job40255479/control/isa-backlog-20261021/Uganda/1006/writer/run.sh writer/compare_frames.py
bash /local/job40255479/control/isa-backlog-20261021/Uganda/1006/writer/run.sh writer/verify_state.py
```

The scoped pytest selection is exactly `tests/test_food_prices_dtype.py tests/test_panel_attrition.py tests/test_uganda_tables.py`; its expected result is 25 passed, zero skips/failures. Supporting selection: `tests/test_food_prices_units_kwarg.py tests/test_wave_getattr.py tests/test_add_market_index_dedup.py tests/test_sample.py::TestUganda2009MarketFallback`, expected 41 passed, zero skips/failures. Both use `-p no:cacheprovider -q -rA -n 3 --dist=loadfile`, private JUnit output and private basetemp. Empty/credentialless expected results are ten pure passes and 15 data skips, preserving the actual requires_s3 policy.

Setup was one physical copy, from the pinned 1,519-file release manifest at `/local/job40255479/control/release-v016/verification-prep-seed1519/copy-manifest-private.json`, selecting Uganda and excluding the four derived names plus nutrition/fct. The 170 selected L2 files were copied from the quiescent `/local/job40255479/release-v016-seed-data` only after exact manifest SHA checks. The 581 held Uganda L1 blobs were physically copied using the saved private per-sidecar membership and SHA manifest. L2 from a different integration head was not used. There are no L1 symlinks. Freshness and all-eight-wave coverage were checked through the normal production gates, never bypassed or manually stamped.

For independent review, stage a separate physical copy into a new private root, retaining the same manifest/hash/absence checks and exact frozen-checkout/config assertions. Adapt only checkout/output constants in copied private runners, not the tests or source/cache hashes. The preparation and four restoration-control roots are setup-once and intentionally reject an already existing destination; do not delete/reuse evidence roots to get another run. Four controls cover original LSMS_DATA_DIR present/absent crossed with ordinary return/injected exception; scoped MonkeyPatch restores exact presence/value, finally clears data_root memoization, and no derived file is ever manufactured or removed. Copies are retained as private evidence.

Source-safe aggregate results and private runner SHA256 values are in validation.json. Reference snapshots are the nine private f8 frames captured during the read-only diagnosis and two attrition matrices. Comparisons are exact, including attrs and axis metadata, with no path normalization. No source download, DVC CLI, shared-cache access/write, fixture rewrite, nutrition/FCT call, full-suite run or country census is part of this reproduction.

-- Sue
