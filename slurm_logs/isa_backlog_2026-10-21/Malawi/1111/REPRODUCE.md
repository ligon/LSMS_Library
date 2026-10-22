# Reproduction commands

The private scripts and raw evidence are under `/local/job40255479/control/isa-backlog-20261021/Malawi/1111-fix`; `validation.json` records their SHA256s. They contain no acquisition route. Do not run cold preparation over an existing root or delete a root to make it pass: cold commands assert a physically fresh destination. A reviewer should allocate independent private roots and adapt the evidence-prefix constant in copies of the scripts for a new cold reproduction. Preserve the existing producer evidence.

The recorded wrapper selects immutable f8 for `baseline` / `sample-baseline` and this writer checkout for `candidate` / `sample-candidate`; sets exact cwd, PYTHONPATH, LSMS_COUNTRIES_ROOT, private LSMS_DATA_DIR, image PATH, numeric/build/make threads one; and runs image Python under `unshare -rn` plus the active socket guard. Public runtime scripts assert all import/config realpaths and deny a socket probe. Tests use two `loadfile` workers. Full output goes to private log files, not a pipe.

## Recorded cold and source-proof sequence

These are the original runnable commands, in order, with stdout/stderr redirected privately. The first preparation is deliberately non-repeatable on populated roots. It physically copies 28 held Stata blobs and two held PDFs with MD5/SHA/inode/source-immutability checks, plus verified f8 prerequisite caches and a current-hash healthy Uganda comparator. No target plot_inputs parquet is copied for Malawi.

```bash
EV=/local/job40255479/control/isa-backlog-20261021/Malawi/1111-fix
bash "$EV/run.sh" baseline "$EV/prepare.py" > "$EV/preparation.private.log" 2>&1
bash "$EV/run.sh" baseline "$EV/runtime.py" cold > "$EV/baseline-cold.private.log" 2>&1
bash "$EV/run.sh" candidate "$EV/runtime.py" cold > "$EV/candidate-cold.private.log" 2>&1
bash "$EV/run.sh" sample-baseline "$EV/sample_runtime.py" cold > "$EV/support-sample-baseline.private.log" 2>&1
bash "$EV/run.sh" sample-candidate "$EV/sample_runtime.py" cold > "$EV/support-sample-candidate.private.log" 2>&1
bash "$EV/run.sh" candidate "$EV/source_expected.py" > "$EV/source-proof.private.log" 2>&1
```

The independent source script verifies the held IHS3 agriculture p.13 and IHS5/IHPS p.10 MD5s, rereads native slots via sanctioned readers and checks every selected quantity against source-native group sums. Raw/native pairs, IDs and all 235 application-pair members remain private. `sample_runtime.py` starts each supporting sample root with physically copied L1 only, no L2.

## Repeatable warm, full-frame and source checks

These commands are runnable against the preserved private roots. They use ordinary hash/manifest-gated Country/Feature serving, not assumed-fresh APIs. Warm scripts compare full frames/dtypes/attrs and every L2 path/byte/size/mtime; the sample comparison also compares all five wave frames and the country cache across four independent roots.

```bash
EV=/local/job40255479/control/isa-backlog-20261021/Malawi/1111-fix
bash "$EV/run.sh" baseline "$EV/runtime.py" warm > "$EV/baseline-warm.private.log" 2>&1
bash "$EV/run.sh" candidate "$EV/runtime.py" warm > "$EV/candidate-warm.private.log" 2>&1
bash "$EV/run.sh" sample-baseline "$EV/sample_runtime.py" warm > "$EV/support-sample-baseline-warm.private.log" 2>&1
bash "$EV/run.sh" sample-candidate "$EV/sample_runtime.py" warm > "$EV/support-sample-candidate-warm.private.log" 2>&1
bash "$EV/run.sh" baseline "$EV/sample_checks.py" > "$EV/baseline-sample.private.log" 2>&1
bash "$EV/run.sh" candidate "$EV/sample_checks.py" > "$EV/candidate-sample.private.log" 2>&1
bash "$EV/run.sh" candidate "$EV/compare_samples.py" > "$EV/sample-comparison.private.log" 2>&1
bash "$EV/run.sh" candidate "$EV/compare_runtime.py" > "$EV/runtime-comparison.private.log" 2>&1
bash "$EV/run.sh" candidate "$EV/final_census.py" > "$EV/final-census.private.log" 2>&1
bash "$EV/run.sh" candidate "$EV/immutable_freeze.py" > "$EV/immutable-proof.private.log" 2>&1
```

The immutable check compares every tracked file's bytes/mode and index mode against the previously frozen f8/de9 manifests, and requires unchanged clean HEADs. Protected driver/helper/schema blobs are separately compared against `git show f8:<path>` without changing a ref or worktree.

## Tests, meaningful negatives, actual scanner phases, hashes

```bash
EV=/local/job40255479/control/isa-backlog-20261021/Malawi/1111-fix
WT=/local/job40255479/worktrees/isa-Malawi-1111
bash "$EV/run.sh" baseline -m pytest -n 2 --dist=loadfile -q -p no:cacheprovider "$WT/tests/test_1111_malawi_applied_kg.py" > "$EV/new-tests-baseline.private.log" 2>&1
# Expected exit 1: 10 failed / 2 passed, using the immutable f8 drivers.
bash "$EV/run.sh" candidate -m pytest -n 2 --dist=loadfile -q -p no:cacheprovider tests/test_1111_malawi_applied_kg.py > "$EV/new-tests-candidate.private.log" 2>&1
# Expected exit 0: 12 passed.
bash "$EV/run.sh" candidate -m pytest -n 2 --dist=loadfile -q -p no:cacheprovider -p late_label_plugin tests/test_1111_malawi_applied_kg.py > "$EV/postlabel-mutant.private.log" 2>&1
# Expected exit 1: 12 failed. The private plugin mutates only in-memory driver text.
bash "$EV/run.sh" candidate -m pytest -n 2 --dist=loadfile -q -p no:cacheprovider tests/test_1111_malawi_applied_kg.py tests/test_867_nitrogen_content.py tests/test_863_transforms.py tests/test_plot_id_canonical.py -k 'not warm' > "$EV/regressions.private.log" 2>&1
# Expected exit 0: 101 passed. Existing assumed-fresh warm helpers are excluded;
# normal public warm frames and zero-write proof above replace that evidence.
bash "$EV/run.sh" baseline bench/feature_audit/scan.py --features plot_inputs --countries Malawi Uganda --phases 1,2 --jobs 1 --out "$EV/audit-baseline.private.jsonl" > "$EV/audit-baseline.private.log" 2>&1
bash "$EV/run.sh" candidate bench/feature_audit/scan.py --features plot_inputs --countries Malawi Uganda --phases 1,2 --jobs 1 --out "$EV/audit-candidate.private.jsonl" > "$EV/audit-candidate.private.log" 2>&1
bash "$EV/run.sh" candidate "$EV/compare_audits.py" > "$EV/audit-comparison.private.log" 2>&1
bash "$EV/run.sh" baseline "$EV/hash_inventory.py" "$EV/baseline-hashes.json" > "$EV/baseline-hashes.private.log" 2>&1
bash "$EV/run.sh" candidate "$EV/hash_inventory.py" "$EV/candidate-hashes.json" > "$EV/candidate-hashes.private.log" 2>&1
bash "$EV/run.sh" candidate "$EV/hash_scope.py" > "$EV/hash-scope.private.log" 2>&1
```

The hash comparison verifies the exact 549-key set and all baseline values against the trusted independent f8 Burkina #1123 inventory, then asserts exactly 26 changed Malawi API hashes, 523 exact others and 39 exact framework fingerprints. No source is fetched to compute a hash.

The safe receipts are `validation.json`, REPORT.md, VALIDATION.md and RESIDUALS.md. Full frames, identities, warnings and logs remain private. Root handles the separate organic composition proof and external issue/PR update after independent review.

Sue (GPT-6).
