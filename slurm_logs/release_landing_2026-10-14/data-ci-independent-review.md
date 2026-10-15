SCOPE DEVIATIONS

None. Read-only independent review; no source, Git, GitHub, dependency, cache or survey mutations. Pytest help only, private config/data roots and pinned BLAS/build workers, CPUs 8–15.

Verdict: APPROVE fix/data-ci-execution-profile at 3d6b8218fbdb7e4f303afaa13218adcb40eda567, based on 47ce939afa1f253a5a76e0832620a3cbb6a4b817.

Independently parsed both complete workflows with PyYAML BaseLoader, preserving the `on` key. Removing only the new data-tests environment map and restoring its prior Run full test suite command yields an identical parsed workflow. Four job-level string pins are exactly LSMS_BUILD_WORKERS=1, OPENBLAS_NUM_THREADS=1, OMP_NUM_THREADS=1 and MKL_NUM_THREADS=1, so build and pytest steps inherit them.

Command is exactly `poetry run pytest --tb=short --timeout=600 --timeout-method=thread --durations=25`. No xdist, selection filter, marker/skip change, retry, continue-on-error or exit suppression was added. The prerequisite `make -j2` and its existing 30-minute step limit remain unchanged; the new worker pin does not claim to serialize independent make recipes. Production package, tests, root conftest and dependency/lock files have zero diff.

pytest-timeout is already declared as ^2.3.0 in the nonoptional test dependency group, locked and locally installed at 2.4.0. Existing CI's same Poetry install recipe demonstrably installed/loaded 2.4.0 (job 116205699698). Independent help invocation accepted every exact new flag and returned 0 with asserted worktree package/config identities. No test suite ran for this review.

The 600-second thread timeout preserves failures as nonzero outcomes and can terminate pytest without a normal summary. This change supplies execution controls and diagnostics, not evidence that any old/current job deadlocked or that #941's cause was identified. A subsequent controlled credentialed run remains separate evidence.

Validation JSON and pytest help output are adjacent. Worktree remained clean; diff whitespace check passed.
