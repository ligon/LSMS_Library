VERDICT: pass -- every before/after number in the report and commits reproduces in my private root (0 of 4,617 per-test outcomes change credential-free, credentialed collection byte-identical, 56-file wall 1016 s -> 131 s), the diff is marks-only plus two CI gates, and the residuals are drafted.

Reviewer: red-team, Slurm job 39923207, own clone of `fix/ci-skips` @ 77cb428e6, own venv, private `LSMS_DATA_DIR`. Baseline = merge-base `2ed85b49b` in a worktree. Credential-free environment reproduced the fix agent's: in-tree `.dvc/s3_creds` moved aside, `LSMS_SKIP_AUTH=1`, `LSMS_CONFIG_DIR` -> empty dir, `AWS_SHARED_CREDENTIALS_FILE=/nonexistent`, `AWS_CONFIG_FILE=/nonexistent` (`~/.aws/credentials` EXISTS on this node, so the override matters), fresh empty `LSMS_DATA_DIR` per run, BLAS pinned, `LSMS_BUILD_WORKERS=1`, `-n 24 --dist=loadfile`. Probe before starting: `aws_creds_available()` False; `Country('Kosovo').housing()` raised `NoCredentialsError` after 56.8 s and `_mentions_missing_credentials` matched it. GitNexus unavailable; `rg`/grep substitutes used throughout.

## 1. Refutations (claim -> what I ran -> result)

- **"56 files: 660 passed, 1248 skipped; 825 s -> 69 s"** -- HELD. `run_cf.sh <checkout> <label> 24 <56 files>` on base and branch, `--junitxml`. base: `660 passed, 1248 skipped, 55 warnings in 1016.53s`, WALL 1024 s. branch: `660 passed, 1248 skipped, 26 warnings in 131.47s`, WALL 145 s. Per-test junit diff: 1,908 ids identical, **0 outcome changes**; summed per-test time 9,069 s -> 127 s.
- **"Full suite: 3017 passed, 1600 skipped; 837 s -> 92 s"** -- HELD. base: `3017 passed, 1600 skipped, 113 warnings in 961.03s`; branch: `3017 passed, 1600 skipped, 85 warnings in 138.53s`. 4,617 ids identical, **0 outcome changes**; summed per-test time 9,358 s -> 644 s.
- **"Only 18 of the 1,248 skips carried the net's own message"** -- HELD: 18 on base56 junit (19 on basefull; 1 on branchfull, the `test_cache_hash_invalidation.py` test the report names).
- **"UnmarkedS3TestWarning count after: 0 on the 56 files"** -- HELD: 0 in branch56.log; exactly 1 in branchfull.log, naming `tests/test_cache_hash_invalidation.py::test_hashless_country_parquet_is_rebuilt_not_trusted`.
- **"Warning survives `-W error::UserWarning`"** -- HELD. `tests/test_cache_hash_invalidation.py` credential-free, `-n 2`: plain `21 passed, 1 skipped, 1 warning in 73.11s`; with `-W error::UserWarning` `21 passed, 1 skipped, 1 warning in 68.70s`; the UnmarkedS3TestWarning line present in both.
- **"The mark is a no-op when credentials are present"** -- HELD. `pytest --collect-only -q` with `AWS_ACCESS_KEY_ID/SECRET` set (fake, `LSMS_SKIP_AUTH=1`): 4,617 ids on both checkouts, `diff` empty. `pytest_collection_modifyitems` returns early when creds are available; nothing else in the diff touches collection. Also RUN, not only reasoned: with the in-tree `s3_creds` restored and `LSMS_SKIP_AUTH` unset, the module-marked `tests/test_guinea_bissau_cluster_features.py` against my private root built Guinea-Bissau `cluster_features` + `sample` from the shared blobs and gave `5 passed in 7.55s` -- executed, not skipped. CI's `data-tests` step writes `s3_creds` with the literal `aws_access_key_id =` line the helper greps for (`ci.yml:324`), so the marks are inert on that job by the file route as well as the env-var route.
- **"56 files = the issue's list; marks only"** -- HELD. The 56 touched test files equal the issue's table exactly (comm: empty both ways). Removed lines are the four `pytestmark = ...` statements re-expressed as lists (skipif/_aws_creds_available, slow, filterwarnings, REPL-dir skipif) -- all re-added inside the list, nothing dropped. No mark landed on a fixture (0 "Marks applied to fixtures" warnings; grep for `requires_s3` followed by `@pytest.fixture` empty). 118 function marks, 9 class marks, 17 module-level `pytestmark` carrying `requires_s3` (counted with `ast`; 5 of them lists that keep a pre-existing mark) -- exactly the report's figures.
- **"PR #973 already merged, no sequencing"** -- HELD and superseded: development has moved 16 commits since the merge-base; a trial merge of the branch onto current `origin/development` (`61217ec98`) is clean (4 auto-merged test files, no conflicts). Credential-free run of those 4 files on the merged tree: `18 passed, 30 skipped, 22 warnings in 9.84s`, 0 UnmarkedS3TestWarning -- the new development tests in them are data-free.
- **"ci.yml: workflow_dispatch admitted by the data-tests gate; dvc-lock-check push-only; concurrency keys on event_name"** -- HELD by parsing: triggers `['push','pull_request','merge_group','schedule','workflow_dispatch']`; `data-tests.if` = `github.event_name == 'workflow_dispatch' || (push && (master || separate_data))`; `dvc-lock-check.if` = `push && master`; `refresh-dvc-locks` `needs: dvc-lock-check` so it is skipped on dispatch; `docs` push-only; `concurrency.group` includes `github.event_name` (line 41). `publish.yml` also carries `workflow_dispatch` but is a separate workflow; no interaction.
- **"Pre-existing xdist INTERNALERROR flake at -n 40"** -- CONFIRMED on the branch, plain `python -m pytest tests -n 40 --dist=loadfile --timeout=600 --timeout-method=thread` credential-free, no controller pre-import: `2 failed, 2215 passed, 1345 skipped in 35.28s`, 147 INTERNALERROR lines (`xdist/workermanage.py:474 unserialize_warning_message` -> `_frozen_importlib._DeadlockError: deadlock detected by _ModuleLock('lsms_library.country')`), gw8/gw9 down with `cannot import name 'get_option' from partially initialized module 'pandas._config'`. The 4 measurement runs above used the report's controller pre-import at -n 24 and had 0 such lines. I reproduced it on the branch only; the report says the development baseline crashed the same way, and I measured no relative frequency, so whether the faster run widens the window is conjecture. The deadlocking imports are lsms_library warning classes, not the conftest one. The controller died before a summary, so the two `F` markers (printed beside the gw8/gw9 `node down` lines) are presumed to be the items in flight on those workers; they are not attributed. Worth the issue the report drafts.
- **"Cache cost: none"** -- HELD. No file under `lsms_library/` touched; `git diff --stat` is `tests/` + `.github/workflows/ci.yml` only.
- **"Per-test net cost ~70-80 s, not the 43 s in the record"** -- HELD here: probe 56.8 s; base junit per-test setup times 60-100 s.

## 2. Claimed vs measured

| quantity | claimed | measured |
|---|---|---|
| 56 files, base | 660 passed / 1248 skipped / 825 s | 660 / 1248 / 1016 s |
| 56 files, branch | 660 / 1248 / 69 s | 660 / 1248 / 131 s |
| 56-file outcome changes | 0 of 1,908 | 0 of 1,908 |
| summed per-test time, 56 files | 7,094 s -> 68 s | 9,069 s -> 127 s |
| full suite, base | 3017 / 1600 / 837 s | 3017 / 1600 / 961 s |
| full suite, branch | 3017 / 1600 / 92 s | 3017 / 1600 / 139 s |
| full-suite outcome changes | 0 of 4,617 | 0 of 4,617 |
| skips via the net's own message (base, 56 files) | 18 | 18 |
| UnmarkedS3TestWarning after (56 files / full) | 0 / 1 | 0 / 1 (same nodeid) |
| `-W error::UserWarning` on unmarked file | 21 passed, 1 skipped, 1 warning | same, both modes |
| collected ids with creds | (implicit: unchanged) | 4,617 = 4,617, diff empty |
| `test_locality_deprecation` residual wait | ~50 s | 105 s / 101 s |

Absolute wall times are longer here (shared node, load 12-28, -n 24 not 40); the ratio (7.8x on the 56 files, 7.0x on the suite) is what the issue asked for.

## 3. Regression

- branch full, credential-free: `3017 passed, 1600 skipped, 85 warnings in 138.53s (0:02:18)` EXIT 0.
- base full, credential-free: `3017 passed, 1600 skipped, 113 warnings in 961.03s (0:16:01)` EXIT 0.
- branch 56 files: `660 passed, 1248 skipped, 26 warnings in 131.47s (0:02:11)` EXIT 0.
- base 56 files: `660 passed, 1248 skipped, 55 warnings in 1016.53s (0:16:56)` EXIT 0.
- No credentialed FULL suite was run (a cold credentialed suite in an empty private root is a corpus rebuild of many hours); the credentialed path is covered by the identical collection plus the one credentialed module-marked file above (`5 passed in 7.55s`), and the marker predicate short-circuits on `aws_creds_available()`. `tests/test_no_editable_install.py` (the ci.yml linter) passed in both full runs. No scan.py: no feature touched.
- Write hygiene: `find lsms_library/countries -name '*.parquet'` empty in clone, base and merge worktrees; every per-run `LSMS_DATA_DIR` has 0 files afterwards (credential-free runs built nothing); the review's shared private root was not written to.

## 4. Scope findings

- Diffstat vs merge-base: 58 files, 230+/13-; exactly the 56 named files + `tests/conftest.py` + `.github/workflows/ci.yml`. No baseline/golden/blessed/lockfile/dependency edit. No assertion weakened, skipped or xfailed: the only removed lines are the four module `pytestmark` assignments re-expressed as lists.
- #941 step 2 was correctly NOT attempted; the report says what it needs.
- New tests: none added. The new behaviour (the net's warning) is exercised only by the credential-free run itself; on base the warning class does not exist (0 occurrences in base logs), on the branch it fires once on the full suite. There is no unit test of `pytest_runtest_makereport` -- see follow-ups.
- CONTENTS.org: n/a (framework/CI); nothing in the branch claims anything about a country.
- Residual drafts: present for #907 (the `pytest.skip`-after-failed-build path, 1,230 of 1,248 skips were that shape, and the two named stragglers), for #941 (step 1 landed, step 2 remains), and a new xdist-race issue. All three match what I measured.

## 5. Residual risks / human decisions

1. **The warning is a `UserWarning` subclass.** Any future `filterwarnings = error` in `pyproject.toml` is handled (verified), but a test module with `pytestmark = pytest.mark.filterwarnings("error::UserWarning")` that hits the net would still be converted, not crashed -- also verified by the `-W` run. No decision needed; recorded so nobody re-tests it.
2. **The 1-line fix the report deliberately left out of scope**: `tests/test_cache_hash_invalidation.py::test_hashless_country_parquet_is_rebuilt_not_trusted` still pays ~100 s on every credential-free run and is now the only net conversion in the suite. Also two PASSING tests wait out the network (~100 s each): `test_locality_deprecation.py::test_locality_emits_deprecation_warning` and -- not in the report -- `test_gh323_site4_dfs_merge.py::test_missing_file_stays_soft_even_for_a_required_column`. Together they are ~300 s of the branch's 644 s summed test time. Human call whether to take them in this PR or the drafted residual.
3. **`workflow_dispatch` on `development` also runs `dvc-remote-reachability`** (its `if` admits the development ref on any event). Read-only sweep; cost only. Dispatch needs Actions write, so no fork exposure of the S3 secrets.
4. **The xdist warning-unserialisation deadlock is real on a 40-core node** and now reachable in ~35 s. CI's `-n auto` runs on 4-core runners where it has not been seen; the report's new-issue draft is the right place for it. Not a reason to hold this branch.

## 6. Recommended follow-up commits (optional; `pass`, not `pass-with-fixes`)

- `tests/test_cache_hash_invalidation.py`: `@pytest.mark.requires_s3` on `test_hashless_country_parquet_is_rebuilt_not_trusted` (saves ~100 s per credential-free run; it is the last net conversion).
- `tests/test_locality_deprecation.py` and `tests/test_gh323_site4_dfs_merge.py`: mark the two network-waiting passers, or make them not reach DVC (each ~100 s).
- `tests/test_conftest_net.py` (new): a `pytester`-based test that an unmarked data test under a credential-free environment is converted to a skip AND emits `UnmarkedS3TestWarning`, including under `-W error::UserWarning` -- the branch's one behavioural change currently has no test that would fail on development.
- Root `conftest.py`: pre-import `lsms_library` on the controller (the report's measured mitigation) as part of the drafted xdist-race issue, not this PR.
