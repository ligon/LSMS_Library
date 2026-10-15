# #907 — whole-job credential-free coverage comparison

The requested whole-job before/after pair is identifiable. The missing acceptance evidence is now available; retaining #907 solely because this comparison was not recorded is no longer warranted. This does not close #941 or establish the cause of any credentialed failure.

| Revision / Actions run | Collected | Passed | Skipped | pytest time | Whole unit job |
|---|---:|---:|---:|---:|---:|
| Before markers: c45f8d02c / [38661469322](https://github.com/ligon/LSMS_Library/actions/runs/38661469322/job/116183682176) | 4,618 | 3,017 | 1,601 | 1,751.57 s | 1,792 s |
| After markers: 9291b79de / [38668460143](https://github.com/ligon/LSMS_Library/actions/runs/38668460143/job/116205699698) | 4,618 | 3,017 | 1,601 | 110.53 s | 141 s |
| Follow-up: 67d781318 / [38681156745](https://github.com/ligon/LSMS_Library/actions/runs/38681156745/job/116245799098) | 4,625 | 3,024 | 1,601 | 83.59 s | 116 s |
| Current development: 0f648c325 / [38683897786](https://github.com/ligon/LSMS_Library/actions/runs/38683897786/job/116255585415) | 7,653 | 3,246 | 4,407 | 129.03 s | 171 s |

The first two runs use two xdist workers and identical `unit-tests` job configurations, including credential-free LSMS_SKIP_AUTH, BLAS pins, `--dist=loadfile`, timeout and durations flags. Runtime package source, dependency declarations, lockfile and root conftest are identical. The existing `pytest_collection_modifyitems` hook is also AST-identical. The tests/conftest runtime warning addition is intentional and separately covered by the later hook tests.

All 56 changed test files are AST-identical after removing only `requires_s3` decorators/module marks and pytest imports; existing unrelated marks remain included in the comparison. There are no added/removed test definitions, parameter sets, assertions or bodies in that pair, and collection stays at 4,618. This controls the test-population issue that makes the September/current headline counts unsuitable. Pytest time falls by 93.69% (15.85 times faster) with exactly unchanged passed/skipped counts. Job elapsed time is reported separately from pytest time, excluding workflow queue delay.

The already archived independent red-team review at `slurm_logs/backlog_review_2026-10-14/verdicts/ci-skips.md` additionally reports zero outcome changes across 4,617 matched local full-suite node IDs and zero across the 1,908 targeted IDs, plus credentialed collection identity. Those are prior independently executed measurements, not newly rerun by this follow-up. The two Actions jobs' default xdist logs do not expose every node outcome, so this follow-up does not claim a new per-node Actions diff. The direct whole-job equality, unchanged test AST/population, and archived node-level comparison together address the mixed-file coverage criterion.

The follow-up adds seven hook-regression cases and retains all 1,601 skips; its increase from 3,017 to 3,024 passes is accounted for by those seven added cases, rather than being used as proof of unchanged older coverage. It also handles the three documented remaining network waiters through a precise mark and synthetic reader/locality isolation.

For context only, the original September run [34751527886](https://github.com/ligon/LSMS_Library/actions/runs/34751527886/job/103708790168) was 2,639 passed / 1,524 skipped / 4,163 collected, 7,053.67 pytest seconds, using one worker. It differs in source population and parallelism; comparing that run directly to today's 3,246 passes is not a no-coverage-loss proof. The October pair above avoids those confounders.

Likewise, integrated 74c9398ec's 3,274 passed / 4,405 skipped / two known failures is not the current-development job. Its test-source difference from 0f648c325 consists of the pending #967 Uganda sentinel expansion and the Nigeria raw-class credential correction; current development has no #967 runtime increment. The two Nigeria failures become collection-time skips. This report does not treat differing release-candidate/current totals as lost coverage or use them for #907 acceptance.

Evidence: `actions/{run_id}.json`, `{run_id}-unit.log`, `907-comparison.json`, and `marker-ast-comparison.json`. `fetch_actions.py` performs only read-only `gh run view`; `analyze_actions.py` reproduces source/config assertions and timing/count extraction. Attempted `gh run view --log` for still-running integrated 38683117467 was refused by gh and is recorded in its fetch JSON; its reported summary is contextual, not a newly downloaded log here.

No new Actions job, issue edit, comment, push, source change, test run or credential access was performed for this comparison. The #1003 fixture work is a separate authorized branch. All private raw logs are unit-job logs with GitHub masking, not credentialed-job payloads. Parent owns issue disposition/publication.
