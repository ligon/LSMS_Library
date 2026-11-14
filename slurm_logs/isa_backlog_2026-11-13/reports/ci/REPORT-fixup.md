The red-team fix-ups are committed in both worktrees and published to the mirror. In job 41276208, every affected test passes, no cache hash moves, and `mkdocs --strict` passes. No jobs are left running and both worktrees are clean.

## New heads
- **A** `ci/1716-windows-pin-20261113`: `38a3b0865f20789801c08d655dde539a3647a40b`. Its fix-up commit sits on top of `68bb94a2c` and `72885852c`.
- **B** `ci/1714-edited-trigger-20261113`: `629a7cb9f96aabcdac128ad968523ca4ad908577`. Its fix-up commit sits on top of `41f1a44a1`.

## What changed
- **A F1:** `--public-pins` now prints the expected values (public in `ci.yml`), the actual row count, and `digest match` or `digest mismatch`. The actual digest is compared inside `_pin_results` and dropped there, so no caller can print it. The docstring explains why the digest is additive, and ledger 1294 has an addendum.
- **A F2:** a test pins `--public-pins` to exactly one use across both workflows, on the `windows-script-build` step whose child is `windows_build_check.py`.
- **A F4:** I re-measured the page's counts myself:
  - Through `f138a3d61`: 2,611 commits and 541 merges. The published 2,447 and 527 are the counts through `e5fdd2fa8`.
  - The 355 PRs reproduce by ancestry, and all 355 merge commits have two parents, so none is a squash merge.
  - 41 of their messages lack "Merge pull request", and 14 of those also lack "Merge PR". Testing subjects or full messages gives the same numbers.
  - Through `f138a3d61` the same test finds 357 PRs, adding #1711 and #1604.
  - The page marks both corrections against the published Release body.
- **B F1:** the workflow comment now names two cases: no earlier success on the head fails closed; an earlier validation of the same head is reused. The PR body says the same.
- **B F2:** the test now says honestly that it pins the workflow's written statement, not the ruleset.
- **B ledger:** 932 has an addendum recording that metadata mode was removed under #1714 option (a).

## Verification (job 41276208)
- Tests, same six files as before:

| tree | passed |
|---|---|
| base | 164 |
| A | 196 |
| B | 157 |
| A and B combined | 189 |

- The flaky `test_real_wrapper_optin_…` passed in this run.
- `mkdocs build --strict` exits 0 on A and on base.
- Hash census: 0 moved for base to A and base to B, across 42 btf, 38 fwimp, 555 table and 2,321 wave hashes, with no errors.
- End to end on Linux, the real build under A's wrapper, as the Windows step runs it:
  - With the new pins: `actual rows=14459 digest match` and `actual rows=116808 digest match`, exit 0.
  - With the old pins: `actual rows=14459 digest mismatch`, exit 1. The actual digest `aac42079f006` does not appear.
  - Stderr was 0 bytes in both runs, and no in-tree parquets were left.

## Drafted residuals
- **Title:** "ci: execute the inline `__main__` blocks of pr-data-validation.yml's event and result gates in tests". **Tier:** B (CI).
  **Body:**
  - `tests/test_gh932_pr_data_validation.py` execs only the functions between the BEGIN/END markers, so the `if __name__ == '__main__'` blocks are never tested.
  - Red-team mutants B7 (the guard swallows its error and exits 0) and B8 (the classifier exits 0 without writing outputs) both pass the suite.
  - After #1714, B7 would still fail the job only incidentally: `pr_data_checkout.py`'s SHA check rejects the empty outputs before the credentials step.
  - Fix: run each inline script as a subprocess with a synthetic `GITHUB_EVENT_PATH`, `GITHUB_OUTPUT` and environment, and assert its exit status and written outputs. This predates #1714.
- **Title:** "tests: `test_gh932_safe_ci_failure_locations::test_real_wrapper_optin_preserves_stream_privacy_cleanup_status_and_static_names` is intermittent on --shared clones under xdist". **Tier:** B (CI); the PR data gate runs this file.
  **Body:**
  - It failed in all 6 per-SHA runs of red-team jobs 41275398 and 41275655, on base, A and B alike, with `junit unmapped failures=1` where 0 was expected: the checked-in locator was not resolved.
  - It passed in both merge-tree runs, in red-team jobs 41275665 and 41275929, and in my jobs 41274206 and 41276208.
  - Cause not established. One hypothesis: `private_ci_check._git` runs with `timeout=2`, and a slow `git ls-tree` or `cat-file` against shared alternates on network storage would leave the name count-only. An instrumented run saw only `rc=0` git calls, so that remains a hypothesis.
  - Next step: record timing and return code for each `_git` call in a failing run.

---

### PR_BODY A
**ci: re-pin windows-script-build at Uganda shocks 14,459; print pin rows and a digest verdict; record v0.17.0 (#1716)**

**Pin (#1716 items 1–2).** `windows-script-build` has been red on every development push since #1478, which deliberately kept two valid Uganda 2015-16 shock rows (#1434): 14,457 became 14,459. I re-measured with `scripts/windows_build_check.py` on Linux, cold, in separate empty data roots at `f138a3d61` (v0.17.0): four builds in two jobs, plus two more by the independent red team. All six agree:
- `Uganda:shocks rows=14459 digest=aac42079f006`
- `Uganda:assets rows=116808 digest=9202ed542a14` (unchanged)

As a control, the same method at `cbcf85f8f`, the last green Windows push, reproduces the old pin exactly (`14457:0ab2ea4c79dd`). The red team found that the only per-row difference between the two trees is the 2 added 2015-16 rows. The comment and `EXPECT` in `ci.yml` are updated.

**The pin holds for the `f138a3d61` tree only.** The #1562 coping stream adds a read-time `HowCopedSet` column to every country's shocks, Uganda included. That changes the served columns of Uganda shocks, so the integrated tree will need a re-pin: re-measure on Linux, cold, at the integrated tree before dispatching the Windows job. Other streams in the same batch were not checked for an effect on Uganda shocks or assets.

**Diagnosability (#1716 item 4).** `scripts/private_ci_check.py` deliberately publishes no child output. Credentialed builds can print survey values in assertion text, warnings and exceptions (#1294), which is why a red run showed only `exit=1`. A new opt-in `--public-pins`, accepted only for the `windows-script-build` stage, adds one line per pinned table:
```
pin Uganda:shocks expected rows=14457 digest=0ab2ea4c79dd actual rows=14459 digest mismatch
```
- The expected values come from the workflow's own `EXPECT`, which is checked in and already public, and are parsed strictly. A malformed `EXPECT` prints only `pin unavailable`.
- From the child it publishes only the row count and a `match`/`mismatch` verdict on the digest, **never the digest itself**. The digest is a sum of per-row hashes under pandas' fixed, public key, so it is additive: actual minus the public expected digest is the hash-sum of exactly the rows that changed, and for a one-row change that is one survey row's hash (red team, measured). The digest is compared inside the parser and dropped, so a re-pin is always measured on Linux.
- Values are read only from a line that matches the `RESULT` format in full, exactly once per pinned table. Otherwise the line says `actual unavailable`. Lines are bounded in length and the log in size, and the exit status is unchanged.
- `.coder/ledger/1294-private-ci-output.md` records this narrow counts-plus-verdict extension and the reason.

Tests: 32 new.
- Line endings (CRLF), ambiguous, truncated, overlong and secret-bearing lines, and non-ASCII.
- A malformed `EXPECT`, and the stage restriction.
- That the actual digest is never printed and does not leave the parser.
- The `RESULT` format, pinned against `windows_build_check.py`.
- That the flag appears exactly once across both workflows, on the step whose child is `windows_build_check.py`.

An end-to-end Linux run of the real build under the wrapper printed `rows=14459 digest match` with the new pins and `rows=14459 digest mismatch` with the old ones, with no stderr and no actual digest in the output.

**Release history.** `docs/releases/v0.17.0.md` is the GitHub Release body plus the published state, which I checked read-only:
- Tag `v0.17.0`, tag object `169517a9068f38255d335fe93394513bf22b24cd`, on master `cf09ea6b4`, whose tree equals `f138a3d61`.
- The GitHub Release, created and published on 2026-11-13.
- `publish.yml` run 43440985001: `build` and `publish` both succeeded.
- PyPI 0.17.0 wheel and sdist, with the sha256 values PyPI reports. No archive-content verification is claimed.

Edits against the published Release body:
- **Two corrected counts, each marked on the page.** Through `f138a3d61` there are 2,611 commits and 541 merges; the published 2,447 and 527 are the counts through `e5fdd2fa8`. Of the 355 PRs (verified by ancestry, all with two-parent merge commits, none squashed), 41 merge messages lack "Merge pull request" and 14 of those also lack "Merge PR". This replaces the published "mostly 19 integration and squash merges". Through `f138a3d61` the same test counts 357 PRs, adding #1711 and #1604.
- Removed the preparer line, the brief's PR-count estimate, and pointers to `CLOSES.md`, `ALREADY_CLOSED.tsv` and `CHECKLIST.md`, which are not in the repository.
- Replaced "wiring is in the pending batch" with where that wiring landed (#1604, issue #1697), and noted that Guinea-Bissau 2021-22 is still unwired.
- Dropped "the current `master`", and put the CI note in the past tense.

The page has no household identifiers or local paths. `mkdocs.yml` gains the v0.17.0 nav entry; v0.12.0 to v0.16.0 have pages but no nav entries (unchanged here).

**Verification (job 41276208).**
- 196 tests passed in the affected files (base: 164). The tree combined with the #1714 branch passed 189.
- `mkdocs build --strict` passes.
- No cache hash moves: 0 of 42 btf, 38 fwimp, 555 table and 2,321 wave hashes.

**Not verified: anything on native Windows**, including the pin, the new tests and CRLF translation. `windows-script-build` never runs on pull requests. Confirm with `gh workflow run ci.yml --ref ci/1716-windows-pin-20261113 -f run_windows_script_build=true`; that dispatch also runs `data-tests`. If the log shows `digest mismatch` with matching rows on this tree, that is a Windows/Linux divergence to investigate, not a value to re-pin.

Addresses #1716. Items 1, 2 and 4 are done here. Item 3 needs a green dispatched Windows run, and a re-pin if the coping change lands first.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

### PR_BODY B
**ci: a PR title/body edit no longer starts PR data validation (#1714)**

**Ruling (2026-11-13, #1714):** "(a) Stop edited triggers". A PR title or body edit must create no new run of the PR data validation workflow. A base change must still force re-validation, or fail closed.

**Cause.** On #1711 a body edit after the data gate started a second run of the workflow on the same head. That run's job was named `pr-data-metadata-edit`. GitHub then showed the required `pr-data-validation` as "Expected" and blocked the merge, even though a passing run existed on that head; the newest run of a workflow on a commit appears to replace older ones (observed, not documented). A run whose jobs all skip is still a run, so the fix has to be at the trigger.

**Change.**
- `edited` is removed from `types`.
- Metadata mode goes with it:
  - The classifier refuses `edited` as an unsupported action, which produces a failing required check if the trigger is ever re-added.
  - The guard accepts only `validate`.
  - The job has one static name, `pr-data-validation`.
  - The concurrency group loses its metadata branch.
- The per-step `run_data` conditions existed only for metadata mode and are removed. A guard failure now stops every later step, so a green required check can never come from skipped data steps.
- `if: always()` stays, because GitHub counts a job skipped by a conditional as a passing required check.
- `.coder/ledger/932-ready-pr-data-validation.md` gains an addendum recording the removal.

**What a base change does now.** Per GitHub's webhook docs a base change emits only `edited`, so a retarget to development without a push starts no run. There are two cases.
- **Fails closed.** If the head has no successful `pr-data-validation` (for example, a PR first opened against another branch), the required check stays "Expected". The merge is blocked until a push, a close/reopen, or draft → ready for review starts a run.
- **Reuses an earlier validation.** This case neither re-validates nor fails closed. If the head was already validated against development, then retargeted away and back with no push, that earlier success still satisfies the check. It is safe because the validated tree is the tree that merges:
  - The classifier only ever validates a development base.
  - Ruleset 25124452 sets `strict_required_status_checks_policy: true`, so the merge requires development's tip to be in the head's history.
  - Ruleset 9389899 forbids non-fast-forward pushes to development, with no bypass, so the base that run validated is an ancestor of that tip.
  - Both merges therefore have the head's tree. If development moves without a head update, the merge is blocked as "out of date", and updating the branch is a push, which re-validates.

  This case depends on the strict setting (read from the rulesets API on 2026-11-13). If that setting is ever cleared, a retarget would reuse a validation against an older development, as the other required check, `unit-tests`, always has. The workflow comment states both cases.

Not tested live: that a base change emits only `edited`, that a newer run on a head replaces an older one, and that a body edit now starts no run.

**Alternative considered, not built.** A second, `edited`-triggered workflow that re-validates only on a base change. To affect the required check it would have to emit a job named `pr-data-validation`. On a title or body edit that job would be skipped by its condition, and a skipped job counts as a passing required check, which fails open. Its precedence over this workflow's own check run is also undocumented. It is neither small nor safe.

**Tests.** `tests/test_gh932_pr_data_validation.py`:
- Every `edited` payload, well-formed or not, is refused.
- `edited` is absent from `types`.
- Every result renders the required job name.
- `metadata` is refused as a mode.
- No step after the guard is conditional.
- One test pins only the workflow's written statement that the reuse case relies on the strict setting. It cannot check the ruleset itself, which lives in GitHub settings.

**Verification (job 41276208).** 157 tests passed in the affected files (base: 164; the edited-mode tests were replaced). The tree combined with the #1716 branch passed 189. No cache hash moves: 0 of 42 btf, 38 fwimp, 555 table and 2,321 wave hashes.

**Known gaps, not fixed here.** Both predate this PR and are drafted as residuals:
- The inline `__main__` blocks of the two gates are untested.
- `test_gh932_safe_ci_failure_locations::test_real_wrapper_optin_…` is intermittent on `--shared` clones under xdist.

**To confirm on GitHub.** Edit this PR's body after its gate starts, then check that no new `PR data validation` run appears and the check is not "Expected".

Closes #1714.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
