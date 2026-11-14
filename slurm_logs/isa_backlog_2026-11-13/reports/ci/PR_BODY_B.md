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
