## Verdicts

- **Candidate A** `72885852cfc9ab765759c044c6b23489f94f3d7a` (#1716), base `f138a3d61c203f892148592675a7ef72e160a322`: **CHANGES**. The new pin is right, and the wrapper parser held up under every attack I tried. One thing needs the user's ruling or a small edit: A now publishes the actual digest of a credentialed build.
- **Candidate B** `41f1a44a170cda31a523d7fddb4b7b8ec3c5a6f7` (#1714), same base: **APPROVE**, with must-state wording in the PR body.

## A: findings

**F1 (should-fix, privacy contract). `--public-pins` publishes the actual 48-bit digest. That digest is survey-derived, not a count.**
- **The digest is additive.** It is a sum of per-row hashes mod 2^48, and pandas' hash key is fixed and public. The expected digest is already public in `ci.yml`, so actual minus expected is the hash-sum of only the rows that changed. For a one-row change, that is one survey row's 48-bit hash.
- **Measured both ways:**
  - Synthetic data (job 41275398): `digest(T+r) - digest(T) == digest(r)` is True.
  - The real #1478 change (job 41275396): base minus control equals the hash-sum of the two added 2015-16 rows, True.
- **Practical risk is low:** the space of candidate rows is large and the values are not very sensitive. But the docstring's "Neither is a survey value" understates it.
- **Context:** the RESULT line was public until #1294 (`513ebb5c7`, 2026-10-24, which wrapped a step that ran unwrapped). So A brings back an old disclosure rather than creating a new one.
- **Authority:** the approved contract (ledger `.coder/ledger/1294-private-ci-output.md` §1/§3) is counts only. No ruling widens it; the printing comes from item 4 of #1716 ("Consider…").
- **Fix, either of:**
  - (a) Print actual rows plus a digest `match`/`mismatch` verdict, not the actual digest. A re-pin has to come from a Linux measurement anyway, as the worker says.
  - (b) Get the user's explicit approval and add an addendum to ledger 1294.
- The pin and the printing share commit `68bb94a2c`, so either fix is a small edit, not a cherry-pick.

**F2 (should-fix, small). Nothing pins `--public-pins` to the one Windows step.** The stage is a label the caller supplies. B's tests count `--public-test-failures` across both workflows; there is no equivalent here. Add a test that `--public-pins` appears exactly once across `ci.yml` and `pr-data-validation.yml`, on the step whose child is `windows_build_check.py`. Misuse would still leak only rows and a digest.

**F3 (must-state). The pin holds only for the `f138a3d61` tree.**
- The coping stream (GH1562, `reports/coping/BRIEF.md:17-31`) adds `HowCopedSet` at read time to every country, Uganda included. That changes the served columns of Uganda shocks, so the digest will change.
- Other streams (manquant, strata, cropkg, benin) had no report when I looked, so I can't rule them out.
- Re-measure at the integrated tree before the dispatch.
- Nothing ran on native Windows. #1716 must stay open until a dispatched `windows-script-build` is green.

**F4 (note, docs). Two counts on the v0.17.0 page are off.** Both lines are on the page; the first comes unchanged from the published Release body.
- "2,447 commits, 527 of them merge commits" sits under "through development f138a3d61". It is actually `v0.16.0..e5fdd2fa8`. Through f138a3d61 the counts are 2,611 and 541.
- "The count includes 19 integration and squash merges whose commits do not say 'Merge pull request'": I count 41 of the 355 merge commits without that phrase, and 14 without "Merge PR" either. The worker's rewording turned the published "the gap is mostly 19" into this exact-sounding line.
- The 355 count reproduces by ancestry.

**F5 (note).** The 1294 and 932 ledgers are now stale, as the worker flagged. Both deserve an addendum in the PR that changes their contract.

**Verified, no finding:**
- **Published state:**
  - The tag object `169517a9068f…` points at `cf09ea6b4`.
  - The Release was published at 14:15:18Z.
  - Run 43440985001: `build` and `publish` both succeeded on `cf09ea6b4`.
  - The PyPI sha256 values for both files match.
  - The `cf09ea6b4` tree equals the `f138a3d61` tree.
- **Page content:** no household ids, local paths or private report pointers. I checked the page's diff against the published Release body.
- **mkdocs:** `mkdocs build --strict` exits 0 on A and on base (mkdocs 1.6.1, material 9.8.1, unpinned). The v0.17.0 page is built and in the nav; v0.12 to v0.16 and `migration/v0.16.0.md` get only INFO-level "not in nav" notices.

## A: re-measurement (cold, private empty roots)

| job | tree | shocks | assets |
|---|---|---|---|
| 41275396, run 1 | f138a3d61 | 14459 / `aac42079f006` | 116808 / `9202ed542a14` |
| 41275396, run 2 (second root) | f138a3d61 | 14459 / `aac42079f006` | 116808 / `9202ed542a14` |
| 41275396, control | cbcf85f8f | 14457 / `0ab2ea4c79dd` | 116808 / `9202ed542a14` |

- **What changed between control and base:** comparing per-row hashes, the only difference is +2 rows, both in 2015-16. Nothing was removed and assets are identical.
- **The real child under A's wrapper** (job 41275397):
  - New pins, cold: `match` on both tables.
  - Old pins: `actual rows=14459 digest=aac42079f006 mismatch`.
  - `EXPECT` unset: `pin unavailable`.
  - `--stage data-tests --public-pins`: exit 125, stage line only.
  - Stderr is 0 bytes in every case, and the pin lines agree with the unwrapped child's RESULT lines.
- **Mutation checks:** all 7 wrapper mutants were caught by A's tests (stage restriction removed, ambiguous lines accepted, `search` instead of `fullmatch`, unterminated lines kept, loose digest, loose `EXPECT`, printing on an unusable `EXPECT`).
- **Windows-like settings on Linux:** `tests/test_private_ci_check.py` passes 60/60 under `PYTHONUNBUFFERED=1`, plus `PYTHONIOENCODING=cp1252`, plus `PYTHONUTF8=0`.

## B: findings

**F1 (must-state; needs the user's confirmation, not a defect). Retargeting away and back reuses an earlier validation.**
- In that case B neither re-validates nor fails closed, which the ruling's wording does not literally cover. The PR body files it under "fails closed".
- **Why it is safe, from the rulesets API on 2026-11-13:**
  - Ruleset 25124452 has `strict_required_status_checks_policy: true`, requires `unit-tests` and `pr-data-validation` (integration 15368), and lets admins bypass always.
  - Ruleset 9389899 has `non_fast_forward` on `development` with no bypass.
  - So the base that was validated is an ancestor of the head, and the validated tree equals the tree that merges.
- **Recovery** from "Expected" needs a push, a close and reopen, or draft then ready for review.
- **The docs agree with the premise.** `edited` covers "…or the base branch of a pull request was changed"; `synchronize` means "head branch was updated"; "A job that is skipped will report its status as Success … even if it is a required check". None of this was tested live.

**F2 (note).** `assert 'strict_required_status_checks_policy: true' in WORKFLOW.read_text()` pins a comment, not the setting.

**F3 (note, also true at base).** The inline `__main__` blocks are untested. Mutants B7 (the guard swallows its error) and B8 (the classifier exits 0) both pass the test suite.
- From reading `.github/scripts/pr_data_checkout.py`: with empty classifier outputs, its SHA regex raises before the credentials step, so B still fails the job.
- My reasoning, not run: base's per-step `run_data` conditions would have skipped every step, turning B7 green, so B is safer on this point.

**F4 (note).** The five edited-event faults in `test_ineligible_and_uncertain_events_refuse_before_any_checkout` now all fail for the same reason, so they are redundant.

**Behaviour on a validate run is unchanged** (reasoning): an `if` without a status function implies `success()`. I did not re-check that against the docs. Mutants B1-B6, B9 and B10 were all caught (re-adding `edited`, dropping `always()`, a conditional step, the classifier accepting `edited`, the guard accepting `metadata`, a dynamic job name, any base, drafts allowed).

## Tests and census (both candidates)
- **The two named files** (`test_gh932_pr_data_validation.py`, `test_private_ci_check.py`) were green on base, A, B and the A+B merge tree in every run. `git merge-tree` is clean, tree `c23e9858b`.
- **Six-file totals** (job 41275655) match the worker's 164/193/157/186. But on base, A and B alike one test failed: `test_gh932_safe_ci_failure_locations.py::test_real_wrapper_optin_…` failed in all 6 per-candidate runs of the two jobs that cloned three SHAs (41275398, 41275655).
  - It passed in both merge-tree runs and in every run of 41275665 and 41275929.
  - The cause is not established; an instrumented run saw only `rc=0` git calls.
  - It does not block either candidate, because base fails identically. The worker's "every test run passed" did not reproduce for me.
  - Draft residual for you: "test_real_wrapper_optin_… is intermittent on --shared clones under xdist" (Tier B, CI). The PR data gate runs this file.
- **Hash census:** 0 moved for base→A and base→B (0/42 btf, 0/38 fwimp, 0/555 table, 0/2,321 wave hashes, 0 errors). No in-tree parquets.

## The worker's questions
- **Nav gap:** confirmed. It is harmless under `--strict`; the Tier C residual is fine.
- **Ledgers:** agree they are stale. The 1294 one matters because of F1.
- **Issue status:** #1714 closes with B, but the body-edit check still has to be confirmed live. #1716 stays open until Windows is green, and it needs a re-pin if coping lands.

## Limits and my own slips
- **Not tested:**
  - Nothing ran on native Windows: the pin, the 29 new tests, CRLF translation.
  - No live GitHub behaviour: that a base change emits only `edited`, that the newest run wins, that a body edit starts no run.
- **Other limits:**
  - On the release page I checked the published-state block, its diff against the Release body, the scope counts (F4) and the GNB wiring claim, not every sentence of its prose.
  - The three Windows-settings runs overwrote each other's output file. Their results are in `logs/rt-ci-six-41275655.out`.
- **My own slips:**
  - I ran system `python3` once on n0035 to parse PyPI's JSON. It imported nothing from the library.
  - I ran `git merge-tree --write-tree` in `/local/job41201840/worktrees/ci-1714`. That wrote unreferenced objects only; no refs, index or working tree changed.
  - All mutations were made only in job-local throwaway clones.

Scripts, logs and outputs are in `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/ci-redteam/` (under `scripts/`, `logs/` and `out/`).
