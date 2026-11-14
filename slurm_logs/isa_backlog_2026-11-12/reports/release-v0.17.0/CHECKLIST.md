# v0.17.0 release checklist

Adapted from `.claude/skills/release/SKILL.md` and the v0.16.0 flow (#1167 and #1168).
Prepared 2026-11-12 (Claude Opus 5.5, read-only).

- `master` = `v0.16.0` = `097283459f80016b0f00a51b814a0cb964a42789`.
- `development` = `e5fdd2fa831c4d03a247d264be7f339da459e6b0` before the batch.
- The default branch is **`master`** (`gh repo view --json defaultBranchRef`), so `Closes`
  lines in a PR into `master` do fire.

## 0. Gate: the batch lands first

- [ ] All six 2026-11-12 batch branches are merged to `development`:
  - Malawi 2024-25;
  - EHCVM-2 Togo, Benin and Côte d'Ivoire 2021-22;
  - Nigeria post-planting `community_prices`;
  - kg-factor evidence #1509 + #1068/#1129;
  - sold kg #1560/#1571;
  - served-wave corrections #1707/#1708/#1536/#1519.
- [ ] Record the new development SHA; every step below uses it in place of `e5fdd2fa8`.

## 1. Fill the release notes

- [ ] Fill `RELEASE_NOTES.md` § "Batch of 2026-11-12": PR numbers, served-number moves
      (countries, tables, sizes from each PR body), and cache cost.
- [ ] Re-run the PR census for anything merged after `e5fdd2fa8`. Ancestry-check each merge
      commit (`git merge-base --is-ancestor <sha> <dev-sha>`) and add the PRs to the notes.
- [ ] Copy the notes to `docs/releases/v0.17.0.md` on `development`:
  - drop the DRAFT header;
  - state the reviewed development SHA and the test evidence, as v0.16.0 did.
- [ ] Decide whether a `docs/migration/v0.17.0.md` is needed; v0.16.0 had one. Candidates:
  - `median_price_valuation` now defaults to within-wave (#1399);
  - `market=` keeps unlabelled households with `m` NA (#1424);
  - calls that served 0 rows now raise (#1589, #1599, #1300, #1303, #1636);
  - EHCVM `u` is now `<unit> (<Size>)` (#1458);
  - the new `period=` and `area_t=` kwargs;
  - the new levels `production_obs`, `input_obs` and `condition`.
- [ ] Note: #1693 (the current development tip) says in its PR body that its cold/warm/Feature
      data acceptance was "pending". Confirm that was completed (commit `99f9898e0`, "Refresh
      reviewed strata PR checks on development", may be it), or say so in the notes.

## 2. Closing keywords (`CLOSES.md`)

- [ ] At `e5fdd2fa8` there are **0 CLOSE verdicts**. Every touched open issue states remaining
      work, and the 191 resolved issues are already closed by hand.
- [ ] After the batch, grade the 14 excluded issues (`CLOSES.md`, "Excluded") from their
      *current* text, and grade any other issue the batch's PRs reference:
      1275, 1604, 1697, 552, 587, 1509, 1068, 1129, 1560, 1571, 1707, 1708, 1536, 1519.
  - Add `Closes #N`, **one keyword per issue, one per line**. A comma list closes only the first
    issue (v0.11.0, #807).
  - Close an issue only if its whole current body is done. Most are umbrellas narrowed in place.
- [ ] Re-run the commit-message hazard check. GitHub closes on commit-message keywords that reach
      the default branch, and does not parse negation:

  ```sh
  git -C /local/job41201840/mirrors/LSMS_Library log --format='%H%n%B' origin/master..<dev-sha> \
    | perl -ne 'while(/\b(close[sd]?|fix(?:e[sd])?|resolve[sd]?):?\s+#(\d+)/gi){print "$2\n"}' \
    | sort -un
  ```

  Every number printed is closed by the master merge. At `e5fdd2fa8`, all 59 hits were already
  closed.
- [ ] Before submitting the PR, confirm GitHub parses exactly the intended set:
      `gh pr view <N> -R ligon/LSMS_Library --json closingIssuesReferences --jq '[.closingIssuesReferences[].number]'`.

## 3. Coverage snapshot: decide

- [ ] `.coder/coverage/latest.csv` was last committed **2026-09-18** (`12c0dec2b`). That is 3,252
      commits before `e5fdd2fa8`, and it misgrades known cells (#1438, open). **Choose one:**
  - **(a) Regrade on a burst node** before the release PR: `make matrix`, run with
    `bench/matrix.py` in a private data root. It is a cold build per cell, so budget it as a
    corpus-wide re-warm, because this release already moves every fingerprint.
    - Pin BLAS: `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1`.
    - Budget against the node's `uptime` load, not `nproc`. n0035 showed load ~19-24 on
      2026-11-12.
    - Commit the snapshot to `development` through a PR. Do **not** touch `blessed.csv`, since
      blessings are human-only.
  - **(b) Ship the stale snapshot and say so** in the release notes: "`ll.coverage()` reads a
    snapshot from 2026-09-18 that predates this release; see #1438." Leave #1438 open.

## 4. Tests (compute node, never the login node)

- [ ] On the release candidate (`development` after the batch), run the suite the way the
      Makefile does, writing output to a file and never through a pipe:

  ```sh
  OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 \
    PYTHONPATH=<checkout> .venv/bin/python -m pytest -n <N> --dist=loadfile > pytest_v0170.log 2>&1
  ```

  - Assert the import identity first (the `PYTHONPATH` rule in `CLAUDE.md`).
  - Consider `--rebuild-caches` (`make test-full`), because every fingerprint moves.
- [ ] GitHub CI on the candidate is green: unit, Windows smoke, and the new required PR
      data-validation check (#1680).
- [ ] `tests/test_version_placeholder.py` passes. `[tool.poetry] version = "0.0.0"` stays in
      the right section.

## 5. Merge `development` → `master`

- [ ] Open the PR with base `master` and head `development`, titled "Release v0.17.0". Use the
      body from `MERGE_PR_BODY.md`, with the `Closes` block filled and the HTML comments removed.
- [ ] **Merge with a merge commit. Never "Squash and merge"** (#807/#814/#815).
- [ ] Verify which issues closed, against the intended list:
      `gh issue list -R ligon/LSMS_Library --state closed --search "closed:>=<merge-time>"`.
      Re-open anything closed by mistake.

## 6. Tag and publish

- [ ] On `master` at the merge commit: `make release v=0.17.0`. It refuses an existing tag, runs
      pytest, then creates the annotated tag; there is no local build.
  - If the system poetry is broken (`packaging.licenses`), run pytest via `.venv/bin/python -m
    pytest` and tag by hand: `git tag -a v0.17.0 -m "Release 0.17.0"`.
- [ ] `git push origin v0.17.0`.
- [ ] `gh release create v0.17.0 --title v0.17.0 --notes-file docs/releases/v0.17.0.md`.
- [ ] **Publish-event gotcha.** The `release: published` trigger is often suppressed for a
      release created or published through a token (the `gh` CLI), an old or recreated draft,
      or a tag that predates the workflow.
  - Prefer the web-UI **"Publish release"** button.
  - Then verify: `gh run list -R ligon/LSMS_Library --workflow=publish.yml` must show a
    `release` run.
  - If it shows nothing, use the `workflow_dispatch` fallback, which builds from the same tag:

    ```sh
    gh workflow run publish.yml -R ligon/LSMS_Library --ref master -f tag=v0.17.0
    gh run watch $(gh run list -R ligon/LSMS_Library --workflow=publish.yml -L1 --json databaseId -q '.[0].databaseId')
    ```

- [ ] Verify the artifact:
  - <https://pypi.org/project/LSMS-Library/0.17.0/> exists;
  - the wheel is `lsms_library-0.17.0-*.whl`, not `0.0.0` (the workflow guard fails on `0.0.0`);
  - optionally, compare the PyPI archives with the tagged tree, as v0.16.0 did.

## 7. After publishing

- [ ] Record the verified publication on `development`, as #1169 did for v0.16.0: the publish
      run URL, PyPI digests and the final test counts in `docs/releases/v0.17.0.md`.
- [ ] Announce the corpus-wide cold re-warm (L2-country and L2-wave). Note the Ethiopia
      stale-root caveat from #1558: one `household_roster()` read heals the default backend,
      and the make backend needs `lsms-library cache clear --country Ethiopia`.
- [ ] Merge `master` back into `development` only if a hotfix landed on `master`. A
      merge-commit release keeps them aligned.
