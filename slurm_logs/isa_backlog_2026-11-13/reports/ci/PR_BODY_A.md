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

