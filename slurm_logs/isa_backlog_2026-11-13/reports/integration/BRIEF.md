# Stream: integration -- the 2026-11-13 ISA batch, one PR into development

Read `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/COMMON-RULES.md` first.

- **Worktree:** `/local/job41201840/worktrees/integration-1113`, branch
  `integration/isa-batch-20261113`, based on GitHub development `c79f543da`
  (= f138a3d61 plus #1718: the Windows pin 14459:aac42079f006, `--public-pins`,
  docs/releases/v0.17.0.md).
- **Report dir:** `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261113/reports/integration/`.
- **Exception to COMMON-RULES:** merge commits do not trigger the publish hook,
  so you MAY `git push origin integration/isa-batch-20261113` (origin is the
  shared mirror) after each merge. Never push to GitHub.
- **Rulings:** `git show docs/isa-sweep-20261103:.sucoder/isa-decisions.org`
  (every 2026-11-13 entry).
- **Stream records:** each stream's REPORT.md and PR_BODY.md, and the red-team
  VERDICT*.md files, are in `reports/<stream>/` and `reports/<stream>-redteam/`.

## Merge, in this order, with `git merge --no-ff` and full SHAs (each red-teamed, APPROVE)
1. strata   `79739f8162b706b72bbc54dffe739e855b154f80`
2. cropkg   `bda250217906c4303e79bfab23a71a5781168708`
3. malawi   `5093b49398424e953fe049cfe12b3f29eb991443`
4. burkina  `f962ef7c9b1cb6c4bc699a20aa5845e94abdc4de`
5. coping   `2ee44de32c1d13ad8f5c1b4ea13ac3c5b7e9e847`

Then WAIT for Sue's message before merging these three. Their final delta
reviews are running, and Sue will send you the approved SHAs:
6. manquant (currently `63aa569ca…`)
7. gnb      (currently `b7e65b83e…`)
8. nigeria  (currently `dc3db3dc7…`)

Benin is NOT in this PR; it gets its own PR.

**Conflict policy:**
- `CONTENTS.org` end-of-file appends: keep both sides, in merge order.
- `.coder/coverage/absent_verdicts.csv`: take the union.
- `tests/test_gh1500_*.py`: coping and manquant re-pin different functions.
- Anything semantic: stop and report.
- Record every conflict and how you resolved it.

## Integration tasks (after all eight are merged)
a. **GNB 2021-22 shocks (coping red team F5).**
   - Stamp option 25's inferred slot with the shared `ehcvm.derive_coping_autre_rank` plus a registered `Guinea-Bissau::shocks::code25-rank-inferred` key. Copy the Benin/Niger entries; it is expected on 3 rows.
   - Make sure the hook writes or keeps `HowCopedSet`.
   - Remove the `optional: true` on GNB `HowCoped0..2`: with 2021-22 filled, the columns are no longer all-null, and the 2018-19 empty positions should warn honestly.
   - Measure all three.
b. **Strata (#1577).**
   - Merge the Guinea-Bissau `served_strata` blocks for 2018-19 and 2021-22 into the EXISTING `sampling.yml` entries; never paste duplicate wave keys. The proposal file is `reports/strata/out/proposed-Guinea-Bissau-served_strata.yml`.
   - Measure that the GNB 2021-22 key sits on every row with a stratum before setting `confidence: high`.
   - Remove GNB from `SERVED_STRATA_PENDING`. Benin stays pending (its own PR).
c. **`tests/test_sample.py` `PARTIALLY_WIRED_SAMPLE`:** remove GNB 2021-22 if it is now fully wired, and check the test.
d. **Recall:** add the GNB 2021-22 row to `.coder/coverage/food_recall.csv` (the gnb report gives it) and regenerate the recall records with the repo's own generator. Find it; do not hand-edit generated files.
e. **Coverage absent-verdicts:** add the coping stream's 8 drafted Nigeria `correctly-empty` rows from `reports/coping/REPORT.md` §6 item 7, with their evidence, `adjudicated_by` = "Sue (Claude Opus 5.5), ISA batch 2026-11-13". Flag the W1/W2 post-harvest assets pair as a judgement call in the PR body.
f. **Windows pin.** Re-measure Uganda shocks and assets rows and digest at the integrated tree with `scripts/windows_build_check.py`, on Linux, cold, in a private root. The coping stream adds `HowCopedSet` and filler NAs, so the shocks digest moves. Update the `.github/workflows/ci.yml` comment and `EXPECT`.
g. **Coverage regrade.** Run `make matrix` (see the Makefile and `bench/matrix.py`) on the integrated tree in a burst job, in a private root, and commit `.coder/coverage/latest.csv`. Report the tier movements per country.
h. **Hash census,** base `c79f543da` → integrated head. Expected: the union of the streams' countries (Benin excluded) and 0 `build_transforms_fingerprint`. Anything else is a finding.
i. **Combined-effect check.** Several countries are touched by more than one stream: Niger, Burkina, Mali, Senegal, Togo, CIV, GNB, Nigeria, Uganda. For each, confirm on a cold integrated build that each stream's headline movement still holds. Examples: Nigeria food kg 1,659,561; coping's −11,275 Nigeria shocks rows; Niger plot_inputs purchase 4,557; the GNB withheld 3,260 / 685; Burkina crop_production 24,705. No interaction may change another stream's numbers; state any that does.
j. **Tests.** Run the full non-S3 suite, plus the `requires_s3` tests of every touched country and of the shared files the streams edited, the way CI does (`-n $SLURM_CPUS_ON_NODE --dist=loadfile`, BLAS pins). Known flakes (do not fix, just label them):
   - `test_sample.py::test_covers_all_waves`
   - `test_gh932_safe_ci_failure_locations::test_real_wrapper_optin_…`

## Deliver
- **PR_BODY.md:** a short overview, then one section per stream condensed from its PR_BODY (rulings, what changes, must-states, cache cost), then integration tasks a–j with results.
- **Issues:** "Addresses #N" lines; this goes to development, so issues are closed by hand afterwards. List which issues close and which stay partial.
- **Ending:** end the PR body with the attribution line.
- **Final message:** the head SHA, the conflict log, the task results and the PR body.
