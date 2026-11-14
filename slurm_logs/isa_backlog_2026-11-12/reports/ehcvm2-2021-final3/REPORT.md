# EHCVM-2 2021-22, final3: Togo recovered-0 counts, the registered deletion, re-review records (addresses #1697)

**Agent:** fix agent, Claude Opus 5.5 (`claude-opus-5-5`), dispatched by Sue (job 41201840, n0035.savio4). No sub-agents or forks.

| | |
|---|---|
| Worktree | `/local/job41201840/worktrees/ehcvm2-2021` |
| Branch | `feature/552-ehcvm2-2021-wiring-20261112` |
| Start | `9b5585cf2dd9264616ee56255aedb5acef22f1f5` (asserted) |
| Head | `ee65db5771aa20b5404c5ef7607d7691eca4bbf2`, published to the mirror by the post-commit hook; nothing pushed to GitHub |
| Base | development `e5fdd2fa831c4d03a247d264be7f339da459e6b0` |

**Read:** `ehcvm2-2021-redteam2/VERDICT.md` (whole); `ehcvm2-2021-final2/{REPORT,PR_BODY}.md`; round-1 `ehcvm2-2021-redteam/VERDICT.md` (F1-F8); the 2026-11-12 entries of `.sucoder/isa-decisions.org` on `docs/isa-sweep-20261103`, newest first ("Togo floored counts and CIV H scope"); the Togo, Cote d'Ivoire and Benin `_/CONTENTS.org` 2026-11-12 sections; `Togo/2021-22/_/mapping.py`, `lsms_library/ehcvm.py` (`food_acquired_ehcvm`, `inputs_last_purchase`), `build_transforms.food_acquired_to_canonical`, `derivations.py` (field contract), the Niger and Burkina `rows: 0` entries.

PR body: `PR_BODY.md` here.

## Verdict

READY for a development PR. Targeted tests on the final head: **2,897 passed, 0 failed**, 84 skipped, 4 xfailed (40 files); schema `-k` the three countries 26 passed.

- **Recovered 0, measured: 21 source rows** (all `s07bq02` = `Oui`; Kara 9, Centrale 4, Plateaux 4, Savanes 3, Maritime 1). No recovered 0 remains: 0 count-recovered rows have value 0. Each of the 21 is served one purchased row with Quantity NA, Expenditure NA and the withheld key. These **21 new served rows** are the only difference from `9b5585cf2` in any Togo table.
- **Deletion entry: 25,234 rows**, equal to the red team's N4 count, returned exactly by its `inputs`.
- Every new and changed key's `derivation_inputs` joins (below). Togo 2018 and every derived table are frame-equal to `9b5585cf2`. Only Togo hashes move.

## Idiosyncrasies (from `CONTENTS.org`, as relevant)

- **Togo:**
  - `s07b` carries no region, so the hook reads `s00q01` from the cover (one region per grappe on all 540).
  - Region code 6 is `Lomé commune` (2018) / `Grand Lomé` (2021-22); its own/gift rows carry a second factor, 1.2.
  - `s07bq03b` and `s07bq07b` label one code book twice (139 `sachet` / `sachet (industrielle)`; 141).
  - Only `unité` (code 147) was floored after scaling.
  - 2018's `s07bq03b` lost its labels, hence the 659/660/662 decode.
- **Cote d'Ivoire:** `s07b` is a full 173-item grid. H rows are rescaled as a whole row (`s07bq04` and `s07bq05` too), which is why "Withhold all three" was already the built behaviour.
- **Benin:** the 6,138 leavers in `s01` (#1565).

Nothing I found contradicts `CONTENTS.org`. One idiosyncrasy is added: the unhashed cover (N5), now in Togo `CONTENTS.org` and a comment in `mapping.py`.

## Commits

| commit | what |
|---|---|
| `51ef26e5a` | `_recover`: a count outside Grand Lomé that recovers to 0 takes the withheld branch. New `rows: 0` entry `Togo::food_acquired::s07bq03a-purchase-removed-by-division` with `inputs_purchase_removed_by_division`. `_purchase_implied` is shared by the hook and the inputs callables. The unhashed-cover comment. Tests: the oracle, a synthetic registry test and a deletion test. |
| `84722db39` | `_recover` also returns the recovered-0 mask, and `_purchase_implied` serves every such row an NA purchased row whatever its own production or gift (Decision 1). The oracle follows. |
| `ee65db577` | Records only (`_/derivations.yml`, `CONTENTS.org`): the measured counts, the ~54 disclosure, the ruling assumptions, N1/N2/N5, #1710, the CIV H scope and sizings. Its hash probe is byte-identical to `84722db39`'s. |

## The changes

**1. Togo `unité` recovery (ruling "Recover; a 0 becomes NA").**
- Exact integer recovery outside Grand Lomé is kept. A count recovers to 0 only when it was filed as 0 (k = 0 is the one integer with floor(k f) = 0), the floor of a count below 1/f.
- **Source rows:** count-recovered 12,665 → **12,644**; withheld 4,035 → **4,056** (Grand Lomé 4,030, Kara 10, Plateaux 7, Centrale 5, Savanes 3, Maritime 1).
- **The 21 rows:**
  - 11 report no own production or gift. At `9b5585cf2` they were served nothing, so the item vanished.
  - 10 report own production or a gift below 1/f (0.5 ×6, 0.25 ×3, 0.16 ×1), and kept those rows.
  - `s07bq06` of the 11: `7 dernier jours` 6, `30 derniers jours` 1, `Plus de 30 jours` 4. Of the 10: `Jamais` 4, `Plus de 30 jours` 6.
- **Served:**
  - purchased rows 83,264 → **83,285**;
  - withheld-keyed NA purchased rows 3,895 → **3,916**;
  - `Expenditure` NA 17,525 → 17,546 (21.1%);
  - purchased `Expenditure` unchanged at 29,435,020.05;
  - the count key still serves 10,039 rows.
- **Disclosed** in Togo `CONTENTS.org` and in the count key's assumption:
  - the ~54 (0.43%) probable fractional counts, from the red team's simulation;
  - the error shape: typically −0.5, never beyond ±0.75;
  - the Maggi-cube concentration (65 of 2018's 79);
  - beside it, the measured 21 filed as 0. These are not reconciled with the simulation's "about one in five".

**2. Cote d'Ivoire H scope (ruling "Withhold all three").**
- Already built: 20,536 served rows (15,769 purchased / 2,722 produced / 2,045 in-kind), all with Quantity and Expenditure NA.
- Confirmed on the final head: `test_civ_2021_wave.py` and `test_ehcvm_7day.py` pass.
- Recorded in a new dated section of CIV `CONTENTS.org` and in the H key's second assumption and evidence.
- No CIV build input changed, and no CIV hash moves.

**3. N4, the registered deletion.**
- **Entry:** `Togo::food_acquired::s07bq03a-purchase-removed-by-division`, `waves: ['2021-22']`, `columns: [Quantity, Expenditure]`, `rows: 0`, `function: recover_s07bq03a`, `inputs: inputs_purchase_removed_by_division`.
- **Predicate:** the branch is divided or count-recovered, the filed residual is `> 0`, and the recovered residual is not `> 0`.
  - The filed test is the reshape's own test for serving a purchased row; none of these rows is at or below 1e-6.
  - Withheld rows are excluded because each is served an NA purchased row where a purchase is implied. 0 withheld rows have a positive filed residual and no NA row.
- **Count: 25,234** (24,489 divided, 745 count-recovered).
  - By `s07bq06`: `Jamais` 11,645, `Plus de 30 jours` 11,871, `7 dernier jours` 1,108, `30 derniers jours` 462, `Hier` 148 (1,718 within 30 days).
  - All report own production or a gift.
  - The recovered residual is exactly 0 on 25,224 and below 0 on 10.
  - By region: Kara 6,540, Plateaux 5,139, Savanes 4,837, Centrale 4,342, Grand Lomé 2,199, Maritime 2,177.
- **Accounting identity:** the 108,057 source rows with a positive filed residual = 79,535 served from the recovered value + 25,234 deleted + 3,288 withheld and served NA + 0 other.
- **Joins:**
  - `inputs` returns 25,234 rows, the raw predicate's rows household for household (the `i` multiset is equal);
  - no served row carries the key, and `attrs['derivations']` lists it at `rows: 0`;
  - 155 of the rows share `(t, i, j)` with a served purchased row (fold partners); this is disclosed.
- **Approximate value:** the deleted filed residual at the last-purchase unit value is about 127k FCFA on 1,599 rows (labels compared as filed; labelled approximate).

**4. N1.**
- Togo `CONTENTS.org` and the PR carry the red team's split (jobs 41251020 and 41251201), labelled as theirs:
  - the decode alone: +624 kg on 1,875 decoded rows;
  - the 2021-22 changes alone: +492 kg on 25,607 other rows;
  - interaction about 1;
  - CIV's +0.6% is entirely its 2021-22 slice.
- I did not re-splice. What licenses carrying the split forward: this head serves Togo 2018 68,062.04 kg, identical to `9b5585cf2`, with every 2018 table frame-equal and 0 kg factors changed.

**5. N2.**
- The red team's numbers, labelled: 122 rows, 122.5 against 216.9 units, about 18k FCFA understated, 0.3% of Grand Lomé's 6.22M.
- Disclosed in Togo `CONTENTS.org`, in the divided-out key's `validated_against` and in the PR.

**6. Other must-states.**
- N5 (the cover; CLAUDE.md cache limitation (a)): Togo `CONTENTS.org`, a `mapping.py` comment and the PR.
- F7 → #1710: all three `CONTENTS.org` and the PR.
- N3/N3b and N7 are ruled. N6 stays open.
- Recorded: N8, the CIV collision sizing, the 76-row 5x cluster and the ~2,200 off-grid purchase-only rows.

## Decisions for the coordinator

1. **The ten own/gift recovered-0 rows get an NA purchased row.**
   - **Why:** the ruling's text says "any count that recovers to 0 is served NA under the withheld key". My first commit served only the 11 with no own production or gift (the existing `implied` rule); the second extends `implied` so all 21 carry the withheld key.
   - **What supports it:** for a filed 0 the filed-residual test is silent, and q4 + q5 < 1/f on all ten, so the quantities cannot exclude a purchase below one unit.
   - **What cuts against it:** `s07bq06` puts none of the ten's last purchase within 30 days, and 4 are `Jamais`.
   - **Cost to revert:** one clause in `_purchase_implied`. Either way the derived tables drop the NA, so no served total moves.
2. **CIV numbers are the red team's.** The collision sizing, N8 and the large-value bound in the new CIV section were measured at `9b5585cf2`, not re-measured here, because no CIV build input has changed.

## Verification

**Every job:**
- `savio4_htc`, `co_carleton`, `-c 8 --mem=64G`, `--exclude=n0035.savio4`, `--chdir` set to `jobs/`;
- submitted with `TMPDIR` unset, with `TMPDIR=/local/job<ID>/tmp` on the job's first line;
- full-SHA `--shared` clones of the mirror, with no fetch inside it;
- private `LSMS_DATA_DIR`, `XDG_CACHE_HOME`, `TMPDIR`, `MPLCONFIGDIR` and `LSMS_ISSUES_LOG` under `/local/job<ID>/priv-*`, exported **before** `bin/savio_venv.sh mount`;
- import identity and a private `data_root()` asserted for every invocation;
- BLAS threads pinned to 1 and `LSMS_BUILD_WORKERS=1`;
- pytest `-n 8 --dist=loadfile`, output to files.

No more than 3 of my jobs ran at once. In-tree parquets after every job: none. The clones' `git status` was clean.

| job | what | result |
|---|---|---|
| 41252020 | cold, `9b5585cf2` vs `51ef26e5a`: Togo builds, checks, compare; corpus hash probe | 11 added NA rows (the non-own/gift ones); deletion 25,234; Togo 22/22 + 22/44 hashes |
| 41252021 | targeted tests at `51ef26e5a` (40 files) | 2,897 passed, 0 failed, 84 skipped, 4 xfailed; schema 26 passed |
| 41252164 | cold, `9b5585cf2` vs `84722db39` | the numbers in this report |
| 41252257 | cold, `9b5585cf2` vs final head `ee65db577` | hash probe byte-identical to 41252164's candidate; Togo checks and frame comparison identical to 41252164's |
| 41252255 | targeted tests at final head `ee65db577` (40 files) | **2,897 passed, 0 failed**, 84 skipped, 4 xfailed; schema 26 passed |

**Togo at the final head against `9b5585cf2`** (`compare.json`, `togo_check_cand.json`):
- **`food_acquired` 2021-22:** 111,637 → 111,658 rows.
  - 21 rows only in the candidate, all `s = purchased`, Quantity and Expenditure NA, keyed `7day + floored-count-withheld`.
  - 0 rows only in the previous head, and 0 changed values on common rows (`Quantity`, `Expenditure`, `Derivation`).
  - The index is unique, and 0 served rows have Quantity NA outside the withheld key.
- **Everything else is frame-equal**, in both waves: `food_acquired` 2018, `sample`, `food_expenditures`, `food_quantities` (kgs and units) and `food_prices`. 0 kg factors changed in either wave.
- **Joins:**
  - withheld: 3,916 served ↔ 3,916 inputs, one-to-one; 21 of the inputs are filed zeros outside Lomé;
  - divided-out: 68,744 of 68,744 groups sum-equal;
  - count-recovered: 10,037 of 10,037 groups sum-equal;
  - 7-day: every served keyed `(t, i, j)` is in the inputs;
  - deletion: as above.
- **Hashes against `9b5585cf2`:** Togo 22 of 22 `_table_cache_hash` and 22 of 44 `Wave._input_hash` (the 2021-22 wave); every other country 0; `build_transforms_fingerprint` 0 of 40.

**Tests:**
- The 40 files are final2's 38 plus `test_gh1503_record_caches_do_not_cross_tests.py` and `test_categorical_mapping_resolution.py`.
- No Togo, Cote d'Ivoire or EHCVM test was skipped for want of data. The skips are:
  - corpus-parametrised `test_sample` cases for other countries;
  - two `case is for Togo/Benin` guards;
  - an import-root guard.

## Disclosures

- **On n0035 I ran only shell commands, never python:**
  - `git` (status, diff, add, commit, log, show, rev-parse, cat-file);
  - `sbatch` and `squeue`;
  - `mkdir` and `cp` of my scripts into the report directory;
  - `cat`, `grep`, `tail`, `jq`, `cmp`, `diff` and `wc` on job outputs;
  - this report, written with a shell heredoc.
- **Job polling:** waiting used `until squeue ...; do sleep ...; done` loops through the harness's background shell on n0035. Their output files are in the harness's task directory under `/local/job41201840/tmp/claude-...`, not in `/tmp`. I started one redundant watcher by mistake and stopped it.
- **No `/tmp` fallback this time:** no job log mentions one, and every identity line names `/local/job<ID>/priv-*`.
- **Comparison tolerance:** frame equality uses `assert_frame_equal(check_exact=False, rtol=1e-9)`. Changed-value counts use the same tolerance, with NaN equal to NaN.
- **Numbers that are not mine:** N1's split, N2's 122 rows and 18k, the ~54 simulation, the 76-row cluster, the ~2,200 rows and the CIV sizings are all the second red team's, and labelled as such everywhere.
- **Approximation:** the deleted rows' 127k FCFA is approximate (labels compared as filed).

**Files:**
- Scripts in `scripts/`: `cold.sbatch`, `tests.sbatch`, `common.sh`, `build_food.py`, `togo_check.py`, `compare.py`, `hash_probe.py`, `hash_diff.py`.
- Outputs in `jobs/<label>-<jobid>/`.
- Aggregates only; no raw rows.

## Not verified

- **Cold builds only.** There were no warm-cache, fresh-process or `assume_cache_fresh` reads, no `Feature()` assembly and no strict levers. The tests were the targeted 40 files and the schema selection, not the full suite.
- **Cote d'Ivoire and Benin were not rebuilt this round.** "Unchanged" rests on 0 hash movement at both tiers against `9b5585cf2` and on no shared-code edit.
- **The 2021-22 `food_quantities(units='kgs')` total (58,744 in the PR) is carried from final2.** This round's frames equal `9b5585cf2`'s: 58,742.6 on `u` = `kg`, 58,743.6 over all rows.

## Addenda

- **Decision 1 was the reviewer's call, not mine.** The reviewer (the advisor tool) recommended serving the ten own/gift recovered-0 rows an NA purchased row, on the ruling's text and on quantities. The `s07bq06` crosstab, measured after that commit, cuts against it.
- **TMPDIR on n0035 is `/local/job41201840/tmp`,** so the shell heredocs (commit messages and this report) did not write to `/tmp`.
- **No job log mentions a `/tmp` fallback.** I grepped the logs of 41252164, 41252255 and 41252257.
- **PR body completeness.** It restores final2's re-pinned-test list, adds the second red team's sizing of the loosened pins (§8) and the round-1 Q7 scope items, and states the same unverified limits as above.
