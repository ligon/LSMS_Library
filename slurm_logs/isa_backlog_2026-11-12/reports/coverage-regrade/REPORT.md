# Coverage regrade for v0.17.0

Agent: coverage-regrade agent, running on Claude Opus 5.5 (`claude-opus-5-5`), dispatched by Sue (job 41201840).
Date: 2026-11-12/13 (UTC).

## Result

- **Commit:** `5cd2787bcc602846be02238e2a2de2df8cb5f02c` on the new branch `coverage/regrade-20261112`, whose parent is
  `446adc3c27b965a67fa32a9679bc2a2362619523` (integration head, PR #1711). The worktree is `/local/job41201840/worktrees/coverage-regrade`.
  The post-commit hook published it to the mirror as `origin/coverage/regrade-20261112`. Nothing was pushed to GitHub.
- **Files changed:** only `.coder/coverage/latest.csv` (+556 / −335 lines). `blessed.csv` and `absent_verdicts.csv` were not touched.
- **Cells:** 2545 before, 2766 after. 2535 keys are in both, 231 are new and 10 were replaced. There are no duplicate keys.
- **Countries:** all 38 configured countries were graded and merged. None was skipped, and every country present before is present after.

## Method

- **One cold build per country** (38 jobs, plus 2 reruns, explained below). Account `co_carleton`, partition `savio4_htc`, QOS `carleton_htc4_normal`, `--exclude=n0035.savio4`, `--requeue`, at most about 8 jobs running at once.
  - Light countries: 4 CPUs, 48G, 4 h.
  - Heavy countries (Uganda, Malawi, Nigeria, Tanzania, GhanaLSS, Ethiopia, EthiopiaRHS, GhanaAHIES, CotedIvoire, Mali, Niger, Burkina_Faso): 6 CPUs, 96G, 12 h.
  - Of the jobs whose shards were used, none was pre-empted, failed or ran out of time. The slowest was Malawi at 988 s; the largest peak RSS was 7.7 GB (Malawi, GhanaAHIES).
  - The first pilot was aborted. Its empty-data-root check ran after the venv mount, which had already imported the library, so Uganda exited and I cancelled Kosovo and Guyana. That attempt's logs are in `old_attempts/`. The check was moved to run before any import, and all three were resubmitted.
  - Concurrency peaked at 9 jobs for about 2 minutes (7 heavy plus the Nepal and Albania reruns).
- **Each job:**
  1. Created a fresh per-attempt root under `/local/job$SLURM_JOB_ID/` and pointed `TMPDIR`, `LSMS_DATA_DIR`, `XDG_CACHE_HOME`, `MPLCONFIGDIR` and `LSMS_ISSUES_LOG` there. All of this happened before anything imported the library.
  2. Asserted that the data root was empty. This is a cold L1 and L2: source files were fetched through the library (`get_dataframe` / DVC sidecar / WB fallback) into the private L1. Data-root sizes after the build range from 2.6 MB (Guyana) to 376 MB (Uganda).
  3. Unset every inherited `LSMS_*_STRICT` lever, plus `LSMS_NO_CACHE`, `LSMS_BUILD_BACKEND`, `LSMS_COVERAGE_SNAPSHOT`, `LSMS_ALLOW_INTREE_SOURCES` and `LSMS_SKIP_AUTH`.
  4. Ran a network preflight.
  5. Ran `git clone --shared` of the mirror and `checkout --detach` at the full SHA, then asserted HEAD.
  6. Ran `bin/savio_venv.sh mount` (image generation `20261019T221223Z`).
  7. Set `PYTHONPATH` to the clone, `PYTHONSAFEPATH=1`, `LSMS_COUNTRIES_ROOT` to the clone's countries directory, BLAS threads to 1 and `LSMS_BUILD_WORKERS=1`.
  8. Asserted that `lsms_library.__file__`, `countries_root()` and `data_root()` were all under the clone or the private root, and not under the shared roots.
  9. Ran `bench/matrix.py --countries <C> --snapshot <local shard> --no-html`, which is what `make matrix C=<C>` runs.
- **Merge:** a burst job read the committed `latest.csv` from a clone at the SHA. It then applied `lsms_library.coverage_matrix.save_snapshot(shard, work, authoritative_countries=[C])` once per country, which is exactly the semantics of running `make matrix C=<C>` for each country in turn. The file was never replaced wholesale, and `bench/matrix_aggregate.py` was not used because it replaces the file wholesale.
  - Each shard was first checked: one country only; no `<country>` broken row; not entirely `broken` unless it was before; at least 0.8 times its old row count; no duplicate keys. All 38 passed.
- **Diff sanity:** the git diff is 556 added and 335 removed lines, which is 325 modified rows, 10 replaced rows and 231 new rows. That matches the row-level diff exactly, so there is no quoting or formatting churn.
- **Hygiene, all 40 runs:**
  - 0 parquets were written into `lsms_library/countries`.
  - 0 untracked files appeared in the clone.
  - The issues logs are empty except for the expected no-microdata and unwired-table entries (Armenia, Nepal, Peru).
- **Scripts and logs:** `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/coverage-regrade/`.
  - `shard.sbatch`, `merge.sbatch`, `merge_diff.py`, `diag.py`: the scripts.
  - `status/*.status`: per-job status, including node, rc, elapsed time, maxrss, in-tree parquet count and data-root size.
  - `shards/`: the per-country snapshot rows.
  - `logs/`: the per-job logs.
  - `merge/run-41259218/`: the final merge, with `new_latest.csv`, `gate.csv`, `tier_counts.csv`, `transitions_overall.csv`, `transitions_by_country_feature.csv`, `worse.csv`, `vanished.csv`, `appeared.csv`, `rows_shrunk.csv`, `per_country.csv` and `summary.md`.

## Tier counts

| tier | before | after | delta |
|---|---|---|---|
| sane | 1559 | 1675 | +116 |
| builds | 10 | 11 | +1 |
| dropped | 38 | 21 | −17 |
| broken | 2 | 2 | 0 |
| correctly-empty | 0 | 2 | +2 |
| asked-not-distributed | 5 | 6 | +1 |
| not-asked | 16 | 16 | 0 |
| blocked | 7 | 7 | 0 |
| absent | 428 | 556 | +128 |
| undeclared | 480 | 470 | −10 |
| blessed | 0 | 0 | 0 |

## Tier transitions (old → new, cells)

Unchanged keys: sane→sane 1550, undeclared→undeclared 470, absent→absent 418, dropped→dropped 16, not-asked 16, builds 7, blocked 7, asked-not-distributed 5, broken→broken 2.

| old → new | cells | where |
|---|---|---|
| (new) → absent | 138 | Armenia ILCS 2001–2017 ×34 (held, not wired); GhanaLSS ×29; Guinea-Bissau 2021-22 ×22; Liberia ×20; Malawi 2024-25 ×20; Niger ×6; Peru ×3; Burkina ×2; Benin ×1; Senegal ×1 |
| (new) → sane | 92 | Benin/Togo 2021-22 ×22 each; CotedIvoire 2021-22 ×20; GhanaLSS ×13 (crop_production, labor, livestock, nonfood_expenditures, plot_inputs, plot_labor); Malawi ×7 (2024-25 food tables + sample, panel_ids/updated_ids); Niger ×2 (cultivation_records, farm_cost_operands); Nigeria ×2 (panel_ids, updated_ids); community_prices Burkina/Guinea-Bissau/Senegal ×1 each; Peru sample ×1 |
| (new) → asked-not-distributed | 1 | CotedIvoire |
| dropped → sane | 20 | Nigeria community_prices PP ×5, interview_date ×4, plot_features ×3, food_coping ×1; GhanaLSS food_prices ×3; Mali interview_date ×2; Tanzania interview_date ×2 |
| dropped → correctly-empty | 2 | GhanaLSS food_prices (verdicts already in `absent_verdicts.csv`) |
| absent → sane | 10 | Mali crop_production ×2, plot_inputs ×2, community_prices ×1; Nigeria cluster_features ×2; Niger community_prices; Burkina plot_inputs; Senegal plot_inputs |
| builds → sane | 3 | Uganda housing ×1, nonfood_expenditures ×2 |
| undeclared → (gone) | 10 | see "Cells that disappeared" |
| **sane → dropped** | **5** | Nigeria individual_education, the PP quarters |
| **sane → builds** | **4** | Malawi plot_inputs, all four waves |

## Cells that got worse (9), with the ordering used

A cell counts as worse if any of these holds:
- R1: it becomes `broken` from anything else.
- R2: it becomes `dropped` from anything other than `broken`.
- R3: it goes `sane`/`blessed` → `builds`.
- R4: it goes from a built tier (`sane`/`blessed`/`builds`) to a tier that is not built or not graded.
- R5: it loses a blessing.
- R6: a built cell disappears.

Only R2 and R3 fired. All 9 cells are deterministic config or design consequences, not fetch, auth or timeout failures, so none needed a transient rerun.

| country | feature | wave | old → new | detail (new) |
|---|---|---|---|---|
| Malawi | plot_inputs | 2010-11 | sane → builds | `fail: no_all_null_columns; warn: index_levels_match_scheme,no_constant_columns` |
| Malawi | plot_inputs | 2013-14 | sane → builds | same |
| Malawi | plot_inputs | 2016-17 | sane → builds | same |
| Malawi | plot_inputs | 2019-20 | sane → builds | same |
| Nigeria | individual_education | 2010Q3 | sane → dropped | `declared for wave but absent from built t-index` |
| Nigeria | individual_education | 2012Q3 | sane → dropped | same |
| Nigeria | individual_education | 2015Q3 | sane → dropped | same |
| Nigeria | individual_education | 2018Q3 | sane → dropped | same |
| Nigeria | individual_education | 2023Q3 | sane → dropped | same |

**Malawi plot_inputs (4 cells): needs a decision before release.**
- Diagnosed with a cold build of the same tree (`logs/diag-Malawi-*.txt`, column names and counts only).
- The served frame now carries the retained native Module H columns (`ag_h*`, about 100 of them), plus provenance and acquisition columns.
- Six of those native columns are null in every wave of the country frame, so the per-wave `no_all_null_columns` check fails: `ag_h17_os`, `ag_h26b_os`, `ag_h27_os`, `ag_h36b_os`, `ag_h26_1b`, `ag_h36_1b`.
- This is the documented design of `95aef402d` ("all native H scalar columns are retained literally, including blank source text", GH #1190, `Malawi/_/CONTENTS.org` 2026-11-10). That commit is already on `development`.
- That entry also says no served Country/Feature build had run yet, so this regrade is the first served measurement of the change.
- Rows rose 20–25% per wave (for example 2019-20: 64,875 → 80,941), from the added Farm-level acquisition rows.
- Not a data loss. It is a schema and grade consequence: either drop or declare-optional the all-blank native text columns, or accept `builds`.

**Nigeria individual_education PP quarters (5 cells): explained.**
- The old snapshot gave each PP quarter exactly the PH quarter's row count (643/643, 313/313, 17,687/17,687, 19,271/19,271, 19,053/19,053).
- Before `dd1dc13a4` (PR #1318, GH #1292), the YAML read the PH section-2 file with no `t` override, so both quarters of each wave folder served the same PH rows.
- Now `t` is pinned to the PH quarter (2011Q1 … 2024Q1), which the cold diagnostic confirms is all the table serves.
- Both quarters still declare the table (`Country('Nigeria')['2010Q3'].data_scheme` contains it), so the PP cells grade `dropped`.
- The data improved (a double-dating removed), but the tier fell. To close these cells, either stop declaring the table for the PP quarters or add evidenced `correctly-empty` verdicts. `Nigeria/_/CONTENTS.org` says PP attainment was not adjudicated, so a verdict needs C4 evidence.

## Unchanged tier, row count fell by more than 10%

These are not tier regressions, but they are worth a look before release:

| country | feature | wave | n_rows old → new | ratio | note |
|---|---|---|---|---|---|
| Nigeria | community_prices | 2016Q1 | 33,781 → 23,153 | 0.685 | 23,153 is the intended count recorded in `Nigeria/_/CONTENTS.org` (the GH #840 `obs` table, line 1893, and the #1287 compiler record, line 3822). GH #587 also now serves the PP quarter: 2015Q3 went dropped → 24,995, so the W3 total is 33,781 → 48,148 |
| Uganda | plot_labor | 2009-10 | 37,107 → 32,758 | 0.883 | not diagnosed; several Uganda 2009-10 id/ea/plot-labor commits land in this range (`90a35781a`, `a2cf04761`, `4ad000b8d`) |

On keys whose tier did not change, 237 changed only `n_rows` and 99 changed `detail`.

## Warnings that appear without changing a tier

The grader adds some notes to `detail` without moving the tier, so the tier rules above cannot see them. Checked across the whole file, old against new:

- **New `scale_ratio` notes (GH #830): 4 cells.** None of the old snapshot's cells carried this note, and all four stay `sane`:
  - GhanaLSS community_prices 1998-99: Price median 2,000 is 5.56× the 1991-92 median (300), per item [108].
  - GhanaLSS food_acquired 1991-92: Price median 100 is 0.16× the 1998-99 median (1,200), per item [45].
  - GhanaSPS crop_production 2009-10: Value_sold median 80 is 0.167× the 2017-18 median (500), per item [16].
  - GhanaSPS crop_production 2017-18: Value_sold median 500 is 6× the 2009-10 median (80), per item [16].
  - The check skips wave pairs with different ISO currency codes; the 2007 cedi redenomination falls between 2005-06 and 2012-13, not inside these pairs. So these are cross-wave scale gaps worth a human look, not currency artefacts.
- **New `dtype_consistency` warning: 35 cells, all `crop_production` and all still `sane`.** It covers 12 countries: Benin, Burkina_Faso, CotedIvoire, Ethiopia, Guinea-Bissau, Malawi, Mali, Niger, Senegal, Tanzania, Togo and Uganda.
- **Other check counts, old → new:** `no_all_null_columns` 0 → 4 (the Malawi failures); `no_null_index_levels` 20 → 4; `no_float_stringified_index` 2 → 0; `ids_format_id_canonical` 2 → 0. The rest grew in line with the new cells: `index_levels_match_scheme` 791 → 848, `no_constant_columns` 248 → 272, `has_household_index` 139 → 155.

## Transient-failure sweep

No matrix log mentions a fetch failure. I grepped every `logs/*.matrix.log` for `timed out`, `EndpointConnection`, `ConnectTimeout`, `SlowDown`, `Connection reset` and `ReadTimeout`, with 0 hits. Every `broken` and `dropped` cell after the regrade is either unchanged from before or explained above.

## Cells that disappeared (10): all explained

Each was a `(country, feature, "")` `undeclared` row replaced by per-wave rows because the feature is now declared:
- Benin, Burkina_Faso, Guinea-Bissau, Senegal: `community_prices`
- GhanaLSS: `crop_production`, `livestock`, `nonfood_expenditures`, `plot_inputs`, `plot_labor`
- Peru: `sample`

No built cell disappeared.

## Countries without microdata keep their verdicts

- **Nepal:** 6 `blocked` and 27 `absent`, both unchanged.
- **Armenia 1996:** the 2 cells stay `broken` (`FileNotFoundError`; the path inside the detail now names this run's job directory, as the old one named job 38714904). The 34 new Armenia cells are the held ILCS 2001–2017 waves, which grade `absent` because they are not wired.
- **Albania 1996:** the `<wave>` row stays `blocked`.

**Finding: `.coder/coverage/blocked_sources.csv` is not in git.**
- It is gitignored by `*.csv` and exists only in the mirror's working tree (mtime 2026-07-12, sha256 `51fb8e50…`).
- The old snapshot was graded with it present. A clean clone at the SHA lacks it, so Nepal's 6 `blocked` cells came out `broken` and Albania's 1996 `blocked` row vanished.
- I copied the mirror's file, read-only, into the clones of two reruns (Nepal, Albania, jobs 41259141 and 41259142; snapshot of the input at `inputs/blocked_sources.csv`) and merged those shards. Every other country is unaffected, because the file is keyed by `(country, wave)` and names only those two configured countries.
- The documented reseed chain (`bin/coverage_reseed*.sbatch`) builds in a clone too, so it would hit the same loss.
- Committing the file, with a `.gitignore` exception like the one `latest.csv` must already have, would fix it.

## Not graded

None of the 38 configured countries. Two caveats:
- **Microdata-only countries.** The `unconfigured` rows for country folders with microdata but no `_/data_scheme.yml` are not emitted by per-country runs. The old snapshot had none either, because a full run filters them out too, so nothing changed there.
- **Blocked sources of unconfigured countries.** The `blocked_sources.csv` rows for Jamaica, Ecuador and Vietnam are likewise not emitted, as before.

## Rule compliance notes

- **Data roots and the tree:**
  - No shared data root was used; each job asserted its `data_root()` was private.
  - No `dvc` CLI was run, and no lock was removed.
  - Nothing was written into any `lsms_library/countries/` tree (0 in-tree parquets, 0 untracked files in every clone).
  - No GitHub write or push, and no `git fetch` in the shared mirror.
- **Work on n0035:** git commands (worktree, commit, log, diff, show) and file reads/writes with grep, cut and the file tools. All Python ran in burst jobs.
- **Two slips on n0035.** I ran `sed` twice by mistake, both times as a read-only stdout filter inside a pipeline (one ended in `head -0`). They edited no file, but they break the "no sed" rule.
- **Detail texts that name job paths.** Two Armenia 1996 detail texts carry a `/local/job…` path from the job that graded them. The old snapshot did the same with job 38714904.
