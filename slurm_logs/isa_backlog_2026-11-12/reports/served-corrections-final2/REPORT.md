# Niger 2018-19 #1708: region floor of 3, only previously valued rows

Fix agent **Claude Opus 5.5** (`claude-opus-5-5`), for Sue (scrum master, Slurm job 41201840), 2026-11-12. I used no sub-agents and made no GitHub writes. This report holds aggregates only: no microdata rows and no household ids. The pickled frames and pytest logs are in the mode-700 `private/served-corrections-final2/`.

## Verdict

**Done. The final head `06bac74298656d4f76b300ddd3b736f61eaf3f69` is green:** 5,940 passed, 0 failed (125 files). Both ruled adjustments are in. Lead consequence for the coordinator:

- **Niger 2018-19's served purchase Expenditure falls by 12.6%**, from 53,351,991.10 to 46,636,756.04 FCFA. The first scope moved it by only 1.6%.
- **5,659 of the 15,014 flagged rows are now served NA:** 2,872 revalued rows that have no 3-report median, plus the 2,787 that #876 left NA.
- 93.1% of the fall is the #876 value of the 2,872 rows that are now NA. Those household × item cells leave `food_expenditures`.

**Branch** `fix/isa-served-wave-corrections-20261112`. I started at `e338fedbac29b283f987717136bbb8e4020f953b` (asserted). The post-commit hook published each commit to the mirror, and every job cloned the mirror at a full SHA.

| commit | what |
|---|---|
| `7644de2b5ccda7b88bd3897b0ee4e1a6a0016030` | `Niger/2018-19/_/mapping.py`: region floor 3; revalue only the flagged rows #876 values, decided by the #876 function itself; the hook checks the scope against the value it served; the switch is removed. Synthetic and served tests updated |
| `06bac74298656d4f76b300ddd3b736f61eaf3f69` | `Niger/_/derivations.yml` and `Niger/_/CONTENTS.org` text (the ruling quoted); `test_ehcvm_7day` Niger 2018-19 total re-pinned |

## The changes (rulings @ligon 2026-11-12, "GH1708 revaluation details, and the release plan")

1. **Region floor of 3.** `LOCAL_MEDIAN_MIN_REGION_REPORTS = 3`, beside the cluster's 3. A row with fewer reports at both levels is NA.
2. **Only previously valued.**
   - `_revalued_records` = flagged ∧ purchased Quantity > 0 ∧ #876 value not NA.
   - The #876 value comes from `_value_876`, which calls `ehcvm.derive_ehcvm_purchase_value` itself.
   - `_revalue_flagged_purchases` now also requires that value to equal the one it served, before writing by position; otherwise it raises `LocalMedianJoinError`.
   - `LOCAL_MEDIAN_REVALUES_EVERY_FLAGGED_PURCHASE` is gone.
   - The 2,787 rows #876 left NA keep that NA and carry the flag key alone.
3. **Unchanged:** the registered key, `inputs`, the flag key, Quantity, `Quantity_kg` and consumed quantities. Verified: the index and both quantity columns equal development's. Derivation is base + flag on the 2,787, base + flag + new key on the 12,227, and base on every other row.

## Measured

Cold Niger builds of development `e5fdd2fa8`, the first implementation `e338fedba` and `7644de2b5`, each in its own private root (burst job 41254597).

| 2018-19 purchases | development (#876) | first scope | now |
|---|---|---|---|
| Expenditure, all (FCFA) | 53,351,991.10 | 52,486,790.70 | **46,636,756.04** |
| on the 15,014 flagged rows | 13,890,402.00 (26.04%) | 13,025,201.60 (24.82%) | 7,175,166.95 (15.39%) |
| flagged rows with an Expenditure | 12,227 | 13,022 | 9,355 |

**Levels among the 12,227 revalued rows.**

| level | rows | share |
|---|---|---|
| cluster | 1,107 | 9.1% |
| region | 8,248 | 67.5% |
| NA | **2,872** | 23.5% |

- **Why the 2,872 are NA.** 1,415 have no genuine report in their region cell; they were NA under floor 1 as well. The other 1,457 have 1 or 2 reports, and the region floor withholds them.
- **What the region values rest on.** 3–4 reports for 225 rows, 5–9 for 1,949 and 10 or more for 6,074. Every cluster value has at least 3 reports.
- **The ruling's estimate of about 3,787 NA, reconciled.** It counted all 15,014 rows under the old scope. In scope the count is 2,872, measured here. Out of scope it would be 915: 577 already NA at floor 1, plus 338 region rows on 1 or 2 reports. The 915 is derived from the first run's counts, not measured. 2,872 + 915 = 3,787.

**Where the fall comes from** (`decomp.json`). The fall is 6,715,235.05 FCFA, all on flagged rows. `CONTENTS.org` says .06, the difference of the rounded totals.

| part | FCFA | share |
|---|---|---|
| #876 value of the 2,872 rows now NA | 6,254,048.04 | 93.1% |
| revaluing the 9,355 still valued (7,636,353.96 → 7,175,166.95) | 461,187.01 | 6.9% |

**Against the first scope.** Expenditure differs on 3,667 rows, all of them flagged:

- 2,210 are rows among the 2,787 that are NA again;
- 1,457 are withheld by the region floor.

The 9,355 rows that both scopes value are identical.

**Against the producer's unit values.** Over the 9,355 rows valued both before and after, the revalued unit value is divided by the producer's `s07bq08 / s07bq07a`:

| | all | cluster (1,107) | region (8,248) |
|---|---|---|---|
| median | 1.00 | 1.00 | 1.00 |
| exactly equal | 28.9% | 17.1% | 30.5% |
| p10–p90 | 0.73–1.25 | 0.71–1.22 | 0.73–1.25 |
| p5–p95 | 0.62–1.31 | 0.65–1.41 | 0.61–1.31 |
| within 10% | 50.9% | 43.5% | 51.8% |
| within 25% | 77.7% | 69.0% | 78.9% |
| within 2× | 95.8% | 97.1% | 95.6% |

**Registered function on `derivation_inputs`.**

- The inputs are 107,269 records, 15,264 of them flagged. The function returns 12,227 rows.
- It reproduces the served Expenditure exactly: the same 12,227 (t, i, j) keys, a max difference of 0, and the same NA pattern.
- Every served row carrying the key joins its inputs (0 without one).

**Other Niger waves.** Niger serves `food_acquired` in 2018-19 and 2021-22 only. 2021-22 `food_acquired` is frame-equal (`assert_frame_equal`, exact) against development and against the first scope.

**Derived tables, against development** (against the first scope in brackets):

| table | 2018-19 | 2021-22 |
|---|---|---|
| `food_expenditures` | 2,872 rows go, 6,746 change, 0 appear [3,667 go, 0 change] | frame-equal |
| `food_prices(units='unitvalue')` | 2,872 go, 6,746 change [3,667 go, 0 change] | frame-equal |
| `food_prices` (kgvalue) | 2,872 go, 6,894 change [3,667 go, 95 change] | **57 change** [54] |
| `food_quantities` (kgs) | 810 change; 202,924.9 → 202,940.0 kg [670] | **93 change**; 208,904.3 → 208,906.0 kg [94] |
| `food_quantities(units='units')` | frame-equal | frame-equal |

The 2021-22 kilogram-mode movement comes from cross-wave pooling of the inferred kg factors (GH #1487).

**Hash footprint** (config-only probe).

| comparison | `_table_cache_hash` | `Wave._input_hash` | `build_transforms_fingerprint` |
|---|---|---|---|
| first scope (`e12278688` probe) → `7644de2b5` | 24 of 544 (Niger 24/24) | 24 of 2,258 (Niger 2018-19) | 0 of 41 |
| development → `7644de2b5` | 96 of 544 (Mali 23, Niger 24, Senegal 22, Uganda 27) | 307 of 2,258 | 0 of 41 |
| `7644de2b5` → `06bac7429` | 0 | 0 | 0 |

The development footprint is identical to the previous report's. Between `7644de2b5` and the head, the only files that changed are `Niger/_/CONTENTS.org`, `Niger/_/derivations.yml` and `tests/test_ehcvm_7day.py`.

## Tests

**Selection.** Every `tests/test_*.py` naming `Niger` or `ehcvm`, plus:

- `test_derivations` and `test_build_transform_hash`;
- `test_no_build_input_in_contents_org` and `test_failure_is_not_absence`;
- `test_schema_consistency` and `test_gh1578_derivation_duplicates`;
- `test_gh1707_served_corrections`.

That is 125 files. They ran with `-n 8 --dist=loadfile`, BLAS pinned to 1 thread, `LSMS_BUILD_WORKERS=1`, in a cold private root, with output to a private file.

| run | result |
|---|---|
| `7644de2b5` (job 41254597) | 5,939 passed, 15 skipped, 4 xfailed, 1 failed: the expected `test_ehcvm_7day` Niger 2018-19 total pin. Every new served pin passed |
| `06bac7429`, the head (job 41254830) | **5,940 passed, 15 skipped, 4 xfailed, 0 failed** (877 s) |

**Re-pinned, with reasons.**

- **`test_ehcvm_7day` `AFTER[('Niger','2018-19')]`:** 52,486,790.70 → 46,636,756.04. 2,872 previously valued rows are withheld, and the 2,787 rows #876 left NA are no longer revalued. Inkind 8,141 is unchanged.
- **`test_gh1707_served_corrections`, served tests:**
  - the revaluation key is on 12,227 rows (was 15,014);
  - the other 2,787 flagged rows are NA and carry the flag only;
  - the function returns 12,227 rows.
- **`test_gh1707_served_corrections`, synthetic tests:**
  - the floors are 3 and 3, and the switch is gone;
  - a new scope test;
  - a region with 2 genuine reports is NA (it would be 305 at floor 1);
  - the Bol-consumed flagged record, which #876 serves NA, is not revalued.

## Text updated

- **`Niger/_/derivations.yml`:**
  - the local-median entry's `rule`;
  - its floor and scope `assumptions`, which quote the ruling;
  - its `validated_against`;
  - the #876 entry's note;
  - the flag entry's rule and assumption.
- **`Niger/_/CONTENTS.org`:** both #1708 sections now carry the ruling, the rule, the measured table and bullets, the comparison with the first scope, and the first scope's superseded numbers.
- **PR body:** `PR_BODY.md`. The region floor and the gate now sit in the rule, not under "For the maintainer".

## For the coordinator

- **The 12.6% fall above.**
- **2021-22 kilogram modes move:** 93 `food_quantities` rows and 57 `food_prices` rows, through GH #1487.
- **Non-integer `s07bq08`.** 162 genuine reports have one, defined as more than 1e-9 from an integer; the first report gave 167. They price their cells. I did not re-measure how many revalued rows read such a cell.
- **Older builds reused.** The Mali, Senegal and Uganda cold builds are the previous run's. The only build-path change since `5f113252a` is this `mapping.py`, and the hash probe moves only Niger.
- **Known loose text, left alone.** In `_stamp_computed_last_purchase`'s docstring, "its Expenditure includes a revalued one" is now loose. Fixing it would move 48 Niger hashes with no job left to test them.
- **The release.** Per the decisions file, v0.17.0 follows the six-branch batch; nothing here touches that.

## Hygiene

- **Private roots.** `LSMS_DATA_DIR`, `XDG_CACHE_HOME`, `TMPDIR`, `MPLCONFIGDIR` and `LSMS_ISSUES_LOG` were exported before `bin/savio_venv.sh mount`.
- **Guards.** Every script asserted the import identity (the clone) and a `data_root()` under `/local/job<ID>/priv-*`, never the shared cache or `~/.local/share`. `LSMS_SKIP_AUTH` was unset, and `config.yml` was readable.
- **`TMPDIR` warning.** Slurm's prolog printed "Unable to create TMPDIR [/local/job41201840/tmp] … Setting TMPDIR to /tmp", because `--export=ALL` carried the submit node's value. Each script's first command set `TMPDIR=/local/job<ID>/tmp`.
- **Data access.** No `dvc` CLI. Sources were read through the library into the private L1.
- **No in-tree parquet** in any clone.
- **Issues logs** were private: 0 lines in the build, hash and decomp roots, 124 in each test root.
- **Jobs:** 2, both on `savio4_htc` with 8 cores and 64 GB, excluding n0035. Job 41254597 ran on n0037 and job 41254830 on n0052.

**What I ran on n0035.** No python, sed, awk or perl. I edited with the file tools and read with Read. Outside the brief's lists I also ran:

- `git grep`, once, to locate helpers in `ehcvm.py`.
- `git cat-file -t`, `git rev-parse` and `git branch --show-current`.
- `git show docs/isa-sweep-20261103:.sucoder/isa-decisions.org`, written to my session scratchpad under `/local/job41201840/tmp/claude-*/` (not `/tmp`).
- One `git show … | grep -n` on a test file.
- In the job-watch loops and log checks: `grep`, `tail`, `cat`, `comm`, `sort` and `wc`.
- `ls`, `cp` (two hash scripts from the previous run), `mkdir`, `chmod`, `sbatch`, `squeue`, and this heredoc.

All of these are read-only, or write only under the report and private directories.

## Files

Under `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/served-corrections-final2/`:

- `REPORT.md` and `PR_BODY.md`.
- `scripts/`: `s2_env.sh`, `s2_common.py`, `s2_build.py`, `s2_compare.py`, `s2_decomp.py`, `s2_hash_probe.py`, `s2_hash_diff.py`, `job1.sbatch`, `job2.sbatch`.
- `results/`:
  - `build_{base,prev,cand}_Niger.json`, `compare.json`, `revaluation.json`, `decomp.json`;
  - `hash_cand.json`, `hash_final.json`, `hash_vs_base/`, `hash_vs_prev/`, `hash_final_vs_cand/`;
  - `pytest_{cand,final}_{selection,summary}.txt`.
- `logs/`.

Private data to delete when this is closed: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/private/served-corrections-final2/`.
