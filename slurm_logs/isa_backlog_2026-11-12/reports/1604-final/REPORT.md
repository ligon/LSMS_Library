# PR #1604 final fixes: red-team findings applied

Fix agent: Claude Code, model **Claude Opus 5.5** (`claude-opus-5-5`), dispatched by Sue (Slurm job 41201840, n0035.savio4), 2026-11-12. No sub-agents. No pushes or GitHub writes. The post-commit hook published the commits to the mirror.

**Verdict: READY.**
- Head: `538eec00d9eb354d0dd7f296660295a8488f61bd` (branch `fix/malawi-food-integration-20261107`).
- Started from `7735489cceb0fd68acf273eff850a5974d71cc58`.
- Base: development `e5fdd2fa8`.

## Commits

| sha | what |
|---|---|
| `b13327088` | **F3.** `country.py` restored byte-identical to `e5fdd2fa8`: no `_panel_id_guard_from`, no `guard_from` branch, no `panel_ids_guarding` import. Guard-scope wording fixed in the `local_tools.panel_ids_guarding` docstring, `panel_ids.py`, the 2024-25 `mapping.py` and the `data_scheme.yml` comment. Also the `ihps_weight` raw-scale comment (F4). |
| `17b056039` | **F1.** Unit code 26 borrows its IHS5 factor (`BORROW_DESPITE_METRIC_LABEL`), applied in both the `allowed` mask and the completeness companion. Companion and `derivation_inputs` now route each contributor to its served key (`_rescaled_at_choke_point`, `_served_unit`). Four synthetic tests added. `derivations.yml` rule text updated. |
| `1ca84789a` | **F5 (Make).** A prerequisite-only rule adds `food_borrowing.py` and `borrow_dictionaries.json` for the 2024-25 food wave parquet. Stale comment fixed; new `test_gh1237` case. |
| `50113e8ac` | **CONTENTS.org.** New 2026-11-12 entry covering what 2024-25 serves, `ihps_weight` scale, stacked shares (#1704), the `check_panel_consistency` FAIL, code-26 counts, the guard residual and the Make change. Weights table gets a 2024-25 row. Dated amendments to the contradicted older text. |
| `538eec00d` | Corrects the unmeasured "as 2019-20's one such row" claim in `CONTENTS.org`, which supersedes that phrase in `17b056039`'s body. `derivations.yml` `validated_against` gains the post-F1 result (174,026 rows). |

### Tests changed for F3, with reasons
In `test_malawi_panel_id_guard.py`:
- `test_country_without_opt_in_keeps_legacy_mapping` was renamed `test_country_dataframe_fallback_uses_the_legacy_builder` and gained the opt-in case. Country no longer reads the key, so the test now pins the residual.
- `test_country_opt_in_uses_the_same_guard_as_the_committed_map_builder` was removed. It asserted the dropped branch.
- `test_guard_keeps_current_failure_cleanup_and_successful_retry` was removed. It asserted the guard inside the retry boundary, which is now development's code unchanged.
- `test_committed_map_path_does_not_recompute_even_when_opted_in` was kept. It exercises the unchanged committed-JSON branch.

In `test_gh970_no_json_make.py`:
- The two added `updated_ids['2024-25']` assertions were removed, because they asserted the route through the dropped branch.
- The 2024-25 fake frame stays, because every declaring wave needs one.

### Blast radius
GitNexus MCP failed to connect, so I swept references with `rg`:
- `_panel_id_guard_from` appeared only in `country.py` and the guard test.
- `panel_ids_guarding` appears in `local_tools.py` (definition), `Malawi/_/panel_ids.py`, and the guard tests.

## Results

**Hash footprint, development vs head, all 38 countries with a config:**
- `build_transforms_fingerprint`: 0 of 41.
- `Country._table_cache_hash`: 25 of 544, all Malawi. The other 37 countries: 0 of 519.
- `Wave._input_hash`: 150 of 2,258, all Malawi. Others: 0 of 2,108. 30 are `None` on both sides.
- Positive control, the old head `7735489cc`: 41/41, 544/544, 2,228/2,258.
- The code head `1ca84789a`, `50113e8ac` and the final head are identical (0 of 41 / 544 / 2,258 moved), since `CONTENTS.org` and `derivations.yml` are not build inputs.
- **Re-warm owed: Malawi only.**

**F1 delta, 2024-25, old head → new head:**
- `food_acquired` differs on exactly **4,031 rows**, all moving from `u='kg'` to `u='5 Litre Bucket(Chigoba)'`, with 0 other value changes. Every touched `u` is in {kg, bucket}.
- Stamped rows: 169,995 → **174,026**.
- Served `u='kg'` rows: 5,972 → 1,941.
- Borrowed bucket kilograms: 22,563 kg on 5,950 buckets, where density 1 gave 29,751 kg.
- **1,783 code-26 contributions have no admitted factor and are still served `u='kg'` at 5 kg** by the shared choke point in `malawi.py`. That choke point applies the same rule in every wave; how many 2019-20 code-26 rows reach it was not measured.
  - Read literally, the instruction "must not be ... rewritten to `u='kg'` at 5 kg" also covers these rows. Keeping them native would mean editing `malawi.py`'s choke point for every wave, plus another `test_gh1190` AST exemption.
  - **Coordinator's call.** It is disclosed in `CONTENTS.org` and the PR body.
- `food_quantities(kgs)` for 2024-25: 713,917 → 706,492 kg (−1.04%). Older waves move −0.001% to −0.010%.

**Derivation probe (job 41240388):**
- `derivation_inputs` returns 174,026 rows, a key bijection (0 / 0).
- Rederived: 174,026 of 174,026 (rtol 1e-9).
- Bucket rows alone: 4,031 served, 4,031 inputs, a bijection, 4,031 of 4,031 rederived.

**Older waves:**
- `sample`, `household_roster`, `food_acquired` and `food_expenditures` are frame-equal to development on every pre-2024 wave. The new columns are empty there.
- `sample` is frame-equal between the old and new heads in all waves.
- `food_quantities(kgs)` total kg, development → head: 2004-05 +1.39%, 2010-11 +0.09%, 2013-14 +0.42%, 2016-17 +0.21%, 2019-20 +0.07%. This is the pooled ladder, GH #1487.

**2024-25 sample:**
- 14,836 rows = 12,888 `cs-24-` + 1,948 retained.
- Raw weight means: IHS6 `hh_wgt` 383; retained `wt_cross` 1,258; `wt_long` 2,531.
- API `weight` / `panel_weight` mean 1.
- `ihps_weight`: 2,763 rows, raw mean 1,784.6 in the API as well.

**Stacked shares (API, panel share of rows / weight):** 2016-17 16.8% / 59.0%, 2019-20 21.7% / 62.8%, 2024-25 13.1% / 33.2%.

**`check_panel_consistency`:**
- Head: 5 pass, 2 warn, 1 fail. The fail is `panel_ids_targets_exist`, 1948/7634. The new warn is `attrition_monotonic` (2019-20, 2024-25).
- Development: 7 pass, 1 warn.

**`make panel-ids`:** byte-identical at both the code head and the final head. sha256 `01f98fd6…` / `173b1be8…` equal the committed blobs.

**Labels (served 2024-25):**
- `Other (specify)` is the only `j` not among the Preferred Labels (927 rows).
- `Bundle` (unit 74): 1,414 rows.
- Item 119 `Grain Meal (Gramill)`: 5,324 rows.
- The full list is in `PR_BODY.md`.

## Jobs

All ran on `savio4_htc`, excluding n0035, with 8 cores and 64 GB:
- Every clone was checked out at a full SHA, with the venv image mounted.
- Every library invocation ran with a private `LSMS_DATA_DIR` / `XDG_CACHE_HOME` / `TMPDIR` / `MPLCONFIGDIR` / `LSMS_ISSUES_LOG`.
  - The build, probe and hash scripts asserted the import identity and the private `data_root()`.
  - Pytest and `make panel-ids` inherited that environment without asserting it. The `make` log prints the job's own clone.
  - Job 41240388's pytest used the new arm's root, warm from its own cold build at the same SHA.
  - The compare scripts import no `lsms_library`.
- BLAS threads were pinned and `LSMS_BUILD_WORKERS=1` was set. Pytest ran with `-n 8 --dist=loadfile`.
- No `dvc` CLI was run.
- Every clone ended with 0 in-tree parquets and a clean `git status`.

| job | head | what | result |
|---|---|---|---|
| 41240388 | `1ca84789a` (and base, old) | Hash probe (3 arms). Cold builds (3 arms): sample, roster, food_acquired, food_quantities(kgs), food_prices, food_expenditures, check_panel_consistency. Probes, compare, `make panel-ids`, targeted pytest (52 files) and `test_sample -k Malawi`. | All rc=0. Pytest: **2,503 passed, 1 skipped** (`LSMS_EXPECTED_ROOT` identity test), 0 failed. Sample: **16 passed**. |
| 41242295 | `50113e8ac` (and base) | Hash re-probe; `make panel-ids`; served-label check; doc/drift, panel and borrowing selection (14 files). | All rc=0. **841 passed**, 0 failed. |

| 41242500 | `538eec00d` (and base) | Same as 41242295, at the final head. | hash re-probe identical to the code head (0 / 41, 0 / 544, 0 / 2,258 moved; development vs this head 0 / 41, 25 / 544 Malawi only, 150 / 2,258 Malawi only), `make panel-ids` byte-identical, and the same 14-file selection: **841 passed**, 0 failed. |

## Not done
- **Full offline suite and `pr-data-validation`.** Neither was run on this head.
- **Malawi re-warm.** Not performed.
- **F7 wording at the PR level.** Covered only where I touched text: the `mapping.py` and #894 notes are qualified.
- **F6 `aggregate_short.yml` `Gramil` entry.** Disclosed, not added.
- **`discover_waves` held-state.** Not checked.
- **2019-20's 10 re-mints.** Left alone, as ruled.

## Paths
- PR body: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/1604-final/PR_BODY.md`
- Scripts, logs, results and frames: `/global/scratch/fsa/fc_jevons/ligon/burst/isa-sm-20261112/reports/1604-final/{scripts,logs,results,frames}`
- Worktree: `/local/job41201840/worktrees/pr1604`
