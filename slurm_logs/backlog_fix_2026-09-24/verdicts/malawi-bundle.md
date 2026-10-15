VERDICT: pass-with-fixes — every before/after figure in the report and commit messages reproduced in my private root (or reconciled to a stated counting rule), the branch touches only Malawi files, the live cache and other countries' hashes are untouched, and 561 tests pass; what remains is two prose follow-ups and one human decision about the `shell_not_applicable`-vs-S/U class (464 sales, 40.8M MWK, mostly Tobacco and Cotton) that the branch suppresses.

Reviewer: red-team element `malawi-bundle`, Slurm job 39199806, clone at `13da05b11` (`fix/malawi-bundle`), `origin/development` at `b5103b64f`, merge-base `242573697`. Package identity asserted (`/local/job39199806/clone/lsms_library/__init__.py`). GitNexus MCP did not connect; call-site sweeps were done with `rg`/`grep` (declared substitution). `gh issue view` printed nothing in this environment; issues were read through `gh api`.

## 1. Refutations (claim → what I ran → what I got)

- **"ag_i02c wired; 642 of 11,321 attached sales contradicted, Groundnut 375 / Rice 97 / Maize 62 / Soyabean 36" — HELD under the per-harvest-row rule.** I rebuilt the sale frames from the raw Module I/Q files with the branch's `_sale_block(condition=...)` and joined them to the PRE-FIX wave parquets (`/local/job39199806/after_cp.py`). Counting attached harvest rows whose every sale row of the same `(i, crop, u)` key contradicts: **174 / 48 / 158 / 262 = 642**, exactly the report. Counting candidate PAIRS (a key with both an S and a U sale contributes two): 661 of 11,520 (Groundnut 379, Rice 109, Maize 65, Soyabean 36). Same phenomenon, different rule; the ordering by crop is identical. After the rebuild the per-row count is **0 / 0 / 0 / 0**.
- **"Attached before → after 5,205 → 4,903, 1,774 → 1,715, 4,564 → 4,317, 7,131 → 6,628; harvest rows unchanged 33,420 / 12,616 / 37,170 / 51,872" — HELD exactly**, from the snapshot parquets (before) and the cold rebuild in my private root (after: `lsms_library.cli cache clear --country Malawi` against `LSMS_DATA_DIR`, then `Country('Malawi').crop_production()`; crop_production rebuilt in 30 s, food_acquired in 179 s).
- **"Basis-mismatch sales / MWK: 303 / 4,922,307; 59 / 969,200; 247 / 20,236,770; 503 / 37,981,880; ambiguity tally 0 in every wave" — HELD exactly.** The four `SaleAttachmentWarning … CONTRADICTS` lines in my rebuild log carry those numbers; no `AMBIGUOUS` warning fired. Cross-check: total `Value_sold` on the table fell 1,176,153,003 → 1,112,045,346 MWK, a drop of 64,107,657, which is the four tallies (64,110,157) minus the 2,500 MWK #854 case re-attached.
- **"The #854 case (2010-11 `303041430013`, Maize, 50 kg Bag) now attaches to the shelled row" — HELD.** Rebuilt parquet: shelled row Quantity 30, Quantity_sold 1.5, Value_sold 2,500; unshelled row NA.
- **"Module Q has no ag_q02c before 2016-17; where it asks, NA 527+94 / 1,087+172 vs S 49+6 / 133+18" — HELD exactly** (raw value counts, `raw_before.log`).
- **"0 (i, crop, u) keys mix an unsold all-NULL line with a real sale" — HELD** in all four waves. The CONTENTS.org gloss "the unsold lines have no unit" is slightly off: 2 unsold lines in 2010-11 carry a unit. Harmless (they mix with nothing).
- **"Groundnut j rows 188 / 225 → 0 in 2016-17 / 2019-20, now `Groundnut - Boiled (Vendor)`; early waves unchanged 4,031 / 3,937 / 1,937" — HELD exactly.** After undoing the relabel, an outer merge of before vs after `food_acquired` on `(t,i,j,u,s)` shows 0 differing Quantity or Expenditure cells; row count 971,531 both sides; index unique both sides.
- **"24 of 44 crop labels occur as a food label; 19 of 51 priced items as a food label, 6 as a crop label" — HELD exactly** (snapshot parquets; the 6 are Banana, Beans, Cabbage, Rice, Tea, Tomato).
- **"data_scheme.yml edits are comment-only" — HELD:** `yaml.safe_load` of base and branch files compares equal.
- **"Cache cost: one Malawi re-warm" — HELD.** `Country._table_cache_hash` for 12 Uganda/Tanzania/Niger tables is byte-identical under `origin/development` and the branch; every Malawi table's hash moves (`malawi.py`, `data_scheme.yml`, wave scripts). Rebuilt Malawi `crop_production` grades FRESH under the branch. The live cache at `/global/scratch/.../cache/lsms_library/Malawi/var/` has no file newer than 2026-09-22.
- **"454 passed" (fix agent's selection) — superseded by my own run**, see §3.

## 2. Claimed vs measured

| claim | claimed | measured | note |
|---|---|---|---|
| attached sales before (4 waves) | 5,205 / 1,774 / 4,564 / 7,131 | same | snapshot parquets |
| attached sales after | 4,903 / 1,715 / 4,317 / 6,628 | same | cold rebuild, private root |
| harvest rows | 33,420 / 12,616 / 37,170 / 51,872 | same, before and after | |
| both sides answered S/U | 11,321 | 11,498 keys with a single sale condition; 11,520 pairs | counting rule |
| contradictions before | 642 (174/48/158/262) | 642 per harvest row; 661 per pair | exact under per-row rule |
| contradictions attached after | 0 | 0 | |
| NA vs S/U before | 465 (127/11/89/238) | 464 distinct sales (125/11/89/239); 474 pairs | |
| basis-mismatch sales / MWK | 303/4,922,307; 59/969,200; 247/20,236,770; 503/37,981,880 | same | warnings in rebuild log |
| ambiguity tally after | 0 every wave | 0 (no AMBIGUOUS warning) | |
| Value_sold total change | not stated | −64,107,657 MWK = tallies − 2,500 | consistent |
| Groundnut rows 2016-17 / 2019-20 | 188 / 225 → 0 | same | |
| Groundnut - Boiled (Vendor) | 188 / 225 | same | |
| early-wave Groundnut rows | 4,031 / 3,937 / 1,937 | same | |
| crop labels in food | 24 of 44 | 24 of 44 | |
| priced items in food / crop | 19 of 51 / 6 | 19 / 6 | |
| Module Q ag_q02c counts | NA 527+94, 1,087+172; S 49+6, 133+18 | same | |
| unsold/sold key mixing | 0 | 0 | 2 unsold lines with a unit in 2010-11 |
| null-index NaN keys | not stated | identical before/after: crop_production plot_id 41, u 8,902; food_acquired u 441 | |
| grain reports / null_read reports | not stated | 0 / 0 after; snapshot carries no `lsms_grain_audit` stamp (before figure is parquet-level) | declared substitute |
| in-tree parquets | 0 | 0 | |
| other-country hashes | unchanged | 12/12 byte-identical | |

## 3. Regression

Recipe: `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 .venv/bin/python -m pytest <selection> -n 8 --dist=loadfile -q` (SLURM_CPUS_ON_NODE=8), output to `/local/job39199806/pytest.log`. Selection: `tests/test_malawi_shipped_factors.py tests/test_malawi_cassava_recode.py tests/test_malawi_unit_cleanup.py tests/test_gh323_malawi_cluster_key.py tests/test_gh323_malawi_gb_cartesian.py tests/test_gh637_malawi_first_sites.py tests/test_schema_consistency.py tests/test_feature_canonical_index.py tests/test_build_transform_hash.py tests/test_declared_spellings.py tests/test_canonical_spellings.py tests/test_null_index_guard.py tests/test_derivations.py`.

    561 passed, 66 warnings in 410.28s (0:06:50)

0 FAILED / ERROR lines. `bench/feature_audit/scan.py` not run: this is a country change, not a framework change (`Feature('crop_production')` still excludes Malawi per CONTENTS.org).

New tests on `origin/development` (branch test file copied into a worktree of `b5103b64f`, `PYTHONPATH` to that worktree, identity asserted): **5 of 6 fail** (`test_sale_block_reads_the_sale_side_condition`, `test_condition_disambiguates_a_split_plot_crop`, `test_a_contradicting_sale_is_not_attached_and_is_counted`, `test_not_applicable_against_shelled_is_a_contradiction`, `test_two_sales_compatible_with_one_row_are_ambiguous`). `test_unknown_on_either_side_is_compatible` **passes on development** — it is a behaviour-preservation test (development attaches regardless of condition), acceptable but non-discriminating.

## 4. Scope findings

- Three-dot diffstat matches the report: 9 files, all under `lsms_library/countries/Malawi/` plus `tests/test_malawi_shipped_factors.py` (appended class only; no existing assertion changed, no skip/xfail, no baseline/blessed/lockfile/dependency edit). `.coder/coverage/blessed.csv` has no Malawi crop_production/food_acquired rows to move.
- **Served-data change the task did not name:** the Aggregate Label of the raw nut in 2004-05 / 2010-11 / 2013-14 moved from `Groundnut, Vendor/Boiled` to `Groundnut, Whole`. Under `labels='Aggregate'` the 9,905 early-wave Groundnut rows now sit in `Groundnut, Whole` (4,031 / 3,983 / 1,961 incl. other Whole labels) and `Groundnut, Vendor/Boiled` is 272 / 442 in 2016-17 / 2019-20 only. Justified (leaving the raw nut in the vendor bucket would have left the Aggregate wrong) and recorded in CONTENTS.org, but it is a second correction riding the commit.
- The snapshot I was seeded with predates development's `f98f92171` (#832, `No.12 Plate` → `No 12 Plate`): 1,305 2013-14 `food_acquired` keys differ between before and after on that spelling alone. Development's, not this branch's (0 hits in the branch diff), and it does not confound the crop figures: every attached harvest row in the seed matched a branch-built sale frame on `(i, crop, u)` with 0 unmatched in all four waves.
- Nothing written outside my private `LSMS_DATA_DIR` (the regression tests warmed other countries there); `git status --short` empty; 0 in-tree parquets.

## 5. Residual risks and what a human must decide

1. **The `shell_not_applicable`-vs-S/U rule is a decision, and it carries most of the suppressed money.** Of the 64.1M MWK now withheld, the class where one side answered NOT APPLICABLE and the other S or U is 464 distinct sales / **40.8M MWK** (3,182,027 / 307,000 / 10,311,530 / 27,026,250), led by **Tobacco** (33 / 3 / 27 / 47) and **Cotton** (24 / 7 / 12 / —), then Groundnut, Sweet Potato, Soyabean. For a crop with no shell, "not applicable" and "shelled" describe the same physical quantity, so treating them as contradictory may over-suppress. The 642 genuine S-vs-U contradictions (23.3M MWK) are unarguable. @ligon should rule whether `shell_not_applicable` is a wildcard for crops that have no shell; that changes four wave tallies and CONTENTS.org, not the mechanism.
2. A sale is now attached per `(i, crop, u, condition_sold)`, so a household selling one crop both shelled and unshelled in one unit gets only the compatible portion on its harvest row. That is the documented rule; a consumer wanting the household-crop total needs `_sale_block`.
3. The Uganda-style two-code-scheme Cassava reconciliation the task mentioned does not apply to Malawi (the agent's finding, which I confirm: Malawi keeps `Cassava Flour` / `Cassava Tubers` apart; its cross-wave cassava recode is #869's `_PERENNIAL_RECODE`). No Malawi mapping change was needed.

## 6. Recommended follow-up commits (before merge, since the Malawi re-warm is already paid by this branch)

- `lsms_library/countries/Malawi/2010-11/_/crop_production.py` and `2013-14/_/crop_production.py`: update the docstrings (`qty=ag_i02a, value=ag_i03`, no S/U) to name `ag_i02c`. Prose-only, but it moves every Malawi hash again if done after merge.
- `lsms_library/countries/Malawi/_/CONTENTS.org`, "Checked, not assumed" paragraph: replace "the unsold lines have no unit, so they sit under `u = NULL`" with the measured fact (0 keys mix; 2 unsold 2010-11 lines carry a unit and collide with nothing).
- Optional: record in CONTENTS.org the per-wave MWK of the NA-vs-S/U class (above) beside the row counts, so the pending decision in §5.1 can be taken with money attached.

Artifacts: `/local/job39199806/{before.py,raw_before.log,after_cp.log,after_fa.py,navs_mwk.py,rebuild.log,pytest.log,base_newtests.log,hash_base.txt,hash_branch.txt}` (job-local scratch).
