VERDICT: pass-with-fixes -- every claimed number in the follow-up commit and report reproduces, served data is unchanged, the suite is clean apart from a failure the pending merge with development cures, and the stash-probe matches (6/15 vs the pre-follow-up commit, 15/15 vs development); but the new docstring's advice for two-round folders does not work on the path it documents (a fixture shows the report never reaches the wave stamp), the `dropped`->`collapsed_rows` rename is pinned by no test, and the CONTENTS.org counts (11 groups, 4 replays) go stale the moment the branch is merged with development's #848 (measured 12 and 5 on the merged tree).

Reviewer: red-team element `868-followup`, job 39922657, branch `fix/868-followup` at `9dfb83394`; `origin/development` at `c7377eca4` (merge-base `340e30f1e`; against the merge-base the diff is exactly the 14 files the diffstat names -- the 78-file `git diff --stat origin/development..HEAD` is development's own bookkeeping plus #848/#811 since the merge-base). A trial merge of development into the branch is clean (`git merge-tree`: no conflicts) and a real merge in a scratch worktree (`/local/job39922657/merged`) was used for the "after merge" measurements below. Private root seeded with pre-fix Uganda + Togo parquets; every build ran against copies of it under `/local/job39922657/`; nothing was written to the live root, to `~/.local/share/lsms_library`, or in-tree (`find lsms_library/countries -name '*.parquet'` is 0 in all four trees). Import identity asserted in every run.

## 1. Refutations

**R1. Stash-probe: "6 moved (Uganda x4, Togo x2); no ADDITIONAL hash beyond what the base moves" -- held.** 15 values probed in three trees (`origin/development`, `b471342e7` = pre-follow-up, HEAD): vs pre-follow-up 6/15 moved, exactly Uganda {crop_production, cluster_features, household_roster, sample} and Togo {cluster_features, sample}; Malawi/Ethiopia/Kosovo/Albania, all four `build_transforms_fingerprint` and Kosovo 2000 housing `Wave._input_hash` byte-identical. vs development 15/15 moved. Command: `probe.py` with `PYTHONPATH`+`LSMS_COUNTRIES_ROOT` per tree, cd'd into the tree. Nit: the commit message's mechanism ("fold its file hash") is wrong and the report's (`framework_imports_fingerprint` over the imported callables' source) supersedes it; the report says so.

**R2. CONTENTS.org item 3, "in-process hook unchanged: 8 GrainConflictWarning, 0 GrainCollapseWarning" -- held.** Branch, cold build of Uganda `cluster_features` in a fresh seed copy: 8 `GrainConflictWarning`, 0 `GrainCollapseWarning`, 8 `cells_blanked` reports filed, 2005-06 `collapsed_rows` = 2,801 (the figure the commit message quotes).

**R3. "a cold `make` build of crop_production emits the child's GrainConflictWarning on stderr AND the parent's GrainCollapseWarning replay in-process (4)" -- held on the branch, STALE after the merge.** Branch cold build (all 7 wave scripts re-ran, 17:57:07-17:58:05): parent 4 `GrainCollapseWarning`, child stderr 4 `GrainConflictWarning` lines, 4 reports = 6+1+3+1 = 11 groups (Unit_sold 9, Condition_sold 2). On the MERGED tree (branch + development's #848, which wires the 2018-19/2019-20 season-B slot 2): parent **5**, child stderr **5**, a fifth report for 2018-19 (1 group, `Condition_sold`, Beans/Kg/fresh_in_pods/season B). #848's own CONTENTS.org text already records "GrainConflictWarning groups 11 -> 12", but the #868 section this branch adds still says "The 11 groups themselves are unchanged" and "(4)"; the textual merge is clean so nobody reconciled them.

**R4. "What the reducer returns is unchanged" -- held.** Byte-equal under `assert_frame_equal`: Uganda `cluster_features` branch-cold vs development (4,805 rows, 3,902 nulls); Togo `cluster_features` branch cold and branch warm vs development (540 rows, 10 nulls); Uganda `crop_production` branch cold vs branch warm (130,606 rows, 452,306 nulls); MERGED-tree `crop_production` warm vs development (133,331 rows, 459,038 nulls -- the +2,725 rows over the branch are #848's, confirmed by t: only 2018-19 and 2019-20 differ). Equal as values but NOT byte-equal: Uganda `cluster_features` branch-warm vs branch-cold and vs development, and merged-warm vs development -- the warm read serves the `v` index level as pandas `str` dtype where the cold read serves `string`; every value identical as strings. That artefact pre-exists (development's own cold vs warm read shows it) and is not this branch's. Index unique everywhere; `null_read_reports` / `null_index_reports` counts identical on both sides (cluster_features 2/0, crop_production 1/1).

**R5. The issue's symptom on development -- reproduced and gone.** Development, second (warm) read in a fresh process: 0 grain warnings, `grain_reports()` empty for both tables. Branch warm read: `cluster_features` 8 `GrainCollapseWarning` / 8 reports, `crop_production` 4 / 4, all `from_cache=True`, `kind=cells_blanked`, `destroyed=nan_key_rows=0`, `collapsed_rows` present, `dropped` absent. Stamps: 8 wave + country stamps for cluster_features, 4 of 7 wave parquets + country for crop_production, none carrying `dropped`. `LSMS_GRAIN_STRICT=1` warm read raises `GrainCollapseError` ("BLANKED") for both tables.

**R6. "No formatter prints `dropped`" -- held.** `_format_grain_report`'s `cells_blanked` branch returns before the `report.get("dropped")` test (country.py:6325-6347). A legacy stamp carrying `dropped` (any root warmed on `dda202b51`) still replays as BLANKED under strict (checked). The commit-message sentence about `bench/grain_census_871.py` is inaccurate but harmless: that script calls `_audit_index_collapse` directly and can never see a `cells_blanked` report.

**R7. The rename is UNPINNED -- refuted as a test change.** HEAD's `tests/test_gh868_reduce_to_agreed_ledger.py` run against the pre-rename tree (`b471342e7`, which still emits `dropped`): 16 passed, 1 skipped, 1 failed -- the failure is a path artefact of running the file from outside its repo, not the rename. Every `collapsed_rows` in the file is in a hand-built input dict; no test asserts the key on a report the reducer filed.

**R8. Follow-up item 1's docstring advice does not work on the path it documents -- NEW finding.** Both new docstrings say a two-round folder "must pass `wave=` explicitly to the reducer to name the round". But `_wave_grain_audit` (country.py) stamps a wave parquet only with reports whose `wave == Wave.year` (the folder). Fixture (`/local/job39922657/tmp/test_round_wave.py`: folder `2010-11`, YAML-path hook calling `collapse_to_cluster_grain(df, on_conflict='na', wave='2010Q3')`): the report files with `wave='2010Q3'`, the wave parquet is written with **no** `lsms_grain_audit`, and the L2-wave hit replays nothing -- exactly the loss #868 exists to close. No live caller today (Nigeria's `cluster_features` is Site 2, not the reducer; Uganda's PP/PH-free). Doc-only in effect, but the follow-up's whole brief was to make this guidance correct, and it now recommends the one spelling the stamp filter drops.

**R9. Report: "`test_every_poetry_install_passes_no_root` fails identically on development today" -- stale.** It passes on `origin/development` (`24cd065b0` fixed `ci.yml`) and on the merged tree (4 passed each); it fails only on the branch (1 failed, 3 passed). The merge cures it; nothing for this branch to do.

**R10. Merge with development is clean and the stamp survives #848's edits to the same scripts -- held.** Merged `2018-19`/`2019-20` wave scripts both keep `to_parquet(..., grain_audit=grain_reports('Uganda','crop_production'))`; `uganda.py` keeps the explicit `country/table/wave` kwargs. Targeted set on the merged tree: 541 passed.

**R11. Residual-drafting rule.** The report neither states that #868 is fully resolved nor drafts a residual. The issue's three asks (file+stamp+replay; strict through the shared predicate; a round-trip test) are met. R8 is the residual; a draft is in section 5.

## 2. Claimed vs measured

| claim | measured |
|---|---|
| stash-probe vs HEAD: 6 moved (Uganda x4, Togo x2), 9 identical | 6/15 moved, same six |
| base moves "all of them" vs development | 15/15 moved |
| in-process cluster_features cold: 8 GrainConflictWarning, 0 GrainCollapseWarning | 8 / 0 |
| crop_production cold make: child 4 GrainConflictWarning + parent 4 GrainCollapseWarning | branch 4 + 4; **merged tree 5 + 5** |
| 11 blanked groups (6+1+3+1) | branch 11; **merged 12** (+1 in 2018-19, Condition_sold) |
| 2005-06 cluster_features `collapsed_rows` = 2,801 | 2,801 |
| reducer output unchanged | 4 pairs byte-equal, 3 pairs value-equal with the pre-existing `str`/`string` warm dtype artefact (R4) |
| targeted (`tests/test_gh868_*.py tests/test_gh323_*.py tests/test_uganda_323*.py`): 229 passed, 2 skipped | 229 passed, 2 skipped, 77 warnings in 82.82s (HEAD); superset on the merged tree 541 passed |
| report: country.py docstring edit moves 0 hashes, all 6 moves come from build_transforms.py | 0/15 moved with only `country.py` applied on top of `b471342e7` |
| full suite: 1 failed (no_root), 5300 passed, 164 skipped | 1 failed (same test), 4623 passed, 175 skipped, 3 xfailed, 1 xpassed. The pass/skip split tracks the warm root: the base-branch review's two-country root gave 4571/189, this two-country root 4623/175, the fix agent's root held ~36 warm countries (38 entries in its `data/`) and gave 5300/164 |
| no_root test "fails identically on development" | passes on development and on the merge |
| scan: 222 warn, class C, 24 replayed findings | 222 warn, 0 fail/error, 24 replayed (16 cluster_features, 8 crop_production) |

## 3. Regression

Full suite, HEAD, recipe verbatim except `-n 32` (idle 40-core node, load 1.1 at start), own copy of the seeded root:
```
FAILED tests/test_no_editable_install.py::test_every_poetry_install_passes_no_root
1 failed, 4623 passed, 175 skipped, 3 xfailed, 1 xpassed, 960 warnings in 1084.39s (0:18:04)
```
0 OpenBLAS thread-init lines, no worker lost. The failure is development's `ci.yml:96` lacking `--no-root`, already fixed on development (R9). The xpassed test is `tests/test_pakistan_cluster_features.py` (pre-existing GH #740 xfail; `7 passed, 1 xpassed` when run alone on HEAD).

Merged tree (branch + current development), `tests/test_gh868_* test_gh323_explicit_reducers test_gh323_togo_cluster_features test_uganda_323* test_grain_collapse* test_uganda_crop_condition test_uganda_sold_unit test_gh842_uganda_harvest_unit test_crop_kg_factor test_schema_consistency test_feature_canonical_index test_build_transform_hash`, `-n 6 --dist=loadfile`, own root copy:
```
541 passed, 54 warnings in 330.37s (0:05:30)
```

`bench/feature_audit/scan.py --features crop_production cluster_features --countries Uganda Togo` (branch, warm private root):
```
by status: {'warn': 222}
candidate findings (warn/fail/error) by severity class: {'C': 222}
```
214 `runtime_warning` (24 are the replayed `reduce_to_agreed` findings; the rest are pre-existing `unclosed file` ResourceWarnings), 3 `null_content`, 2 `index_levels_match_scheme`, 2 `has_household_index`, 1 `no_constant_columns` -- identical to the base-branch review.

## 4. Scope findings

- Against the merge-base: 14 files, all named by the brief or the base branch; the follow-up commit itself touches exactly the four files the task names. No baseline / golden / blessed / lockfile / dependency edit; no existing assertion weakened, skipped or xfailed; the two edited test literals are input dicts, not pins (R7).
- New tests (mandate item 6): the follow-up commit adds none; its two literal edits in `test_gh868_reduce_to_agreed_ledger.py` are hand-built inputs, not pins, and the file passes on the pre-rename code (R7). The base branch's 24 tests were already shown to fail on development by the earlier review (23 of 24).
- `git status --short` empty in the clone before and after; all scratch under `/local/job39922657/` (worktrees removed at the end).
- The "commit message" half of task item 3 was carried by the follow-up commit's message (the base commit cannot be reworded without a force-push); reasonable, and stated as a decision.

## 5. Residual risks and what a human must decide

1. **Cache cost (accepted by the 2026-10-14 ruling, restated for the merge note):** merging this branch invalidates **every** L2-country hash and **every** L2-wave input hash in the corpus -- countries the branch never touches included (Malawi, Ethiopia, Kosovo, Albania measured) -- so the next warm of any root rebuilds every table cold from L1. The follow-up commit adds nothing to that bill.
2. **R8 residual, to be filed on #868 (draft):** "A `reduce_to_agreed(on_conflict='na')` call reached through the framework context in a two-round wave folder (Nigeria PP/PH layout) cannot name its round: the context supplies the folder (`Wave.year`), and a report filed with an explicit `wave=<round>` -- the spelling `_resolve_build_context` / `_replay_grain_audit` now recommend -- is excluded from the YAML L2-wave stamp by `_wave_grain_audit`'s `wave == folder` filter, so it is not replayed on an L2-wave hit. No live caller today (Nigeria's `cluster_features` is Site 2). Fix: have `build_context` put the folder on the report (e.g. `wave_folder`) and have `_wave_grain_audit` filter on that instead of on `wave` (note: filtering on the frame's `t` values would NOT work -- a YAML-path frame's `t` is stamped as `Wave.year` by `check_adding_t`, so its `t` set is the folder too), with the fixture in `/local/job39922657/tmp/test_round_wave.py` as the pin; until then the docstrings should say the explicit-`wave=` path is stamped only by an out-of-process script that hands the whole ledger to `to_parquet`."
3. **Post-merge doc drift (R3):** once merged with #848 the Uganda `CONTENTS.org` #868 section's "11 groups" and "(4)" are 12 and 5. Not a defect of the branch, but the merge note or a one-line follow-up should reconcile it so the two sections of the same file agree.
4. Warm reads of Uganda `cluster_features`/`crop_production` under `LSMS_GRAIN_STRICT=1` now raise (intended; unchanged from the base-branch review).

## 6. Recommended follow-up commits

1. `tests/test_gh868_reduce_to_agreed_ledger.py::test_na_branch_files_one_cells_blanked_report_under_the_named_cell`: add `assert 'dropped' not in rep and rep['collapsed_rows'] == rep['rows'] - rep['groups']` so the rename is pinned (today it passes on the pre-rename code).
2. `lsms_library/country.py` `_wave_grain_audit` + `build_transforms._cells_blanked_report`: stamp a `wave_folder` field from `build_context` (the folder the framework is building) and filter the wave stamp on it, leaving `wave` free to name the round; add the R8 fixture as a test. (Filtering on the frame's `t` values is not a fix: on the YAML path `t` is `Wave.year`, the folder, so the fixture would still fail.) Alternatively, amend the two docstrings to state the limitation and file the residual above. Either way the docstring and the filter must agree.
3. `lsms_library/countries/Uganda/_/CONTENTS.org` (after merging with development): "11 groups" -> "12 (the twelfth is #848's 2018-19 season-B Condition_sold conflict)", "(4)" -> "(5)".
4. Merge note: state the corpus-wide, both-tier re-warm (item 5.1) and that `test_every_poetry_install_passes_no_root` is green on the merge.

Artefacts: `/local/job39922657/out/` (probe JSONs, cold/warm logs and frames, scan.jsonl), `/local/job39922657/pytest.log`, `/local/job39922657/merged_pytest.log`, `/local/job39922657/tmp/test_round_wave.py` -- job-local, destroyed with the job.
