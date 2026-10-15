VERDICT: pass-with-fixes -- the three code changes do what the commits say, every number reproduces, 0 of 170 cache hashes move against the merge-base and the full suite is green; the fixes needed are documentation only (a Guatemala doc now false under (c), the new lever missing from CLAUDE.md, and one residual item the live issue body does not carry).

Reviewer: red-team, job 39923182, own clone of `fix/labels-relabel-787` @ 6922c077a, private `LSMS_DATA_DIR` pre-seeded warm with Uganda/Malawi/Tanzania. Base comparisons were run in scratch worktrees of `origin/development` (15 commits past the branch point) AND of the merge-base `2ed85b49b`; the merge-base is the right comparator for this branch and is what every "before" below means unless stated. Import identity asserted on every run (`lsms_library.__file__` under the tree being tested). `git merge-tree --write-tree origin/development HEAD` is conflict-free. Diff against the merge-base is exactly the four files the report names.

## 1. Refutations (claim -> what I ran -> result)

1. **"(a) 'Preferred Label' is now a deliberate no-op; warm Uganda `food_expenditures(labels='Preferred Label')` frame-equals the default read."** Ran `warm.py` on branch and merge-base. Branch: `assert_frame_equal` passes, and the SAME object is returned. **HELD**, with one unstated detail: on the merge-base the same call was NOT frame-equal to the default -- same rows and total (247,049 / 726,299,305) but a different row order, because the accidental identity rename still went through the `reaggregate` groupby and re-sorted. The branch skips that sort. Content-equal before and after; row order changed. Not a defect.
2. **"(b) Malawi `labels='2004-05'` -> 32 of 147 j unrenamed on 42,206 rows (6.2%), 1 LabelPassThroughWarning; row count and total unchanged."** Branch: `n_unmatched=32, n_values=147, rows=42,206, row_share=0.0619`, 1 warning, 681,980 rows, total 281,093,686. Merge-base: same rows/total, no attrs, no warning. **HELD.** The warning is attributed to the caller's line (`warm.py:13`; `feature.py` on the Feature path) -- the `stacklevel=4` claim **HELD**.
3. **"Uganda/Malawi 'Aggregate', Tanzania 'FTC' -> 0 unmatched, silent."** All three return no `labels_unmatched` key and zero pass-through warnings on the branch, with row counts/totals identical to the merge-base (Uganda 242,043 / 726,299,305; Malawi 660,968 / 281,093,686; Tanzania 196,291 / 458,253,625). **HELD.**
4. **"(c) Uganda `labels='Code'` served `j` as int64; now `LabelUnavailableError`."** Merge-base: 247,049 rows, `j` dtype int64, 130 distinct. Branch: `LabelUnavailableError` naming the NUMERIC column and listing `['Aggregate Label', 'Nutrition Label']`. Tanzania `labels='Code'` likewise (merge-base served int64 with 60 distinct j vs 59 under 'FTC'; branch refuses). **HELD.** Config-only sweep confirms dtypes: Serbia `proizvod` int64, Guatemala `FCT code` float64, EthiopiaRHS `1989` float64, Mali `Code` str (still accepted, as claimed). Bool and nullable `Int64` columns are also refused (synthetic); a `category` column is accepted.
5. **"Contract B untouched; a Feature call degrades with one aggregated warning."** `Feature('food_expenditures')(['Uganda','Malawi','Tanzania'], labels='Code')` on branch: 0 rows, one aggregated `labels_unavailable` warning naming all three, same shape as `labels='Bogus'` on the merge-base. **HELD.** (An earlier KeyError in my own probe was my script reading a `country` level off the empty frame, not the branch.) `labels='Aggregate'`: Malawi+Uganda kept, Tanzania dropped with one warning, 903,011 rows -- identical on both trees.
6. **"0 of 191 probed hashes moved."** My probe: `Country._table_cache_hash` for every table of Uganda, Malawi, Tanzania, Serbia, Guatemala, Niger, Nigeria, GhanaLSS plus five `build_transforms_fingerprint` values = 170 hashes. Branch vs merge-base: **0 moved. HELD.** Branch vs `origin/development`: 27 moved, all Uganda -- and `origin/development` vs merge-base moves the same 27, so those are development's own Uganda commits (`uganda.py`, `data_scheme.yml`, `crop_production.py`), not this branch.
7. **Config-only sweep claims** (Niger `Original Label` 153 conflicting keys; Uganda `Aggregate Label` 1 conflict = `Cassava (flour)` -> `Cassava`/`Cassava (flour)`; Nigeria 1; GhanaLSS has `Aggregate Label` 0 conflicts; 5 of 22 food countries carry an Aggregate column). All reproduced exactly. **HELD.**
8. **"Serbia CONTENTS.org note corrected and true."** `proizvod` is int64 in `Country('Serbia').categorical_mapping['harmonize_food']`, and the branch refuses it, so the corrected sentence is accurate. **HELD.** But see Scope finding 2: Guatemala's `categorical_mapping.org:75` makes the identical now-false claim for `labels='FCT code'` and is mentioned nowhere in the report.
9. **Docstring: "Ethiopia `labels='2011-12'` left 51 of 76 served foods unrenamed, carrying 46.6% of expenditure."** NOT re-measured: Ethiopia is not in the private root and the report says so too. The figure is lifted verbatim from the #787 issue thread (two independent comments quote it). Unverified here, correctly attributed.
10. **New tests fail on the old code?** Copied the branch's `tests/test_food_labels.py` into the merge-base worktree (shimming the two new import names). 6 of the 10 new tests fail there (`preferred_label_is_a_deliberate_noop`, `reports_pass_through_in_attrs_and_warns`, `pass_through_is_fatal_under_strict_lever`, `report_survives_reaggregate_attrs_copy`, both `refuses_numeric_target` params). 4 pass on both trees and are negative controls by construction (`suffixed_and_bare_spellings` -- the bare fallback already resolved `'Aggregate Label'`; `fully_covered_axis_is_silent`; `pass_through_ignores_null_j`; `string_typed_code_column_is_still_accepted`). The discriminating tests exist for every behaviour change.

## 2. Claimed vs measured

| claim | claimed | measured (branch) | measured (merge-base) |
|---|---|---|---|
| Uganda `food_expenditures()` rows / distinct j / total | 247,049 / 130 / 726,299,305 | same | same |
| Uganda `'Preferred Label'` == default | frame-equal | frame-equal, same object | same content, different row order |
| Uganda `'Aggregate'`,`'Aggregate Label'` | 242,043 / 76 / 726,299,305, no attrs | same | same |
| Uganda `'Code'` | LabelUnavailableError | LabelUnavailableError | 247,049 rows, j int64 |
| Malawi `'2004-05'` unmatched | 32 of 147, 42,206 rows, 6.2%, 1 warning | 32 / 147 / 42,206 / 6.19%, 1 warning | no report, 0 warnings; rows/total identical |
| Malawi `'Aggregate'` | 660,968 / 65 / 281,093,686 | same | same |
| Tanzania `'FTC'` | 196,291 / 59 / 458,253,625 | same | same |
| Tanzania `'Code'` | LabelUnavailableError | LabelUnavailableError | 196,293 rows, j int64, 60 distinct |
| hashes moved | 0 of 191 | 0 of 170 vs merge-base | (27 Uganda vs development = development's own) |
| Niger `Original` conflicts | 153 | 153 | n/a (config) |
| Uganda / Nigeria `Aggregate` conflicts | 1 / 1 | 1 (`Cassava (flour)`) / 1 | n/a |
| Aggregate-capable countries | 5 of 22 | 5 | n/a |
| Serbia `proizvod` / Guatemala `FCT code` / Mali `Code` dtype | int64 / float64 / str | int64 / float64 / str | n/a |
| new tests fail on old code | (not stated) | 6 of 10 fail on merge-base; 4 negative controls pass on both | |
| grain / null-index / null-read reports | unchanged | identical counts on every call both trees (grain 0; null-index 1/2/3 cumulative, all pre-existing `v`/`u` NaN keys; null-read 0); index unique on every result | |

## 3. Regression

Full suite on the branch, verbatim recipe (`OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1 MKL_NUM_THREADS=1 LSMS_BUILD_WORKERS=1 pytest tests -n 40 --dist=loadfile -q`), log at `$STAGE/pytest_full.log`:

    4697 passed, 166 skipped, 3 xfailed, 1 xpassed, 860 warnings in 991.20s (0:16:31)

`bench/feature_audit/scan.py --features food_expenditures food_acquired --countries Uganda Malawi Tanzania` (`$STAGE/scan.jsonl`): 418 records, 0 skips, 0 fail/error. All `warn` records are pre-existing audit traffic -- 277 unclosed-file ResourceWarnings from the hasher, Site Q quantity screens, Site I NaN keys, Tanzania cluster_features grain reports, population heterogeneity, and the Contract B drop of Tanzania under `labels='Aggregate'` (the `reaggregate_conservation` 31% total change is that drop, not a relabel loss). No `LabelPassThroughWarning` fired in the scan (Aggregate crosswalks are fully covered on Uganda/Malawi).

`find lsms_library/countries -name '*.parquet'` -> 0. No file under the shared `~/.local/share/lsms_library` newer than my first probe. `git status --short` empty at the end; scratch worktrees removed and pruned.

## 4. Scope findings

1. Diff vs merge-base is exactly `country.py`, `tests/test_food_labels.py`, `docs/guide/data-methods.md` (generated, pinned by `test_api_discoverability`), `Serbia/_/CONTENTS.org`. No baseline/golden/blessed/lockfile/dependency edits; no `xfail`/`skip`/pin moved (grep of the diff is empty). Merges cleanly onto today's `origin/development`.
2. **`Guatemala/_/categorical_mapping.org:75`** says "`labels='FCT code'` is the spelling that resolves". Under (c) it resolves to a refusal (float64). The report applied the hashed-input reasoning to Serbia's `categorical_mapping.org` and corrected Serbia's `CONTENTS.org`, but Guatemala -- the same case -- is not mentioned anywhere. Guatemala's `CONTENTS.org` has no such sentence, so there is nothing free to correct; the stale line should be named in the follow-up/issue text.
3. The new lever `LSMS_LABELS_STRICT` appears in `country.py` and the generated `data-methods.md` only; CLAUDE.md's lever prose (`LSMS_GRAIN_STRICT` / `LSMS_READ_STRICT` / `LSMS_QUANTITY_STRICT`) and `docs/guide/label-selection.md` do not mention it. `label-selection.md:210-211` still shows the `.to_dict()` `rdict` (report acknowledges this).
4. Residual drafted? Yes -- REPORT.md drafts a three-item residual. The live #787 body (narrowed by Sue 2026-10-14) carries only two of them (decision (d), per-country curation). The third, real and measured here -- `attrs['labels_unmatched']` dies at `Feature()`'s concat whenever two or more countries are kept (my two-country `labels='Aggregate'` Feature frame has no such key; the one-country `'2004-05'` frame keeps it only because concat of one frame preserves attrs) -- is in neither the issue nor `feature._ATTRS_CARRIERS`.

## 5. Residual risks and what a human must decide

- **Decision (d)** stands exactly as the issue states it: a Preferred Label mapping to >1 distinct target is still last-row-wins (Niger 153, Senegal 134, Burkina 72; Uganda `Cassava (flour)` under Aggregate). Not touched, as scoped.
- **Behaviour change the commit does not state:** `labels='Preferred Label'` used to come back re-sorted by the reaggregate groupby; it now comes back in the input's row order (same content). Anyone positionally comparing to an old output sees a reorder.
- **Edge cases, corpus-inert:** `'Preferred Label Label'` strips one suffix, falls into the old duplicate-column path and emits a spurious "all j unrenamed" `LabelPassThroughWarning`; a `bool` crosswalk column is refused as "NUMERIC". Nobody types either; neither blocks.
- **`labels_unmatched` does not survive multi-country `Feature()`** (needs an `_ATTRS_CARRIERS` entry + `merge_attrs`). The per-country warning still fires, so the signal is not lost, only the structured report.
- The Ethiopia 51/76 / 46.6% docstring figure is quoted from the issue, not re-measured here.
- Cache cost after merge: this branch moves nothing, but development's own Uganda commits re-warm Uganda regardless; do not attribute that to #787.

## 6. Recommended follow-up commits (docs only; no code change required)

1. `CLAUDE.md`: add `LSMS_LABELS_STRICT` (and `LabelPassThroughWarning` / `attrs['labels_unmatched']`) to the `labels=` paragraph and beside the other `*_STRICT` levers, with the Malawi 32/147 measurement.
2. `docs/guide/label-selection.md`: replace the `.to_dict()` `rdict` snippet (§Design note 1) with the filtered dict comprehension; add the accepted-value surface and the numeric refusal.
3. GH #787 body: add the third residual item (`labels_unmatched` carrier in `feature._ATTRS_CARRIERS`) and a note that `Guatemala/_/categorical_mapping.org:75` and `Serbia/_/categorical_mapping.org:68` document a numeric `labels=` target that now refuses (hashed inputs; fix when those countries next re-warm).
4. Optional, `.claude/skills/add-feature/food-acquired/aggregate-labels/SKILL.md`: "4 of 16" -> GhanaLSS now curates `Aggregate Label` (5 countries), per the sweep.
