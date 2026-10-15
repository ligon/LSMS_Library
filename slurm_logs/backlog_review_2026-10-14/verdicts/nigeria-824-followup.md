VERDICT: pass-with-fixes — every data claim on the branch reproduces to the row (Mali/Nigeria kg N with and without the withdrawn alias, Unit_sold counts, agreement rates, the raw→built decomposition, the W1–W3 fertilizer-type value labels), `transformations.py` is byte-identical to development, no pre-existing column or audit moves, and scope is exactly the five named files; what is missing is process: the report drafts no residual for GH #824 (Niger and the other twelve `Value_sold` declarers; a consumer of `Unit_sold`), and the two "report for filing" findings are described but no issue text is drafted or filed.

Reviewer: red-team element `nigeria-824-followup`, Slurm job 39922577, own clone of `fix/nigeria-824-followup` at `eeb445ebf` (merge-base with `origin/development` = `340e30f1e`; `origin/development` itself has since moved to `c7377eca4`, which is why a raw `git diff --stat origin/development..HEAD` shows 73 files — the merge-base diff is the 5 Nigeria/test files and nothing else). Private `LSMS_DATA_DIR` seeded with pre-fix Nigeria + Mali parquets; base worktree of the merge-base at `/local/job39922577/base` (removed at the end). Package identity asserted (`lsms_library.__file__` under the clone / under the worktree) at the start and inside every measurement script. Nothing committed, nothing pushed; `git status --short` empty; 0 in-tree parquets; 0 files in the live root or the mirror newer than my start marker.

## 1. Refutations (claim → what I ran → what I got)

**Alias removal returns Mali / Nigeria kg N to the development figures** — HELD exactly. `nitrogen_kg(Country(c, assume_cache_fresh=True).plot_inputs())` on the seeded warm `plot_inputs` parquets with branch `transformations`: Mali 906,361.8 kg N, matched 28,929 rows, 2,869 plots, `Other inorganic` (9,640 rows) in `unmatched_labels`; Nigeria 173,672.0, matched 10,014, 4,359 plots, `Other Inorganic Fertilizer` (170 rows) unmatched. Re-adding `'other inorganic fertilizer': 0.20` in-process reproduces the aliased figures: Mali 1,028,795.6 / 38,569 / 3,029; Nigeria 178,074.5 / 10,184 / 4,472. Δ = −122,433.8 and −4,402.5, as claimed. `git diff 340e30f1e..HEAD -- lsms_library/transformations.py` is empty.

**Nigeria 'Other Inorganic Fertilizer' provenance by wave (109 on the relabel, 36 + 25 literal)** — HELD: rows per `t` on the served `plot_inputs`: 2010Q3 66, 2012Q3 8, 2015Q3 35, 2018Q3 36, 2023Q3 25. `Nigeria/_/categorical_mapping.org:758` is the `harmonize_input` row `5 | Other Inorganic Fertilizer | other (specify) | Other (specify) | 4. OTHER (SPECIFY) | other inorganic | other inorganic`.

**W1/W2/W3 fertilizer-type value labels and the W3 stem** — HELD from `pyreadstat` metadata: W1 `sect11d_plantingw1.dta` `s11dq3` = {1 npk, 2 urea, 3 composite manure, 4 other (specify)}; W2 `sect11d_plantingw2.dta` = {NPK, UREA, Composite Manure, Other (specify)}; W3 `secta11d_harvestw3.dta` `s11dq3`/`s11dq15`/`s11dq27` = {1. NPK, 2. UREA, 3. COMPOSITE MANURE, 4. OTHER (SPECIFY)} with varlabels "MAIN TYPE OF LEFT OVER / PURCHASED INORGANIC FERTILIZER USED ON [PLOT]". The CONTENTS.org sentence ("the stem's word against its own option list") is true.

**Does it fix the issue (Nigeria half of #824)?** — HELD. The pre-fix parquet read through branch code with `assume_cache_fresh=True`: 62,844 × 9, no `Unit_sold`. `cache clear --country Nigeria` against MY root (var/ emptied to `livestock.parquet.bak866`), then `Country('Nigeria').crop_production()` cold: built in 16.5 s, 62,844 × 10, three `No rule to make target` make failures (GH #866 route, as documented), `Unit_sold` non-null 2011Q1 3,486 / 2013Q1 3,430 / 2016Q1 0 / 2019Q1 0 / 2024Q1 0, dtype `string`, 0 duplicate index rows.

**"Every pre-existing column byte-identical, 62,844 rows"** — HELD. `assert_series_equal` on all 9 pre-existing parquet columns and an identical sorted index, pre-fix snapshot vs the rebuilt parquet. API frame 62,844 × 10, `Quantity_sold`/`Value_sold` served `Float64`.

**Audits before vs after (pre-fix parquet vs cold rebuild)** — unchanged: `grain_reports` 0 / 0; `null_index_reports` 1 / 1 (level `v`, 86 NaN keys in 2013Q1 — pre-existing); `null_read_reports` 5 / 5 (Quantity_sold, Value_sold, harvest_month, intercropped, planting_month per-wave nulls — pre-existing; `Unit_sold` is `optional`, so Site B does not fire on it); `quantity_reports` 3 / 3 (4 + 2 + 3 implausible `Quantity` rows, pre-existing).

**Built-table / API agreement, NA causes, disagreement cells** — HELD exactly on BOTH the parquet and the served frame: 2011Q1 both 3,451 / equal 3,374 (97.8%), non-null 3,486, positive-sale rows with `Unit_sold` NA 41, `Unit_sold` without a positive sale 1, extra labels ∅, top cells `l→Kg` 10, `Kg→Sack/Bag` 8 + `Sack/Bag→Kg` 6 (=14); 2013Q1 both 3,411 / equal 3,283 (96.2%), non-null 3,430, 6 / 5, extra = {Olodo}, `Kg→Tiya` 13 + `Kg→Mudu` 11 (=24). The read-path categorical mapping does not move `u` away from `Unit_sold` (no `unit_sold` table exists in `lsms_library/categorical_mapping/` or Nigeria's org).

**Raw→built decomposition transcribed into CONTENTS.org** — HELD, re-derived independently on the raw W1/W2 files using the script's own key (`format_id(hhid)`, `format_id(plotid)`, crop LABEL via `nigeria._crop_labels()`, stable sort, `keep='first'`): both-units rows with an unresolved crop label **0 / 0**; both-units rows dropped by the dedup **93 / 66** (3,548→3,455; 3,478→3,412); of those **151** (89 + 62) carry a different `Quantity_sold` than the kept row; **17 / 23** are keys whose kept row has no positive sale (40); unlabelled unit codes → NA **4 / 1** (`u` '5.0' ×4 in W1; `Unit_sold` '50.0' ×1 in W2); closure 3,455−4 = 3,451 and 3,412−1 = 3,411. Raw agreement 3,438/3,548 and 3,316/3,478; canonical 3,466 / 3,349 — all exact. One definitional variance: "122 W1 / 81 W2 duplicate keys among positive-sale rows" — I get **125** / 81 counting keys duplicated anywhere in the frame that hold ≥ 1 positive-sale row (78 / 43 if both rows must be positive-sale). Not a refutation; the sentence should say which it means. Nit: "93 W1 … positive-sale rows are discarded" is the both-units count; positive-sale rows discarded are 96 in W1 (66 in W2).

**Cache cost "all 24 Nigeria tables re-warm; Mali does not"** — HELD. `Country._table_cache_hash(t, waves)` under `LSMS_COUNTRIES_ROOT=<base>` vs the clone: Nigeria **23 of 23** scheme tables moved (24 with `panel_ids`, which is a property, not a table); Mali **0 of 23**. `tests/test_build_transform_hash.py` passed in the selection below.

**Full-suite claim "1 failed (pre-existing), 5347 passed"** — the failure HELD (`tests/test_no_editable_install.py::test_every_poetry_install_passes_no_root` fails identically on the base worktree); my totals differ (4,744 passed / 192 skipped = 4,941 executed vs the agent's 5,347 / 171 = 5,523; `--collect-only` here reports 5,769). Which tests execute depends on the environment and on which countries are warm in `data_root()` (mine: Nigeria and Mali only); not investigated further and not attributable to the branch — the same tree was under test in both runs.

## 2. Claimed vs measured

| claim (report / commit / CONTENTS.org) | claimed | measured |
|---|---|---|
| Mali kg N, alias → removed | 1,028,795.6 → 906,361.8 (−122,433.8) | 1,028,795.6 → 906,361.8 |
| Mali matched rows / plots | 38,569→28,929 / 3,029→2,869 | same |
| Mali `Other inorganic` rows | 9,640 | 9,640 |
| Nigeria kg N | 178,074.5 → 173,672.0 (−4,402.5) | same |
| Nigeria matched rows / plots | 10,184→10,014 / 4,472→4,359 | same |
| Nigeria `Other Inorganic Fertilizer` rows, by wave | 170; 66/8/35 + 36/25 | 170; 66/8/35/36/25 |
| `transformations.py` vs development | byte-identical | empty diff |
| cold rebuild shape / time | 62,844 × 10, 14.9 s | 62,844 × 10, 16.5 s |
| `Unit_sold` non-null per wave | 3,486 / 3,430 / 0 / 0 / 0 | same |
| pre-existing columns | byte-identical | 9/9 equal, index identical |
| raw agreement | 3,438/3,548; 3,316/3,478 | same |
| canonical agreement | 3,466; 3,349 | same |
| built / API both & equal | 3,451/3,374; 3,411/3,283 | same, both tiers |
| NA causes: unrecorded sold unit / unit w/o sale | 41/6 ; 1/5 | 41/6 ; 1/5 |
| unresolved crop labels | 0 | 0 |
| dedup drops (both-units) | 93 / 66 | 93 / 66 |
| dropped with different `Quantity_sold` | 151 | 151 |
| only sale on key | 40 (17/23) | 40 (17/23) |
| duplicate keys among positive-sale rows | 122 / 81 | 125 / 81 (definition-dependent) |
| unlabelled unit codes → NA | 4 / 1 | 4 / 1 |
| only extra `Unit_sold` label | Olodo | Olodo (2013Q1) |
| top disagreement cells | l→Kg 10, Kg↔Sack 14; Kg→Tiya/Mudu 24 | 10, 8+6; 13+11 |
| W1–W3 `s11dq3` labels | {npk, urea, composite manure, other (specify)} | confirmed, all three waves |
| Nigeria L2-country hashes moved / Mali | 24 / 0 | 23 of 23 tables (+panel_ids) / 0 |
| audits before→after | not claimed | grain 0→0, null_index 1→1, null_read 5→5, quantity 3→3 |

## 3. Regression

- Country selection (branch, `-n 12 --dist=loadfile`, BLAS pinned, `LSMS_BUILD_WORKERS=1`): `tests/test_nigeria_cluster_identity.py tests/test_nigeria_food_acquired_sources.py tests/test_nigeria_kg_factor.py tests/test_nigeria_sold_unit.py tests/test_schema_consistency.py tests/test_feature_canonical_index.py tests/test_build_transform_hash.py` → `423 passed, 32 warnings in 381.54s (0:06:21)`.
- Full suite (branch, `-n 20 --dist=loadfile`, same env, log at `/local/job39922577/scratch/pytest_full.log`): `1 failed, 4744 passed, 192 skipped, 3 xfailed, 1 xpassed, 827 warnings in 1224.41s (0:20:24)`; the failure is `tests/test_no_editable_install.py::test_every_poetry_install_passes_no_root`, which also fails on the base worktree (`.github/workflows/ci.yml` lacks `--no-root`; zero diff on this branch).
- `bench/feature_audit/scan.py --features crop_production --countries Nigeria Mali`: 137 records, all `warn`, severity C: 129 `runtime_warning` (unclosed-file ResourceWarnings from the scanner's own script reads), Nigeria 5 `null_content` (the same five pre-existing per-wave-null columns; `Unit_sold` not among them) + 1 `index_levels_match_scheme` (`v` joined at API time — every country), Mali 1 + 1 of the same kinds. Nothing new.
- New tests on the base worktree (file copied in, `PYTHONPATH=<base>`, identity asserted): **6 fail on development** — `TestWiring::{test_sold_unit_is_canonicalised_like_the_harvest_unit, test_frame_without_sold_unit_delivers_all_na_string_column, test_frame_without_plot_grain_sale_delivers_all_na_string_column}`, `TestConfig::{test_only_w1_w2_frames_name_a_sold_unit, test_w1_w2_name_sa3q11b_literally, test_scheme_declares_unit_sold_optional}`. **12 pass on development**: `TestRawFiles` ×4 (raw-data facts, version-independent), `TestOtherInorganicSpelling` ×4 (a guard — passes wherever the alias is absent, by design), `TestConfig::test_canonical_schema_declares_unit_sold_optional_for_crop_production` (data_info.yml already carries it from the Uganda half), `TestWiring::test_column_order_keeps_every_pre_existing_column` (checks only that declared ⊆ built, and base's scheme lacks `Unit_sold`), and `TestBuiltTable` ×2, which read whatever parquet is in `data_root()` and therefore passed against MY branch-built parquet under base code — they cannot discriminate code versions and skip when the parquet is cold or pre-fix.

## 4. Scope findings

- `git diff --stat <merge-base>..HEAD`: exactly `Nigeria/_/CONTENTS.org`, `_/crop_production.py`, `_/data_scheme.yml`, `_/nigeria.py`, `tests/test_nigeria_sold_unit.py`. No baseline / blessed / lockfile / dependency / CI edit; no skip or xfail added; no moved pin. The `TestOtherInorganicSpelling` inversion is a tightening, not a weakening. `Mali/_/CONTENTS.org` deliberately untouched (the alias is gone, so the §6.1 Mali line no longer applies) — reasonable.
- **Residual drafting rule not met.** GH #824 remains open and this branch closes its Nigeria row (Uganda landed 2026-09-10, Malawi earlier). The report drafts no residual text for #824 (Niger 63–99% by wave; the other declarers to check — Ethiopia, Tanzania, Mali, Benin, Burkina Faso, CotedIvoire, GhanaSPS, Guinea-Bissau, Senegal, Togo; and the consumer side — `median_price_valuation` / farmgate price dividing `Value_sold` by `Quantity_sold` in `Unit_sold`). `gh issue list` shows no issue yet for either "report for filing" finding (the `drop_duplicates` grain collapse; the unread `sa3q16a/b`, `sa3q17` other-buyer columns); the report describes them with numbers but does not draft issue text. Both are human/dispatcher actions, not code.
- The report says it was written to `$STAGE/REPORT.md`; the array element dir (`backlog_fix_39919516/nigeria-824-followup/`) holds no REPORT.md — the report lives only in the local-runner dir `backlog_fix_local_39919092/`. Bookkeeping, noted so the next reader can find it.

## 5. Residual risks and what a human must decide

1. **File / draft the #824 residual** (Niger + 11 declarers + the consumer side) so the open issue describes exactly the remaining work; the branch is a partial close.
2. **Open the dedup issue**: `nigeria.crop_production_for_wave` `drop_duplicates(subset=[t,i,plot_id,crop], keep='first')` discards 93 / 66 both-units positive-sale rows (151 with a different `Quantity_sold`, 40 the only sale on their key) and 125 / 81 duplicate keys touch a sale; the "a handful" comment at `nigeria.py:~526` still understates it. Decide whether the raw duplicates are genuine second records (a missing level) or entry error.
3. **Open the other-buyer issue** (`sa3q16a/b`, `sa3q17`): served sale columns are main-buyer only.
4. **The Nigeria re-warm** (23 tables + panel_ids, L2-country and every L2-wave parquet) is real and unpaid on the shared cache; `nigeria.py` changed again on this follow-up, so a cache warmed on `fix/nigeria-824` at `d568ab428` is also stale.
5. `tests/test_no_editable_install.py::test_every_poetry_install_passes_no_root` fails on development (`ci.yml:96` `poetry install` without `--no-root`); not this branch's, but will show red on CI until someone fixes it.
6. W3's stem calls code 4 "inorganic" while option 3 is manure; the `harmonize_input` relabel to "Other Inorganic Fertilizer" is therefore a pre-existing editorial reading, now documented. Whether #867 ever assigns a share is a signed decision there.

## 6. Recommended follow-up commits (none blocks merge of the data change)

1. `Nigeria/_/CONTENTS.org` §"DONE Unit_sold": say "93 / 66 positive-sale rows **that record both units**" (positive-sale rows dropped are 96 / 66), and define the "122 / 81 duplicate keys" count (keys duplicated in the frame holding ≥ 1 positive-sale row: 125 / 81; keys where both rows are positive-sale: 78 / 43).
2. Dispatcher / human: a comment on #824 with the Nigeria close and the residual (Niger, remaining declarers, `Unit_sold`-aware farmgate price), and two new issues from REPORT.md items 1–2 (dedup collapse; `sa3q16a/b`/`sa3q17`), cross-referenced from `nigeria.py:~526`.
3. Optional: `tests/test_nigeria_sold_unit.py::TestWiring::test_column_order_keeps_every_pre_existing_column` could assert `UNIT_SOLD in out.columns` explicitly so it discriminates (today it passes on development).
4. Separate PR for whoever owns `.github/workflows/ci.yml`: add `--no-root` at line 96.
