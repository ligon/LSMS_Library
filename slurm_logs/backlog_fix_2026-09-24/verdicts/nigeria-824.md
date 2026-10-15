VERDICT: pass-with-fixes — the Nigeria `Unit_sold` fix works and every number in the report reproduces to the row, nothing pre-existing moves and scope is clean; but the #867 "spelling only" rider is not Nigeria-only: it silently re-values 9,640 Mali `plot_inputs` rows (+122,434 kg N, +13.5 % of Mali's nitrogen total), and two prose claims that ship with the branch are wrong (why the built-table counts are lower; the instrument context of the Nigeria "other (specify)" rows). Disclosure plus a maintainer signature on the class share is the remedy; the data change itself is not fabricated.

Reviewer: red-team element `nigeria-824`, Slurm job 39199807, own clone of `fix/nigeria-824` at `d568ab428` (merge base `242573697`; `origin/development` at `b5103b64f`), private `LSMS_DATA_DIR` seeded with the pre-fix Nigeria parquets, base worktree of `origin/development` at `/local/job39199807/base` (removed at the end). Package identity asserted (`lsms_library.__file__` under the clone) at the start and inside every build script. Nothing committed, nothing pushed, `git status --short` empty in both trees at the end.

## 1. Refutations (claim → what I ran → what I got)

**Does it fix the issue?** — HELD.
- Pre-fix parquet in my root (built from `development`): 62,844 × 9, no `Unit_sold`. Pre-fix API frame read on the branch with `Country('Nigeria', assume_cache_fresh=True)`: 9 columns.
- Route 1 (what a user hits — plain `Country('Nigeria').crop_production()` on the branch with the warm pre-fix parquet in place): the stale parquet was evicted (GH #866), `make` failed three times with `No rule to make target` (the three candidate paths), `try_script` ran `_/crop_production.py`, and the read served 62,844 × 10 with `Unit_sold`. **Second read: 1.5 s, warm, 0 `No rule` warnings** — the table converges; the new parquet carries `lsms_cache_hash` and the full 10-wave `lsms_cache_waves` manifest.
- Route 2 (mandate's cold route): `lsms-library cache clear --country Nigeria` against MY root (var/ emptied to `livestock.parquet.bak866`), then the same read: 21.9 s, same 3 `make` warnings. **Route 1 parquet == Route 2 parquet (all 10 columns, `assert_frame_equal`) and Route 1 API frame == Route 2 API frame.**

**"62,844 rows unchanged, every pre-existing column byte-identical"** — HELD. `assert_frame_equal` on the sorted index + the 9 pre-existing columns, pre-fix snapshot vs branch build, on BOTH the parquet (`Quantity_sold`/`Value_sold`/`intercropped` stay `string` in the parquet — the pre-existing defect the report names — and `Float64`/`boolean` on the API frame) and the served frame. Duplicate index rows: 0 before, 0 after.

**Raw-file agreement 3,438/3,548 (96.9 %) and 3,316/3,478 (95.3 %); canonical 3,466 (97.7 %) / 3,349 (96.3 %)** — HELD exactly, re-measured from the raw `.dta` via `get_dataframe` with the test's own `pos & both` definition (positive-sale rows 3,610 / 3,491, matching the older CONTENTS.org table too).

**Built-table agreement 3,374/3,451 (97.8 %) and 3,283/3,411 (96.2 %); non-null `Unit_sold` 3,486 / 3,430 / 0** — HELD exactly on the parquet and on the API frame. Top served disagreement cells HELD: 2011Q1 `l→Kg` 10, `Kg↔Sack/Bag` 14; 2013Q1 `Kg→Tiya/Mudu` 24. "Only label in `Unit_sold` absent from `u` is `Olodo`" — HELD.

**Value-label identification** — HELD. `pyreadstat` metadata: W1 `sa3q6b`/`sa3q11b` share one 36-entry label set, W2 `SECTA3_Q6A2`/`SECTA3_Q11B` one 48-entry set, dict-equal in both. W1's varlabel for `sa3q11b` is literally "(11a).Quantity sold through buyer (unit)" — the slip is real. **No sold-condition column in W1/W2 A3** — HELD: the only column whose label matches `condit|state|shell|form` is `state` (the geographic state code); 45 / 43 columns swept.

**Explanation of the raw→built gap ("the build drops rows with no resolved crop label and de-duplicates")** — REFUTED in its first half. Rows with a positive sale and both units whose crop label fails to resolve: **0** in both waves. The gap is entirely (a) `drop_duplicates(subset=[t,i,plot_id,crop], keep='first')`: **93** such rows dropped in W1 (3,548→3,455) and **66** in W2 (3,478→3,412), and (b) a recorded unit code that is unlabelled in Stata and canonicalises to NA (`u` = `'5.0'`/`'0.0'`: 4 rows in W1; `Unit_sold` = `'50.0'`: 1 row in W2). The dedup itself is PRE-EXISTING (it governed `Quantity_sold`/`Value_sold` before this branch) but it is not "a handful": of the 159 discarded positive-sale rows, **151 carry a different `Quantity_sold` than the row kept for their key, and 40 are keys whose kept row records NO sale at all** (17 W1, 23 W2). A country-script collapse selecting one observed row per key is exactly the GH #323 shape, upstream of every core audit, so `grain_reports()` cannot see it. Not introduced here; the branch inherits it for `Unit_sold` and mis-describes it.

**#867 rider: Nigeria 170 rows unmatched → 0; matched rows 10,014→10,184; plots 4,359→4,472; total N 173,672→178,074 kg** — HELD to the row, running `nitrogen_kg` on the SAME pre-fix `plot_inputs` parquet under base-worktree and branch `transformations` (base names `'Other Inorganic Fertilizer' (170 rows)` in the `NutrientCoverageWarning`; branch does not). The alias resolves because `_normalise_input_label` tries the full lowercase label first.

**#867 rider: "spelling only", Nigeria's label, "not a new number"** — REFUTED on reach. `_normalise_input_label` also ADDS ` fertilizer` to any label not ending in it, so Mali's served label `Other inorganic` (`Mali/_/categorical_mapping.org:573`, `harmonize_input` row `other_inorganic → Other inorganic`, a fixed EACI questionnaire slot beside Urea/DAP/NPK; named in `Mali/_/data_scheme.yml:188`) now resolves to the new key too: `_normalise_input_label('Other inorganic')` → `['other inorganic', 'other inorganic fertilizer']`, second candidate matches on the branch, nothing on base. Measured on a cold build of `Country('Mali').plot_inputs()` in my private root (113,735 rows): **9,640 `Other inorganic` rows** (2014-15 2,829; 2017-18 6,811; `u` NA 7,954, `Sac` 1,063, `Kg` 409, carts 200), `nitrogen_kg` matched rows **28,929 → 38,569**, plots **2,869 → 3,029**, total **906,361.8 → 1,028,795.6 kg N (+122,434, +13.5 %)** (same frame with vs without those rows; on base they were unmatched and contributed 0). Neither the commit message, the report, nor CONTENTS.org mentions Mali. No third country carries the label. The share is defensible (the instrument's slot is "other inorganic fertilizer" and 0.20 is the existing class share), but a 13.5 % move in a country's served nitrogen total is not a spelling fix and must be disclosed and signed.
- **The Nigeria framing is also wrong for most of its 170 rows.** The commit says the label is GHS-Panel "code 4. OTHER (SPECIFY) among the INORGANIC types (NPK / urea / other)". The W1 and W2 fertilizer-type value labels (`s11dq3`) are `{1: npk, 2: urea, 3: composite manure, 4: other (specify)}` — code 3 is ORGANIC, so code 4 is "other than NPK / urea / manure", which the instrument does not call inorganic. `Nigeria/_/categorical_mapping.org:758` (pre-existing) is what names W1–W3's code 4 "Other Inorganic Fertilizer"; only W4/W5 have a source column literally called `other inorganic`. By wave: 2010Q3 66, 2012Q3 8, 2015Q3 35 (**109 rows on the pre-existing relabel**) vs 2018Q3 36, 2023Q3 25 (61 rows where the source says inorganic). Small (+4,402 kg N, 2.5 % of Nigeria's total), but the comment in `transformations.py` asserts a provenance that is not the instrument's.

**Cache cost: "all 24 Nigeria tables re-warm; transformations.py reaches no fingerprint"** — HELD. `Country._table_cache_hash(t, waves)` for all 24 Nigeria tables, base worktree vs branch: **24 of 24 moved**. `build_transforms_fingerprint` for `crop_production`, `plot_inputs`, `sample`, `food_acquired`: **0 moved**. `tests/test_build_transform_hash.py` (incl. the no-transformations-symbol pin) passed in the selection below. Corollary for the rider: Mali's and Nigeria's `plot_inputs` parquets do NOT rebuild — the new nitrogen numbers appear on the next warm `nitrogen_kg` call, with no cache signal.

**Docstring: "NA means the frame names no sold-unit column, or the row records no sale"** — slightly overstated. Positive-sale rows with `Unit_sold` NA because the sale's unit was simply not recorded: 41 (2011Q1) / 6 (2013Q1); rows with a `Unit_sold` but no positive sale: 1 / 5. Nit.

## 2. Claimed vs measured

| claim | claimed | measured |
|---|---|---|
| rows / pre-existing columns | 62,844, byte-identical | 62,844; identical (parquet and API) |
| `Unit_sold` non-null 2011Q1 / 2013Q1 / W3–W5 | 3,486 / 3,430 / 0 | 3,486 / 3,430 / 0 |
| raw-code agreement 2011Q1 | 3,438 / 3,548 (96.9 %) | 3,438 / 3,548 (96.900 %) |
| raw-code agreement 2013Q1 | 3,316 / 3,478 (95.3 %) | 3,316 / 3,478 (95.342 %) |
| canonical agreement, raw files | 3,466 (97.7 %) / 3,349 (96.3 %) | 3,466 (97.689 %) / 3,349 (96.291 %) |
| built-table agreement | 3,374/3,451 (97.8 %) ; 3,283/3,411 (96.2 %) | identical, parquet and API |
| top disagreement cells | l→Kg 10, Kg↔Sack 14; Kg→Tiya/Mudu 24 | 10, 14; 24 |
| label-set identity | W1 36 = 36, W2 48 = 48 | identical dicts, 36 / 48 |
| raw→built gap cause | unresolved crop labels + dedup | **0 unresolved**; dedup 93 / 66 + unlabelled-code NA 4 / 1 |
| #867 Nigeria unmatched rows | 170 → 0 | 170 → 0 |
| #867 Nigeria matched rows / plots / kg N | 10,014→10,184 / 4,359→4,472 / 173,672→178,074 | 10,014→10,184 / 4,359→4,472 / 173,672.0→178,074.5 |
| #867 other countries affected | none stated ("Nigeria's label", "spelling only") | **Mali `Other inorganic`: 9,640 rows; matched 28,929→38,569; plots 2,869→3,029; 906,362→1,028,796 kg N (+13.5 %)** |
| Nigeria "other" rows are inorganic by the instrument | implied for all 170 | 61 (W4/W5); 109 are W1–W3 "other (specify)" beside "composite manure" |
| L2-country hashes moved | 24 | 24 of 24; fingerprints 0 of 4 |
| second read after rebuild | (not claimed) | 1.5 s warm, 0 `make` warnings |
| cold (`cache clear`) build == warm-route build | (not claimed) | identical, 10/10 columns |

## 3. Regression

Run in place of the full suite, deliberately: only Nigeria was warm in this root (a full run would cold-build the corpus into it), and the one framework edit is a dict literal that measurably reaches no build fingerprint. Selection = every Nigeria test file + every test consuming `_NITROGEN_CONTENT`/`nitrogen_kg` + the mandated framework files, under the verbatim env pinning, `-n 8 --dist=loadfile`, output to a file:

```
tests/test_nigeria_sold_unit.py tests/test_nigeria_kg_factor.py tests/test_nigeria_cluster_identity.py
tests/test_nigeria_food_acquired_sources.py tests/test_863_transforms.py tests/test_build_transform_hash.py
tests/test_derivations.py tests/test_feature_canonical_index.py tests/test_schema_consistency.py
tests/test_uganda_sold_unit.py tests/test_plot_id_canonical.py
550 passed, 8 skipped, 62 warnings in 414.77s (0:06:54)
```
Skips: `test_863_transforms.py:599` × 8 — Malawi / Uganda tables not warm in this root. No failures. Note `test_derivations::TestDelivered` cold-built GhanaLSS `food_acquired` into my private root during the run (hence the GhanaLSS warning noise in the log); the shared blob store served every read with no write.

`bench/feature_audit/scan.py --features crop_production --countries Nigeria`: `features: 1  records: 97  skips: 0 ; by status: {'warn': 97}` — all 97 are pre-existing classes (unclosed-file `ResourceWarning`s from the hasher, the same five `null_content` rows the pre-fix read emitted, `index_levels_match_scheme` extra `v`, canonical-alignment `u` sentinel fill 13,138, population pooling); none names `Unit_sold`.

Audits, pre-fix read vs branch read, identical: `grain_reports()` `[]`; `null_read_reports()` the same 5 wave-scoped rows (`Quantity_sold`, `Value_sold`, `harvest_month`, `intercropped`, `planting_month`); `null_index_reports()` `v` NaN 86 rows in 2013Q1; `quantity_reports()` 4/2/3 offenders in 2011Q1/2013Q1/2016Q1. `Feature('crop_production')(['Nigeria'])` assembles: 62,844 × 9 columns on the canonical 10-level index, `Unit_sold` string, 6,916 non-null, 0 duplicates.

Stray writes: `find lsms_library/countries -name '*.parquet'` = 0 throughout; both worktrees clean; every parquet written (Nigeria, GhanaLSS, Mali) landed under my private root.

## 4. Scope findings

- `git diff --stat origin/development...HEAD`: exactly the six files in the report (CONTENTS.org, `_/crop_production.py`, `_/data_scheme.yml`, `_/nigeria.py`, `transformations.py`, new `tests/test_nigeria_sold_unit.py`). The two-dot diffstat shows deletions under `slurm_logs/backlog_fix_2026-09-24/` and `.sucoder/handoff.org` — `development` advancing, not branch edits. No file on `development` since the merge base touches Nigeria, `transformations.py`, `data_info.yml` or `tests/` (merge is clean).
- No baseline / golden / `blessed.csv` / lockfile / dependency edit. No existing assertion weakened, skipped or xfailed (only additions).
- `data_scheme.yml` and the test file are unnamed by the task but necessary and not on the stop-list; the report declares both. **The Mali effect of the rider is an undeclared scope extension** — a served-number change in a country the task never named.
- New tests on `origin/development` (base worktree): **8 of 15 fail** (three wiring, three config, two #867). **7 pass on base**: the two value-label-identity and two agreement-rate tests (they read raw files with the pre-existing `_canon_unit`, so they document the data, not the fix), `test_canonical_schema_declares_unit_sold_optional_for_crop_production` (`data_info.yml` already had it from Uganda), `test_column_order_keeps_every_pre_existing_column` (vacuous on base: base's scheme lacks the column so "declared ⊆ built" holds trivially), and `TestBuiltTable::test_unit_sold_populated_only_in_w1_w2` (parquet-content only; it passed on base, I infer from timestamps, because my Route 1 build had just written the branch parquet — base test run ~03:55, parquet mtime 03:56 — and against a pre-fix parquet it skips, so it never fails).

## 5. Residual risks and what a human must decide

1. **Sign the 0.20 N share for Mali's `Other inorganic` (9,640 rows, +13.5 % of Mali's kg N) or exclude it from this branch.** The label is the instrument's own "other inorganic" slot, so the class share is the same logic as the existing `inorganic fertilizer: 0.20`; but #867's own terms make N-share assignment a maintainer decision, it is undisclosed, and there is no cache signal when it lands (no fingerprint moves).
2. **Sign or scope the same share on Nigeria's W1–W3 "other (specify)" (109 of 170 rows).** The pre-existing `harmonize_input` relabel calls them inorganic; the questionnaire does not. Cost of accepting: 2.5 % of Nigeria's nitrogen total.
3. **The country script's `drop_duplicates(keep='first')` is a silent grain collapse** — 159 positive-sale rows discarded in W1/W2, 40 of them the only sale on their key — invisible to `grain_reports()`. Pre-existing and out of this task's scope, but `Unit_sold` now inherits it and CONTENTS.org mis-attributes it. Deserves its own issue (which row to keep, or whether the raw duplicates are genuine second records per plot-crop).
4. **`Quantity_sold` / `Value_sold` / `Unit_sold` are the MAIN buyer only.** W1/W2 A3 also carry `sa3q16a/b` (quantity + unit sold to other buyers) and `sa3q17` (their value); neither the build nor CONTENTS.org mentions them. Pre-existing scope; a per-unit farmgate price from these columns is a main-buyer price.
5. **The Nigeria re-warm is real**: all 24 L2-country hashes move, and every Nigeria L2-wave parquet with it. Stated correctly; nobody has paid it on the shared cache yet.
6. Unlabelled Stata codes in a unit column canonicalise to NA silently (`u` 4 rows W1, `Unit_sold` 1 row W2) — the same treatment `u` gets today; nit.

## 6. Recommended follow-up commits (doc/comment-level plus one measurement; none reverts the data change)

1. `lsms_library/transformations.py` (`_NITROGEN_CONTENT` comment, ~line 4514): state that the key is reached by BOTH Nigeria's `Other Inorganic Fertilizer` (strip/first candidate) and Mali's `Other inorganic` (add-rule), with the measured counts (170 rows / +4,402 kg N; 9,640 rows / +122,434 kg N), and replace "among the INORGANIC types (NPK / urea / other)" with the measured provenance — W1/W2 `s11dq3` = `{1 npk, 2 urea, 3 composite manure, 4 other (specify)}`; the "inorganic" reading for W1–W3 comes from `Nigeria/_/categorical_mapping.org:758`, not the instrument. Amend the commit message / PR body the same way, and re-measure Mali on the shared warm cache before merge. Add a Mali line to `Mali/_/CONTENTS.org` (which currently says nothing about nitrogen shares) and `Nigeria/_/CONTENTS.org` §"GH #867 rider". Have @ligon sign both shares in that commit, or drop the alias from this branch and let #867 carry it.
2. `Nigeria/_/CONTENTS.org` §"DONE Unit_sold": replace "(built-table counts are lower than raw because the build drops rows with no resolved crop label and de-duplicates on the index grain)" with the measured decomposition (0 unresolved; 93 / 66 rows dropped by the `(t,i,plot_id,crop)` dedup, 151 with a different `Quantity_sold` than the kept row, 40 the only sale on their key; 4 / 1 rows with an unlabelled unit code → NA). Open a GH issue for the dedup itself, cross-referenced from `_/nigeria.py:526` ("a handful of duplicate (i,plot,crop) rows exist in W1" understates it: 122 keys W1, 81 keys W2 among positive-sale rows alone).
3. `Nigeria/_/nigeria.py` `crop_production_for_wave` docstring: "NA means the frame names no sold-unit column, the row records no sale, **or the sale's unit was not recorded** (41 / 6 positive-sale rows)".
4. `tests/test_nigeria_sold_unit.py::TestOtherInorganicSpelling`: add the Mali label (`'Other inorganic'`) to the resolution test so the reach of the alias is pinned, not discovered; optionally make `TestBuiltTable` assert on the API frame rather than the raw parquet so it exercises the code path.
