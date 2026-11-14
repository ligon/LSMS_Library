# PR #1604 red team: Malawi 2024-25 food with IHS5 factor borrowing

**Verdict: CHANGES (minor).** No data-integrity defect was found in panel IDs, the 2024-25 sample, the 2024-25 food household set or the derivation stamp. Older waves are frame-equal.

**What merge is waiting on:**
- **F1** (bucket rows). There are two ways to resolve it:
  - (a) *Text-only*: state the density-1 choice. This is a modelling choice the 2026-11-12 ruling did not address, so it needs a one-line maintainer answer.
  - (b) *Code*: admit code 26 to borrowing. This needs no new ruling, since it applies the approved item 49 borrowing, but the borrowing and derivation probe would have to be re-run.
- **F2** (`check_panel_consistency`). Text-only disclosure. Nothing gates on it: the only test that runs `check_panel_consistency` targets GhanaLSS, and no `.github` workflow calls it.
- **PR body.** Rewrite it with the must-state items below.

Everything else is LOW or informational.

- Reviewer: independent red-team agent (Claude Code), model **Claude Opus 5.5** (`claude-opus-5-5`), dispatched by Sue (Slurm job 41201840, n0035.savio4), 2026-11-12.
- Base: `e5fdd2fa831c4d03a247d264be7f339da459e6b0` (development).
- Candidate: `7735489cceb0fd68acf273eff850a5974d71cc58` (`fix/malawi-food-integration-20261107`).
- Read-only: I made no checkout edits and no GitHub writes. The only clones were throwaway ones on job-local disk.

**Independence disclosure.** While listing the reports directory, I printed the first 5 lines of `../1604-reverify/REPORT.md` (the verifier's header: verdict line and head SHAs). I read nothing else there. I treated the job ids and counts quoted in `derivations.yml` and `CONTENTS.org` as claims to test, not as evidence.

---

## Idiosyncrasies that matter (from `Malawi/_/CONTENTS.org` and the sources)

1. **2024-25 is two overlapping catalog entries.** IHS6 (8507, `Data/Cross_Sectional/`) and IHPS 2024 (8520, `Data/Panel/`). The 815 IHPS *refreshment* households are the *same interviews* as IHS6 households: their EAs are a subsample of IHS6's Q2 EAs. Stacking the covers the way 2019-20 does would count them twice.
   - Measured: all 815 refreshment `case_id`s are unique and in the IHS6 cover; 815/815 refreshment EAs are IHS6 EAs; **0/1,948** retained EAs are IHS6 EAs.
2. **The panel half links on 51 EAs.** `y4_hhid` is blank on the 815, so only 1,948 retained households link (`y5_hhid -> y4_hhid`).
3. **Weights differ by half.** `hh_wgt`/`panelweight_2019` are null on the 815; `wt_cross` covers all 2,763 and `wt_long` covers 1,948. The ruling fixes `weight` = `hh_wgt` (IHS6) / `wt_cross` (retained IHPS) and `panel_weight` = `wt_long`. The two are different variables pooled in one wave column, and the decision-48 joint-inclusion question is still open.
4. **Identifier namespaces collide.** Raw IHPS ids are `NNNN-NNN` and are **reused across waves** (1,083 2024-25 `y5_hhid`s also appear as raw keys in 2013-14). Canonical ids are IHS3 12-digit `case_id`s with `_N` split suffixes. The cross-sections are prefixed `cs-17-`, `cs-19-`, and now `cs-24-`.
   - Bare IHS6 `case_id`s coincide with 3 served 2010-11 ids, so the prefix is load-bearing (GH #1278 precedent).
5. **IHPS tracks split-offs, so claimants of one previous id get `_N` suffixes** (`update_id`, GH #504). `update_id` guards only against ids live in the *current* wave. A later wave can re-mint a suffix that a since-departed lineage held. 2024-25 did so 25 times, and 2019-20's own 10 such re-mints are pre-existing and left alone.
6. **The panel crosswalk is a committed SOURCE** (design A, GH #894/#914). `make panel-ids` regenerates it from the per-wave `panel_ids:` blocks, and row order decides who keeps the bare id.
7. **2024-25 ships no geovariables** (no coordinates for `cluster_features`) and has no `household_roster` wiring yet. This is what drives F2.
8. **Malawi food `u` is label-based in IHS4+.** The size code (e.g. `10A` Heap Small) lives in `hh_g0Xb`; the served `u` is the label (`Heap`). The IHS5 factor table keys on the full size-bearing code, plus `Otherunit` text for code 23.
9. **`malawi.names_a_food` builds a food vocabulary from every `harmonize_food` column** (`malawi.py:227`). So adding rows or a wave column to that table can flip older waves' code-23 specify texts to `Unknown` (GH #1042 ruling 5). See F8: measured, no flip today.

---

## Findings

### F1. MEDIUM: `5 Litre Bucket(Chigoba)` (unit code 26) is refused borrowing as "metric", rewritten to `u='kg'` at 5 kg, and loses its native unit. 2019-20 serves the same unit natively with the released factor.
- **Where:**
  - `2024-25/_/food_borrowing.py:70-74`: `_metric` counts any label `_metric_kg_factor` parses.
  - `food_borrowing.py:121`: borrowing is refused for those labels.
  - `Malawi/_/malawi.py:713`: the choke point scales Quantity and stamps `'kg'`.
- **Evidence (probe2 `metric_and_borrow_scope`, job 41238038):**
  - 3,316 purchased, 1,707 produced and 791 in-kind active rows carry code 26. All are classed metric. **4,031** of them have an *admitted* full-key IHS5 factor: median **3.88 kg/bucket**, range 2.29–5.33.
  - Served 2024-25 has **5,972** `u='kg'` rows, all with `Quantity_kg` NA (2019-20 has 156), and only 21 bucket-labelled rows.
  - In 2019-20, **3,303/3,304** bucket rows are served natively with released-factor `Quantity_kg` (median 3.88).
  - The 2024-25 buckets are therefore served at density 1 (5 kg/bucket), about **+29%** against the median admitted factor (3.88; I did not look up which items carry it) and up to ×2.2 against the smallest (2.29). Roughly 10,000 kg are at stake against the wave's 717,132 kg in `food_quantities(units='kgs')`, estimated at the median.
  - 1,783 of the 5,814 code-26 source rows have **no** admitted factor, so admitting code 26 would not borrow for them. They would fall to the inference ladder instead.
  - The 5,972 served `kg` rows include other leading-magnitude labels besides the buckets.
- **Why it matters:** it contradicts the PR's own contract.
  - `CONTENTS.org:21`: "Existing metric conversion stays at its original choke point, native amounts/units are retained".
  - `derivations.yml:57`: "Keep the existing native Quantity, unit and Expenditure".
  - A 5-litre bucket is a volume container, not a stated mass. The leading-magnitude labels in grams (`Satchet/Tube (25G)`, `Loaf (600G)`, …) are different, and refusing those is harmless.
- **Change requested:** narrow `_metric` so a *volume* magnitude is not treated as an already-metric mass, or admit code 26 explicitly. The bucket rows would then borrow their released item factor and keep `u` native, as in 2019-20.
  - If the author prefers the density-1 reading, put it to the maintainer and state it in the PR body, `CONTENTS.org` and the derivation record, with the counts above. As written, the stated contract is false for these rows.

### F2. MEDIUM: `check_panel_consistency(Malawi)` regresses from 0 FAIL to 1 FAIL, undisclosed.
- **Evidence:** `frames/{base,cand}.build.json`, `panel_consistency`.
  - Base: 7 pass / 1 warn.
  - Candidate: `panel_ids_targets_exist` **FAIL**: "1948/7634 current-wave targets MISSING (26%) vs spine from household_roster". There is also a new warn, `attrition_monotonic` "(2019-20,2024-25): zero overlap".
  - Cause: the crosswalk now links 2024-25, but the roster spine has no 2024-25 wave (only `sample` and `food_acquired` are wired).
  - The 1,948 targets *do* exist in `sample` (measured: all 1,948 non-`cs-24-` ids are in V).
  - The `CONTENTS.org` GH #894 entry still records "7 pass, 1 warn".
- **Change requested:** disclose it in the PR body and `CONTENTS.org`, either as a known diagnostic failure until `household_roster` 2024-25 is wired, or by letting the check fall back to the `sample` spine. It is not a served-data defect.

### F3. MEDIUM (cost, not correctness): the corpus-wide re-warm is caused solely by the `country.py` hunk, and that branch is not reached in production.
- **Evidence:** fingerprint probe on extracted trees.
  - The `country.py` hunk alone moves `build_transforms_fingerprint` for **10/10** probed tables plus `btf(None)`. It also moves `_table_cache_hash` for **5/5** probed non-Malawi cells (Uganda roster and sample, Tanzania food, Niger sample, Albania roster).
  - The `local_tools.py` additions alone move **none** of these.
  - The candidate *without* the `country.py` hunk moves only Malawi.
- **Evidence:** branch probe, job 41237351, `results/branch_{default,make}.json`.
  - Under **both** the default and `LSMS_BUILD_BACKEND=make` backends, in fresh private roots, `Country('Malawi').updated_ids`/`.panel_ids` are served from the committed JSON.
  - The wrapped `panel_ids`/`panel_ids_guarding` were called **0** times, and the same holds for Uganda, Tanzania, Nigeria and Ethiopia.
  - The guarded DataFrame branch (`country.py:7571`) is reachable only when a country ships no committed JSON. `DESIGN_B_PENDING` is empty, so in the corpus only tests reach it: the `test_gh970` Design-B fixtures and the `Country._compute_panel_ids` / `_panel_id_guard_from` tests in `test_malawi_panel_id_guard.py`.
- **Assessment:** the ruling accepted "moves every build fingerprint (corpus-wide re-warm)", so this is not blocking. But the cost buys only fallback parity.
- **Change requested (coordinator's choice):** either drop the `country.py` hunk, making the re-warm Malawi-only; or keep it and state in the PR body that the corpus-wide re-warm protects only the no-committed-JSON fallback.
  - Dropping it also means dropping the four `Country`-level tests in `test_malawi_panel_id_guard.py` and the two added `test_gh970` assertions.

### F4. LOW: `ihps_weight` is served at raw expansion scale beside mean-1 weights.
- `_SAMPLE_WEIGHT_COLUMNS` = `('weight', 'panel_weight')`. The API 2024-25 means are weight 1.0, panel_weight 1.0, **ihps_weight 1,784.6**.
- This is harmless for rebuilding the IHPS cross-section, which uses ratios. But a user who reads it as a third weight gets another scale with no marker.
- **Change requested:** say so in the `data_scheme.yml` comment (`Malawi/_/data_scheme.yml:16-24`) and `CONTENTS.org`, or normalise it.

### F5. LOW: Make prerequisites miss the 2024-25 helper files (GH #1237 class).
- `Malawi/_/Makefile:39` names only `../%/_/food_acquired.py malawi.py categorical_mapping.org`. `2024-25/_/food_acquired.py` imports `food_borrowing.py`, and that reads `borrow_dictionaries.json`.
- Under `LSMS_BUILD_BACKEND=make`, an edit to either will not rebuild the wave parquet. The default backend is safe, because `Wave._input_hash` hashes every `.py`/`.json` in the wave `_/`.
- **Change requested:** add a 2024-25-specific prerequisite rule, or record the gap.

### F6. LOW, must-state: 2024-25 label decisions beyond item 119.
- The new `harmonize_food` 2024-25 column maps two *broadened* items onto existing Preferred Labels (`categorical_mapping.org:79`, `:95`):
  - 112 "Buns, scones, muffins" → `Buns, Scones`.
  - 509 "Other poultry … quails etc" → `Other Poultry …`.
- It also adds 10 new Preferred Labels with GD/Aggregate classes: Soya Pieces, Bean Mixed, Eggplant, Carrots, Offal, Processed Meat, Peaches, Water Melon, Energy Drinks and Gramil. Only item 119 is ruled.
- The factor borrowing correctly *refuses* 112 and 509 (dictionary labels differ), while the `j` label asserts continuity. These are defensible labelling choices, but they should be stated.
- 2024-25 "Other (specify)" is unmapped (`---`).
- `aggregate_short.yml` has no `Gramil` entry, so a rerun of `util/orgtbl` cannot regenerate that cell.

### F7. LOW: stale or unreproduced documentation.
- The PR body is stale: it says "draft under that hold" and cites old heads.
- The ledger's final section says weights and normalisation are "HELD".
- `CONTENTS.org:2114` (GH #894 entry, pre-existing) and `2024-25/_/mapping.py:44` say the make backend "reads this block directly". At the Country level I measured the opposite: the make backend serves the committed JSON (F3).
- The hook is still correct defensive code. The sentence should be qualified or backed by the route that reproduces it.

### F8. INFO: the `names_a_food` vocabulary grows by 20 words, and no older wave flips today.
- Added words: animals, balls, blood, carrots, chilled, eggplant(s), energy, liver, melon(s), minced, mixed, muffins, offal, parts, peaches, sausage, slaughtered, soya. None are removed, and the unit words are unchanged.
- The older-wave `food_acquired` is frame-equal (below), so no code-23 specify text in 2004-05 to 2019-20 changes. The coupling is latent: any future table edit can flip older units.

---

## Per-question results

### Q1. Panel IDs: PASS on every point
- **Guard logic.** `update_id_reserving` equals `update_id` plus reservation of all earlier-wave map values. `panel_ids_guarding` runs `update_id` itself before `guard_from`. String comparison is valid for these wave labels.
- **Pre-guard waves.** On the real frame (job 41237351, `results/panel_check.json`), plain vs guarded give identical maps for 2013-14, 2016-17 and 2019-20, and an identical recursive `panel_ids`.
- **Moves and collisions.** The guard moves **33** of the 2024-25 ids.
  - Unguarded, **25** mints equal an id issued earlier to another lineage. Guarded: **0**.
  - Against *served* earlier waves, the 610 minted 2024-25 ids appear in none, and the 1,338 that do appear are exactly their own lineage's continuation id.
  - Collisions with raw ids are structurally impossible: minted ids carry `_N`, and raw ids never contain `_`.
  - 2019-20's 10 pre-existing re-mints are confirmed and left, as documented.
- **id_walk.** The 2024-25 partition is K−V 1,948, V−K 1,948, **K∩V 0**.
  - Served 2024-25 `sample` holds only walked-only (1,948) and inert `cs-24-` (12,888) ids. `food_acquired` holds 48,067 walked-only and 283,610 inert rows. There are 0 raw-only and 0 chained ids, so neither "mixed" nor "undecidable" can arise.
  - A second `id_walk` is an identity on the 2024-25 slice and on all waves.
  - Fresh-process warm reads (all waves, `waves=['2024-25']`, `waves=['2019-20']`) raised no `IdWalkError` and are `assert_frame_equal(check_exact=True)` to cold, including dtypes and attrs keys (job 41238038).
- **Regeneration.** `make -C …/Malawi/_ panel-ids` regenerates both JSONs **byte-identically** (`git diff --exit-code` = 0). sha256: `01f98fd6…` (panel_ids) and `173b1be8…` (updated_ids).
- **Pre-2024-25 entries.** No `panel_ids`/`updated_ids` entry before 2024-25 is changed or removed: 5,686 entries carried over, 1,948 added.
- **Other countries.** The other 10 panel countries do not opt in (`Panel IDs:` appears only in Malawi's `data_scheme.yml`). `panel_ids`/`update_id` are untouched, and their committed JSONs are untouched. Uganda, Tanzania, Nigeria and Ethiopia `updated_ids` equal their committed JSON under both backends.

### Q2. Sample 2024-25: PASS
All checks below are against the raw covers and the raw L2 parquet.
- **Rows.** 14,836 = 12,888 (IHS6) + 1,948 (retained), `i` unique, 0 duplicate index entries.
- **No double counting.** None of the 815 refreshment `y5_hhid`s is in the index. All 815 have their IHS6 row, and the hook fails closed otherwise.
- **`cs-24-`.** 0 occurrences in any earlier wave. No raw `NNNN-NNN` id is left.
- **Food households (saved frames).** The 2024-25 `food_acquired` slice has 331,677 rows over 14,836 households: 12,888 `cs-24-` and 1,948 canonical.
  - Every one is in the 2024-25 `sample`.
  - No `i` matches the raw `NNNN-NNN` form.
  - `v` is non-null on all rows.
  - So no refreshment household is served twice in food either.
- **Weights (raw parquet).**
  - `weight` equals the ruled column on **14,836/14,836** rows: `hh_wgt` for IHS6 (including the 815) and `wt_cross` for retained.
  - `panel_weight` equals `wt_long` on 1,948 rows and is NA elsewhere (14,836/14,836 correct).
  - API means: weight and panel_weight are 1.0 in every wave.
- **Other columns.** `v` is non-null on all 14,836 rows. `ihps_hhid`/`ihps_weight` = `y5_hhid`/`wt_cross` on exactly 2,763 rows and NA elsewhere. Both are declared `optional: true`, and both are **0 non-null** in every earlier wave.
- **Earlier waves.** `sample` is frame-equal (exact) on the original columns.

### Q3. Food borrowing: PASS, except F1 and F6
- **Source factors.** 1,768 factor rows, unique on the full four-field key. 1,492 admitted, 276 refused (exact match to the PR).
- **Stamp.** `Derivation == borrow-ihs5-factor` on exactly the **169,995** rows with non-null `Quantity_kg`, and on no row of any other wave.
- **Join.** `Country.derivation_inputs(key, wave='2024-25')` returns 169,995 input rows at `(t,i,j,u,s,food_obs)`. On `(t,i,j,u,s)` the join with the stamped served rows is a **bijection**: 0 served keys without inputs, 0 inputs without a served row.
  - Measured: the served and input `u` value counts are identical (Heap 73,631, Piece 70,402, …). I did not separately establish why.
- **Rederivation.** Σ native quantity × `ihs5.factor` reproduces the served `Quantity_kg` on **169,995/169,995** rows (rtol 1e-9).
  - The inputs carry the raw operands, factor key, origin (1: 168,089; 4: 1,792; 2: 114), year and MD5 `dc1bed57…`.
- **Refused and unsupported keys.** These leave `Quantity_kg` NA at `food_acquired`, by construction and confirmed in synthetic tests. Downstream they fall to the existing kg ladder (`kg_factor_sources`: survey_kg 589,944; metric 344,238; item_unit 280,943; unit 83,843; none 3,450; all waves).
- **`categorical_mapping.org`.** Only `harmonize_food` changed. All **148** existing rows are identical in all 9 original columns. The 10 new rows have `---` in every older wave column, and no new Preferred Label collides with an existing one.
- **Item 119.** `Grain Meal (Gramill)`, GD `Maize`, own aggregate `Gramil`, matching the CONTENTS ruling text.
- **Older waves.** No served label changes: `food_acquired` is frame-equal on all original columns, and the new `Derivation` column is 0 non-null on older waves.

### Q4. Framework blast radius
- `rg` sweep: `panel_ids_guarding`/`update_id_reserving` are referenced only in `local_tools.py`, `country.py:38,7571-7574`, `Malawi/_/panel_ids.py` and the new tests.
- No code iterates the top-level keys of `data_scheme.yml`, so the new `Panel IDs:` key is read only by name. `test_no_duplicate_yaml_keys` reads after `Data Scheme:`.
- **The other 10 panel countries are provably unaffected in data**: no opt-in, unchanged `panel_ids`, unchanged committed JSON, and identical reads.
- **Their caches are not unaffected:** see F3.

### Q5. Tests: no weakening found
- `test_gh1190`: the inverse check exempts exactly the 4 new names and a bare `import functools`, so any other `malawi.py` change still fails.
- `test_sample.py`: removing Malawi from `PARTIALLY_WIRED_SAMPLE` strengthens the test.
- `test_recall_record.py`: 67 → 68 is a count pin. It is legitimate, because a new record was added.
- `test_gh1228`: the fact `waves == set(fa.t)` is kept, and only the count moves 5 → 6.
- `test_gh1210`: an exact `== 5` became an exact set of the declared waves. That is stronger.
- `test_gh970`: assertions were added.
- **New test coverage.**
  - `test_malawi_food_borrowing.py` is synthetic only: it pins the key/ambiguity/metric/completeness rules, but nothing pins F1's behaviour on code 26 or the served join.
  - `test_malawi_panel_id_guard.py` pins the guard on toy frames.
  - No test pins the 2024-25 sample weights or the derivation join on real data. Both hold, as measured above; adding a cache-gated test would be cheap.

### Q6. Scope against the 2026-11-12 ruling: within scope, with F1/F6 as unruled choices
- No other wave's weights change (sample frame-equal; `_normalise_sample_weights` untouched).
- No panel id before 2024-25 changes.
- The other-country edits are comment-only: 19 `recall.yml` headers ("34 of 58" → "35 of 59"), `docs/guide/recall.md` 55/67 → 56/68, and one added row in `.coder/coverage/food_recall.csv`.
- Within the ruling, the unruled modelling choices are F1 (density-1 buckets) and F6 (broadened-item labels plus new classes).

### Derived tables on older waves (disclosed in the ruling as "materially moves")
Measured, base → candidate:
- `food_quantities(units='kgs')` totals move per wave by 2004-05 **+1.4%**, 2010-11 +0.1%, 2013-14 +0.4%, 2016-17 +0.2% and 2019-20 +0.1%. A few rows move between `Unknown`/native and `kg` (0.003% of `u` labels).
- `food_prices()` (kgvalue): **8.1%** of older-wave price rows change. Wave sums move by −0.3% to −1.0%; the 2013-14 median goes 289.66 → 284.09 and 2019-20 682.90 → 676.56.
- `food_expenditures` is frame-equal on older waves.

---

## What I ran

All runs used fresh private roots, with `LSMS_DATA_DIR`, `XDG_CACHE_HOME`, `TMPDIR`, `MPLCONFIGDIR` and `LSMS_ISSUES_LOG` pointed under `/local/job<ID>/priv*`. `data_root()` was asserted private and the import path was asserted to be the clone at the full SHA. Threads were pinned (`OPENBLAS/OMP/MKL=1`, `LSMS_BUILD_WORKERS=1`). The `dvc` CLI was never run. Scripts are in `scripts/`, raw outputs in `results/`, `frames/` and `logs/`.

| job | what | result |
|---|---|---|
| **41237339** (rt1604-build) | Base and candidate clones, cold builds in separate roots. Tables: `sample`, `household_roster`, `food_acquired`, `community_prices`, `food_quantities(kgs)`, `food_prices()`, `food_expenditures`, plus `check_panel_consistency`. Then compare and candidate probes. | All builds rc=0. Pre-2024 frames equal (see above). Probe sections `covers`, `sample_raw_parquet`, `sample_api` and `id_walk` saved; the later sections crashed on my own JSON-key bug and were re-run in job 41238038. 0 in-tree parquets; clean `git status`. |
| **41237351** (rt1604-panel) | Plain vs guarded crosswalk on the real frame; `make panel-ids` plus `git diff --exit-code`; branch probe (default and make backends); targeted pytest. | Pytest: 15 files (guard, borrowing, gh970, gh1190, recall_record, panel_ids_persistence, id_walk, derivations, categorical_mapping_resolution, schema_consistency, build_transform_hash, failure_is_not_absence, gh1278, gh1228, gh1210) gave **1,320 passed, 1 skipped**. `test_sample.py -k Malawi` gave 16 passed. 0 in-tree parquets. |
| **41238038** (rt1604-deriv) | Candidate cold build pickled; derivation join and metric probes; fresh-process warm vs cold. | Join bijection and rederivation exact. Warm equals cold exactly (4/4 calls). 0 in-tree parquets. |
| (login node, light) | Fingerprint probe on `git archive` trees (base, base+cand `local_tools`, cand−`country.py`, cand); vocabulary diff; `harmonize_food` diff; JSON analysis. | F3, F8, Q3 and Q1 static results above. |

---

## Must-state items for the PR body (rewrite it; it still says "draft under that hold" and cites old heads)

1. **Ruling.** The 2026-11-12 ruling "Lift hold, rebase, merge" covers this PR's content only. `weight` pools `hh_wgt` (IHS6, including the 815 refreshment households) and `wt_cross` (retained IHPS) in one wave column. `panel_weight` = `wt_long`. Decision 48's joint-inclusion question is **not** settled.
2. **Sample.** 2024-25 `sample` has 14,836 households: 12,888 `cs-24-` plus 1,948 retained. The 815 IHPS copies are dropped onto their IHS6 rows. `ihps_hhid`/`ihps_weight` cover the 2,763 IHPS 2024 households, and **`ihps_weight` is raw expansion scale** (F4).
3. **Crosswalk.** It adds 1,948 2024-25 links and changes no earlier entry. The guard moves 33 of the 2024-25 ids so that 25 re-mints of departed lineages' ids do not happen. 2019-20's own 10 re-mints remain (separate issue).
4. **`check_panel_consistency`.** It now FAILS `panel_ids_targets_exist` (1,948 2024-25 targets are not in `household_roster`, which is not wired for 2024-25) and warns on 2019-20→2024-25 attrition (F2).
5. **Borrowing.** 169,995 2024-25 rows carry borrowed `Quantity_kg`, stamped and fully recoverable through `derivation_inputs`. Refused and unsupported keys stay NA and fall to the existing inference ladder. Metric-labelled units are not borrowed. **The 5,814 active code-26 bucket source rows are rewritten to `u='kg'` at 5 kg per bucket** (density 1), unless F1 is fixed.
6. **Older-wave movement.** Adding the wave moves older waves' *derived* kilograms and kg prices through the pooled factor ladder (figures above). Native `food_acquired`, `sample`, `household_roster` and `community_prices` older waves are unchanged.
7. **Re-warm.** The `country.py` hunk forces a corpus-wide cold re-warm (every build fingerprint moves), and it guards only the no-committed-JSON fallback (F3). The re-warm has not been performed, and none is claimed.
8. **Labels.** The 2024-25 `harmonize_food` choices beyond item 119 (F6).

## Unverified limits

- I did not build Malawi tables other than `sample`, `household_roster`, `food_acquired`, `community_prices` and the three derived food tables. `crop_production`, `plot_*`, `livestock`, `shocks` and the rest are unverified for older-wave equality.
  - The risk is low: the `malawi.py` diff only adds functions and an import, and the `harmonize_food` readers are food-only or keyed by unchanged Preferred Labels.
- I did not check another country's *data* beyond the committed-JSON reads, and I did not run the full suite.
- The pytest selection had 1 skip, whose identity was not recorded (`-q`).
- Accuracy of the borrowed IHS5 factors against any external answer key is not assessed. The PR does not claim it either.
- The F1 size estimate (about 10,000 kg) uses the admitted-factor median; I did not rebuild with code 26 admitted.
- F3's "unreachable" was measured for `Country.panel_ids`/`updated_ids` on fresh roots under both backends. I did not exhaust every wave-level route; `Country['2024-25'].panel_ids()` reads the YAML block, and the mapping hook protects that route.
- Feature-level (`ll.Feature`) assembly with the new wave was not run.
