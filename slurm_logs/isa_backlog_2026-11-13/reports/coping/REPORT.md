
# GH #1562 coping contract: implementation (stream `coping`)

**Branch** `feature/1562-coping-set-20261113`, base `f138a3d61`, **head `2ee44de32c1d13ad8f5c1b4ea13ac3c5b7e9e847`**. Every commit printed `SUCODER: published`.

| commit | what |
|---|---|
| `2187a3c992003cd836bafebe1331f6a3ea554ee2` | The 0-hash layer: `lsms_library/coping.py`, called from `Country._finalize_result` after `dropna(how='all')`; `_EXCLUDED_CALLABLES` entries; the canonical `shocks` schema; 15 `countries/<C>/_/coping.yml`; `tests/test_coping.py`. |
| `6a6b1618bf360ff4a8f294639864c3bbb7875a7b` | Build-time changes: the EHCVM 2018-19 grid set (`ehcvm.coping_set_from_grid`); CIV's hook moved to its wave; GNB codes 18-25 relabelled; option 25 registered for Benin and Niger 2021-22; Nigeria quarter pins, `HowCoped3`, W4 set; Liberia's `ForestProduct0..2`. |
| `6bdeea29ebb212b39e3d686abe4886b94afe3569` | CONTENTS sections for 16 countries; Guatemala's stale line; corrected Nigeria assets note. |
| `9abab1855454d4e20cece07f05b9bef788f1a0a0` | Set-only hooks write HowCoped0..2 present and NA, so an empty single-quarter answer keeps its schema. |
| `0bc56ada8b36ae0046523f241205eadc92a346b1` | Exact 2018 grid oracle test; Malawi "Dont know" basis; Mali hollow rows recorded. |
| `3336ffc99f9d435dee2fe75ef382432cf781a2ff` | `test_gh1500` Senegal AST pin re-pinned. |
| `2ee44de32c1d13ad8f5c1b4ea13ac3c5b7e9e847` | **Follow-ups**: W1 assets NA columns; Uganda's slot-aware class; F1, F4 and F9. |

Rulings implemented (`isa-decisions.org`):
- **2026-11-13, "Coping follow-ups"**: "Pin + W1 NA columns"; "Did nothing when alone".
- **2026-11-13, "Coping-strategy implementation, GH1562"**, every bullet.
- **2026-11-12, "Coping-strategy contract"**.
- The coordinator default: GNB codes 18-25 follow the Portuguese questionnaire.

## Changes since 3336ffc99 (red team CHANGES, `reports/coping-redteam/VERDICT.md`)

- **F2, ruled "Pin + W1 NA columns".** Assets stays de-duplicated. W1 now serves `Age` and `Value` as NA, with no `optional:`.
  - **File:** a new wave module, `lsms_library/countries/Nigeria/2010-11/_/2010-11.py`, holding an `assets` hook.
  - `Wave.formatting_functions` loads it beside `mapping.py`, and `Wave.data_scheme` excludes the wave-folder stem, so it declares no table.
  - No other stream edits this file. It avoids W1's `data_info.yml` (edited by #1713) and `mapping.py` (edited by the food streams).
  - **Measured cold and warm** (job 41280195, head `2ee44de32`). Each request was made cold in an empty root, then after an all-waves build in a fresh process:

    | request | cold → warm |
    |---|---|
    | `assets(['2011Q1'/'2013Q1'/'2016Q1'/'2019Q1'])` | 0 → 0 |
    | `assets(['2010Q3'])` | 38,947 → 38,947 |
    | `shocks(['2010Q3'/'2018Q3'])` | 0 → 0 |

    Base gives the duplicated rows both ways. Before this fix, `assets(['2011Q1'])` raised cold and gave 0 rows warm.
  - **Finding:** W1's per-unit roster `sect5b_plantingw1` (56,816 rows) carries `s5q3` "how long ago was item acquired? (years)" (56,755 non-null) and `s5q4` "if you wanted to sell one of this today, how much would you…" (56,588 non-null). Those are W2's Age and Value variables. **So the ruled NA means "not wired", not "not asked".** See Q1.
- **F3, ruled "Did nothing when alone".** A new slot-aware registry class, `did_nothing_when_alone_first`, carries a `served_as` label.
  - Uganda 2013-14 / 2015-16 code 40 "Nothing else", as the first answer with nothing else recorded (later slots empty or 40): 55 / 41 rows.
  - **Decision: the position is served as "Did nothing" as well as the set.** Position and set then agree, as for every other lone "did nothing" answer, and the label is Uganda's own (2018-19's). The parquet keeps "Nothing else". NA would contradict "alone, the set is {did nothing}, distinct from NA".
  - Elsewhere code 40 stays a filler (NA): later slots, 628 / 697 rows. It also stays a filler as the first answer followed by a strategy (3 / 3 rows), giving a recorded gap; the ruling does not cover this case, and I applied the filler default.
  - The loader requires `served_as` to be a `did_nothing` label for every wave of the entry. It allows filler plus slot-aware for one label, and refuses did_nothing plus slot-aware.
- **F1.** `test_gh1562_coping_build.py` now asserts `ph in _pinned(info['individual_education'])`.
- **F4.** GNB CONTENTS now cites the within-country 2021-22 comparison (bétail 24.8 vs 27.9, spirituelles 2.2 vs 1.9, Aucune 177.8 vs 137.9 per mille). It says the cross-country evidence is mixed (it favours the old labels at codes 18 and 22), that 23-24 are inferred, and that 2,199 rows are relabelled.
- **F9.** Corrected:
  - Ethiopia `coping.yml` and CONTENTS: W1 '0.0' ×10 plus '24.0', '25.0', '60.0' (13 cells); '21.0' is W2 only.
  - CIV CONTENTS: "never three positives in one row".
  - `tests/test_assets_additive.py` docstring.
- **Not done, per the instructions:** F5 (GNB 2021-22 option-25 stamp on 3 rows; GNB `optional:` positions) and F6 (Windows pin re-measure) are left for Sue at integration. No GNB 2021-22 file was edited.

## 1. What changed

- **Contract** (`data_info.yml`, `shocks`):
  - HowCoped0..3 are the reported order: NA where only which-strategies is recorded, never code order.
  - `HowCopedSet` is distinct labels, sorted, `+`-joined; test membership by split, never substring.
  - New optional columns: `HowCoped3`, `HowCopedSet`, `Derivation`, `ForestProduct0..2`.
- **Read time** (`coping.py`, 0 hashes): on every `shocks` read, after the dropna step (no row is ever deleted):
  - the slot-aware "did nothing" rule;
  - fillers and missing codes NA in every position;
  - the set taken from the build where it wrote one, else from the positions, with the filler and "did nothing" rules applied to both.
  - Labels are per country in `_/coping.yml`, matched exactly, with evidence. The parquet keeps the build output.
  - **The wave API (`Country(C)[w].shocks()`) does not reach these rules**, as for kinship (F11).
- **EHCVM 2018-19** (8 countries): `ehcvm.coping_set_from_grid` writes the set from `s14q05__N` and HowCoped0..2 present and NA.
  - Chosen = a positive cell (1, 2 or 3); 0 = offered, not chosen; NA = not asked; anything else raises `CopingGridError`.
  - The evidence is the CAPI ordered multi-select plus the data's structure, and it is base's own `>=1` reading. The meaning of 3 is not documented.
  - CIV's hook moved to `2018-19/_/mapping.py`.
  - GNB: codes 18-22 and 25 follow the Portuguese workbooks; 23-24 are unprinted and assigned by elimination. Positions are `optional`; `HowCopedSet` is declared.
- **EHCVM 2021-22 option 25:** Benin and Niger keep the inferred position (`ehcvm.derive_coping_autre_rank`) and stamp `<C>::shocks::code25-rank-inferred` beside a ranked strategy: Benin 13 rows, Niger 17. It is registered, with inputs returning the raw `s14bq05__1..26`. The hooks are per-country copies, so GNB 2021-22 does not inherit this (F5).
- **Nigeria:**
  - `shocks` is pinned to the post-harvest quarter and `assets` to the post-planting quarter (same defect, rows identical on base).
  - W1-W3 gain `HowCoped3 = s15aq5d`.
  - W4 (select all that apply) gets a set plus NA positions.
  - W1 assets gains NA `Age`/`Value`.
- **Liberia:** `S17_5_A/B/C` become `ForestProduct0..2`, with "no other product" NA; there is no HowCoped column and no set.
- **Guatemala:** CONTENTS only.

## 2. Idiosyncrasies named, per country

I read the shocks passages and the related TODO/WAITING entries of each touched CONTENTS.org, not all ~45,000 lines.

- **Benin.** Split 14.04 layout by vague (#1561). At most 2 strategies in 2018-19. 2021-22 serves 4,756 of 10,982 shocks (follow-ups only for the last 12 months). Option 25 is Oui/Non, 13 of 34 rows beside another strategy.
- **Burkina.** Vague layout and an always-empty third slot in 2018-19. Raw ranks in 2021-22, including option 25. In 2014, "Aucune" fills later slots (146 rows).
- **CIV.** The hook was in the country module (126 wave hashes per edit). `.d` missing-value transport (#1561). Damaged 2021-22 keys (44 rows collapsed). Option 25 on 15 rows, all alone.
- **GNB.**
  - Portuguese instrument and answers; six-column 14.04.
  - Options shifted by one after dropping "Louer", with 23-24 unprinted.
  - 47 (1,1) rows; 1,540 "Nenhuma estratégia" rows, never beside another option.
  - 2018-19 is the only wired wave.
- **Mali.** 2014-15 "Manquant" (177 cells; its variable labels say "CHOC"); 2017-18 "Aucune" fills unused slots; 2018-19 option 26 beside other options on 444 rows; option 25 on 72 rows, all alone.
- **Niger.** 2011-12 unlabelled 99 (317 cells) and "Nothing" (code 24); 2014-15 "Manquant" (207 cells); option 25 on 52 rows, 17 beside another strategy.
- **Senegal.** Option 26 beside others on 37 rows. 2018-19 `mapping.py` is AST-pinned (`test_gh1500`).
- **Togo.** Option 25 on 26 rows, all alone; split 2018 layout.
- **Nigeria.**
  - No shocks passage before (added).
  - "A block reads one file and is stamped with every round of its directory."
  - The W1-W3 labels call the question a "consequence".
  - W4 is select-all.
  - W2 has unlabelled codes 31/41/51.
  - W2 assets Value has been summed per unit since #629, so the "KNOWN OPEN DEFECT" now covers Age only.
  - W1 holds Age and Value in `sect5b`, not wired.
- **Liberia.** Forest-proximate NHFS. Products, not strategies. S17_3/S17_4 unwired.
- **Guatemala.** One `accion` per problem. 1,885 shocks without one (audit and docstring disagree on why).
- **Uganda.** Code 40 is in the value labels but not the printed list; 96 stays a strategy; the 2005-06 relabel folds (F6).
- **Malawi.** 2004-05 code 99 "No other response given"; "Dont know".
- **Ethiopia.** Two label spellings (W1-W3 against W4-W5); unlabelled W1/W2 codes.
- **Tanzania.** R1/R2 have no coping in the UPD release; 2 slots in 2019-20 and 2020-21.
- **Iraq.** "Did not do anything" beside a strategy on 412 rows.

## 3. Measurements

All cold, base `f138a3d61` against candidate, in separate private empty roots.

**Heads and jobs:**
- **Nigeria and Uganda** at `2ee44de32`: job 41280196 (`out/measure-k1/`) and the cold/warm job 41280195 (`out/ppreq3/`).
- **The other 15 countries** at `9abab1855`: jobs 41276486-89 (`out/measure-h{1..4}/`). The follow-ups change behaviour only where a registry declares the new class, which is Uganda alone.
- Earlier jobs (g-runs 41276277-80) reproduce.

**Every non-coping column is identical on common rows in all 17 countries.** Warm reads equal cold, with no `'nan'` strings and string dtypes. `Feature('shocks')` was run per group: unique index, set present for all 15 coping countries (Albania and Liberia have none by design). Derivation inputs join: Benin 13/13, Niger 17/17.

| Country | rows | what moved |
|---|---|---|
| Benin | 17,061 = | 2018-19 positions → NA (14,809 cells / 12,305 rows); set 1:9,802 2:2,503. 2021-22: 13 rows keyed. |
| Burkina_Faso | 24,837 = | 2018-19 → NA (10,235 cells / 8,765 rows). |
| CotedIvoire | 21,306 = | 2018-19 → NA (15,975 / 13,778). |
| Guinea-Bissau | 9,487 = | 2018-19 → NA (11,166 cells); set 1:7,813 2:1,669 3:5; **2,199 rows relabelled (codes 18-25; 23-24 inferred)**. |
| Mali | 26,774 = | 2014-15 "Manquant" 177 cells → NA on 71 rows (3 rows left empty); 2018-19 → NA (11,917 / 9,837), option 26 dropped from 444 sets. |
| Niger | 17,506 = | `'99.0'` 317 cells / 181 rows; "Manquant" 207 / 141; 2018-19 → NA (7,682 / 6,551); 17 keyed. |
| Senegal | 10,313 = | 2018-19 → NA (6,627 / 6,131); option 26 dropped from 37 sets. |
| Togo | 9,896 = | 2018 → NA (7,829 / 7,230). |
| Nigeria | 22,550 → 11,277 | −11,275 rows (exactly the post-planting copies); +2 in 2011Q1 (only slot 4 recorded); HowCoped3 on 274 / 165 / 150 cells; 2019Q1 → NA (4,913 / 3,870). **assets 865,318 → 432,659**, Value totals unchanged (2012Q3 N576,299,043). |
| Liberia | 1,358 = | → ForestProduct0..2; "no other product" 577 cells → NA on 342 rows. |
| **Uganda** | 14,459 = | "Nothing else" → NA on 980 / 1,024 cells (2013-14 / 2015-16); **"Nothing else" → "Did nothing" on 55 / 41 rows** (set size 0 → 0; size 1: 445 / 318); `'99.0'` 1 cell (2009-10). Feature set count 14,072 → 14,168. |
| Malawi | 104,691 = | 2004-05: 49,158 + 185 filler cells → NA on 27,053 rows (26 rows left empty). |
| Ethiopia, Tanzania, Iraq, Guatemala | = | The set only. |
| Albania | = | Nothing. |

Per-wave fill and set sizes are in `out/per_wave_movement.tsv` (the h-runs). Row details are in `out/details.json`.

## 4. Hash census, base `f138a3d61` → head `2ee44de32` (job 41280197)

| kind | moved |
|---|---|
| `build_transforms_fingerprint` | **0 / 42** |
| `framework_imports_fingerprint` | 0 / 38 |
| `Country._table_cache_hash` | 218 / 555 |
| `Wave._input_hash` | 538 / 2,321 |

Per country, tables and wave-tables:

| Country | tables | wave-tables |
|---|---|---|
| Benin | 22/22 | 44/44 |
| Burkina_Faso | 25/25 | 25/75 |
| CotedIvoire | 21/21 | 126/126 |
| Guinea-Bissau | 22/22 | 22/44 |
| Liberia | 10/10 | 10/30 |
| Mali | 24/24 | 24/96 |
| Niger | 25/25 | 50/100 |
| Nigeria | 24/24 | 192/240 |
| Senegal | 23/23 | 23/46 |
| Togo | 22/22 | 22/44 |

- Uganda, Malawi, Ethiopia, Tanzania, Iraq and Guatemala: 0. That includes Uganda's new rule (read path).
- Identical to the census at `3336ffc99`: `2010-11.py` adds to Nigeria hashes that already moved.
- Nothing moved outside the expected set.
- CIV's 126 come from removing the hook from its country module (one-time).
- **Cost:** a cold re-warm of these 10 countries.
- The red team's probes: `coping.py` and `coping.yml` edits move 0; `coping_set_from_grid` moves exactly the eight 2018-19 waves; `derive_coping_autre_rank` moves Benin and Niger 2021-22.

## 5. Tests (head `2ee44de32`)

| run | result |
|---|---|
| Full static suite `-m "not requires_s3"` (job 41280198) | 15,598 passed, 35 skipped, 4 xfailed, **1 failed: `test_gh932_safe_ci_failure_locations.py::test_real_wrapper_optin_…`**. It is a parallel-run flake unrelated to this diff: it passed at `3336ffc99`, and passes 54/54 three times serially at `2ee44de32` (job 41280727). |
| Static suite at `3336ffc99` (job 41277113) | 15,587 passed, 0 failed. Base: 15,503 passed. |
| Data-backed group A (shocks, coping, derivations, schema, build-hash, assets files; job 41280199) | **1,001 passed, 11 skipped** (no-filler countries). Base without the new files: 873 passed. |
| Group B (`-k "shock or Shock or assets"`) | 94 passed, 164 skipped (no cache for other countries). Base: 76 passed. |
| `test_table_structure -k "Nigeria and (assets or shocks)"` with a built cache (`0bc56ada8`, job 41276905) | 12 passed |

New tests:
- **Slot-aware rule:** lone first slot; first followed by a strategy; later slot; wave scope; idempotence; five registry refusals; Uganda's declaration.
- **Nigeria W1 assets:** hook columns with no `optional:`; the wave module declares no table; post-harvest single-quarter answers are 0 rows with their columns (`requires_s3`).
- **Uganda served data:** 55 / 41 rows (`requires_s3`).
- Updated: F1's membership assertion.

Not run:
- the full `requires_s3` suite;
- re-measurement of the ~200 other tables whose hashes moved (only `shocks` and Nigeria `assets` were compared).

## 6. Issue status and residual drafts

**#1562: closes on merge for the ruled contract, with the residuals below.** The W1 assets regression is resolved by ruling.

1. **[C] Nigeria W1 Age/Value are recorded but not wired.** `sect5b_plantingw1` carries `s5q3` and `s5q4`, the same variables and labels W2 uses for Age and Value. Wiring them would mean a `dfs:` merge like W2's, with Value summed per unit. It edits W1's `data_info.yml`, which #1713 also edits.
2. **[C] EHCVM 2021-22 option-25 stamp is not shared.** GNB 2021-22 needs it on 3 rows (F5). CIV, Mali, Senegal and Togo have 0 rows today; add a stamp or a guard.
3. **[C] Liberia drops 2,164 of 3,522 experienced shocks** (S17_3/S17_4 unwired).
4. **[C] Tanzania R1/R2 coping is all NA in the UPD release.**
5. **[C] Uganda 2005-06 relabel folds two code pairs.**
6. **[C] "Other, specify" text unwired**: Ethiopia `s9q04_os`, Malawi `hh_u04*_os`/`ab07*oth`, Nigeria `s15aq7_os`.
7. **[coverage, for Sue] 8 new `dropped` cells** (F8): Nigeria shocks 2010Q3/2012Q3/2015Q3/2018Q3 and assets 2011Q1/2013Q1/2016Q1/2019Q1.
   - Draft `correctly-empty` rows, not written to the shared file:
     - **Shocks, post-planting quarters.** Section 15A is post-harvest only. W1's planting contents (`Panel_Household_Questionnaire_FINAL.pdf` p4) list sections 1-10 with none on shocks. For W2-W4, full-text searches of the planting questionnaires find no shocks heading.
     - **Assets, post-harvest quarters.** W1/W2 post-harvest section 7 is "Household assets sale and acquisition" (transactions, not holdings; a judgement call). W3's contents say "7 HOUSEHOLD ASSETS MOVED TO PP". W4's post-harvest contents list no assets section.
   - The EHCVM 2018-19 cells and Nigeria 2019Q1 stay `sane`, like `Tanzania,shocks,2008-09`. No affected cell is blessed.

## 7. Questions for the user

1. **Wire Nigeria W1 Age/Value from `sect5b_plantingw1`** (residual 1), or keep the ruled NA?
2. **GNB 2018-19 codes 23-24** are unprinted and assigned by elimination (contre saison, Autre). Confirm.
3. **EHCVM 2018-19 "chosen = a positive cell".** It is base's own reading, and the meaning of 3 is undocumented. Confirm.
4. **Unlabelled numeric codes** are kept as recorded: Ethiopia W1 '0.0' ×10, '24.0', '25.0', '60.0'; W2 '21.0'; Nigeria W2 '31.0', '41.0', '51.0'. Serve them NA as missing codes?
5. **Malawi "Dont know"** (185 cells, all in the first slot) is classed as a missing code. Confirm.
6. **Hollow rows** (Mali 3, Malawi 26) are kept, not deleted. Keep?
7. **Liberia's `ForestProduct0..2` names**: review.
8. **The read-time recode is not a registered derivation** (the read path cannot stamp). Are the schema note and `coping.yml` sufficient?
9. **Option-25 stamp or guard for CIV, Mali, Senegal, Togo** (0 rows today)?
10. **GNB `optional:` positions** at integration (F5).

## 8. Disclosures

- **I ran system `python3` on n0035 twice**: a JSON read with no `lsms_library` import, and one empty-stdin call that did nothing. After that I used `jq`.
- I read the shocks passages of the CONTENTS files, not every line.
- GitNexus was unavailable, so I swept call sites with `rg`.
- The wave API bypasses the read-time rules (§1).

