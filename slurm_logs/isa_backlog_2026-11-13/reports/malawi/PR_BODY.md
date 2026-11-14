## Malawi: later-wave cowpea from Module D, registered in every wave (GH #1228); the six empty Module H columns dropped (GH #1712)

**Merge condition: met.** The #1228 ruling (@ligon, 2026-11-12, "Join D, single-text only")
made a season check its precondition: do Modules D and G share the reference season? The
maintainer has **accepted it as met** (2026-11-13, "Accept as met"), on this evidence:
- **The household's reference season is exported**, in the household file: Module X
  `hh_x02`, "ENUMERATOR: WHAT WAS THE LAST COMPLETED RAINY SEASON?", in the 2016-17 and
  2019-20 cross-sections (and 2024-25). The Panel halves' Module X files have no `hh_x02`.
  No per-module D-0 / G-0 / I-0 answer is exported. The earlier statement that "the
  reference season is not exported" was wrong. It had not searched Module X (red team F1).
- **The forms tie the agriculture modules to it** (red team, quoted, not re-read here).
  IHS4 agriculture B_A-1: "REFER TO THE HOUSEHOLD QUESTIONNAIRE, MODULE X. WHAT WAS THE
  REFERENCE RAINY SEASON FOR THIS HOUSEHOLD?". IHS4 and IHS5 D-0, E00, F-0, G-0, H-0 and
  I-0 restate it, and the IHS5 BID (p. 18) and the DDI of entries 2936 / 3818 set one
  reference season per household, by interview date.
- **Measured here** (jobs 41275928, 41276124, reproducing the red team). The season of
  each G line's planting date equals `hh_x02` on 24,664 of 25,938 dated lines (2016-17
  XS) and 31,065 of 32,646 (2019-20 XS), about 95%. On the relabelled lines it is 228 of
  238 and 563 of 584. The Panels' Module X files have no `hh_x02`.
- **Red team only, not reproduced.** Where a module's own season answer is exported, it
  agrees with Module X for 7,536 of 7,724 households (IHS3 `ag_c0a`) and 8,857 of 8,957
  (IHS6 `ag_e00`), 97.6–98.9%. That bounds module-versus-household disagreement; nothing
  in the data dates Module D itself. The Panels rest on the forms and the IHPS 2016 BID.

### GH #1228: what changes
- **The join.** `malawi.module_d_other_crop_text` joins Module D question 20 onto G on
  (household, `gardenid`, `plotid`). It keeps a text only where there is one D row, one
  "Other Crop" text, one D code-48 slot and one G code-48 line. The text reaches
  `_harvest_block` / `_seed_block` as `crop_text`, the 2010-11 mechanism.
  `_CROP_TEXT_RECODE` gains the 2019-20 spelling `COW PEAS`. Local names stay unmapped.
- **Slot pairing (red team F2).** In 2019-20 XS, D20's labels are `48 OTHER SPECIFY 1`
  and `49 OTHER SPECIFY 2`, and the texts pair with them: `ag_d20_oth` names code 48's
  crop and `ag_d20b_oth` code 49's. That gives 963 plots with slot 48 only, 39 with slot
  49 only and 27 with both. A code-48 line now reads `ag_d20_oth` only, and
  `ag_d20b_oth` never feeds one; a new reason, `text_not_for_code_48`, covers the case.
  That fires on 0 lines today, so no served row moves by it. The earlier prose and test
  fixture ("two texts against one code-48 slot") are corrected.
- **Registration (ruling "Register all waves").**
  - Two keys, `Malawi::crop_production::cowpea-from-crop-text` and
    `Malawi::plot_inputs::cowpea-from-crop-text`, carry one rule
    (`derive_cowpea_from_crop_text`) and one inputs callable
    (`inputs_cowpea_from_crop_text`).
  - The inputs are the relabelled G lines under their original names, with `plot_id`,
    `source`, and (2016-17 / 2019-20) the plot's question-20 answers including
    `ag_d20_oth` / `ag_d20b_oth`. They are indexed `(t, i, crop)`.
  - The keys are stamped in 2010-11 (the #1523 rows, until now unregistered), 2016-17
    and 2019-20.
  - A row that already carries a sale-basis key gets both, `MULTI_SEP`-joined and sorted.
    `plot_inputs` has no other key today, but its stamp also unions with any `Derivation`
    already on the frame (red team D3).
  - A seed row is stamped only where it has a payload, so `seed-line-no-payload` is
    untouched.
  - `plot_inputs.Derivation` is declared `optional: true` in `lsms_library/data_info.yml`.
  - `_assert_label_is_text_only` refuses to stamp if a `harmonize_crop` code ever maps
    to `Cowpea (Khobwe)`.
- Code 48 stays the recorded code. No `harmonize_crop` row or synthetic code is added,
  per #1523 (coordinator default: the label stays declined).
- A sale attaches exactly as before. On a single-plot line it moves with the line. A
  Farm-level code-48 sale takes the label only where all its household's code-48 lines
  carry it (the #1457 rule).

**Measured, cold, private roots, `f138a3d61` → `51a918684` (burst job 41275716):**

| | 2010-11 | 2016-17 | 2019-20 |
|---|---|---|---|
| `crop_production` `crop` `Other` → `Cowpea (Khobwe)` | 0 (already, #1523) | 310 (302 harvest, 8 Farm-level) | 716 (697, 19) |
| `crop_production` rows keyed `cowpea-from-crop-text` | 160 | 310 | 716 |
| … of which also carrying a sale-basis key (`+`-joined) | 6 | 5 | 29 |
| `plot_inputs` seed rows relabelled / keyed | 0 / 155 | 301 / 301 | 697 / 697 |
| inputs (G lines) | 157 | 302 | 697 |
| sales now on a cowpea row | — | 30, 733,110 MWK | 78, 2,185,350 MWK |
| code-48 G lines left `Other` | 171 | 180 | 527 |

- Row counts are unchanged in every table, and 2013-14 is frame-equal.
- In `crop_production` only `crop` and `Derivation` move. `Derivation` moves on 1,186
  rows: 1,146 gain the key alone, and 40 have it joined to `sale-other-unit-single-row`
  (37) or `sale-basis-no-reference` (3). Every sale-basis ladder tally replays
  identically.
- In `plot_inputs` only `crop` moves (301 / 697). The frame gains `Derivation` and loses
  the six #1712 columns.
- **302 vs 301 (red team F8):** the one 2016-17 line with no seed row (`cs-17-103041550311`,
  `RG02_R01`) has no seed answer, so `seed-line-no-payload` deletes its seed row. In
  2010-11, 2 such lines give 157 vs 155.
- **Joins.** Every keyed row of the two tables joins its inputs (38 of 38 key × wave
  checks). The cowpea key's plot rows join on `(t, i, crop, plot_id)`, its Farm-level
  sales on `(t, i, crop)`, and its inputs are unique on `(t, i, crop, plot_id)`.
- **`derivation_inputs` of the 41 pre-existing key × wave frames, base vs candidate:**
  38 are equal, including every 2010-11 frame. **The 132 relabelled 2010-11 raw I/Q
  lines change none of the 41.** The other 3 differ only in `crop`, `Other` →
  `Cowpea (Khobwe)`: `sale-other-unit-single-row` 2016-17 (5 rows) and 2019-20 (26), and
  `sale-basis-no-reference` 2019-20 (3). These are exactly the rows that now carry both
  keys.
- `harvest_kg` keeps the same wave kilograms with and without the shipped factors; only
  its `j` split moves. `seed_kg`, `nitrogen_kg` and `gross_crop_revenue` are frame-equal.
  `crop_diversity` rises on 7 / 52 households.
- **Site Q (red team F3).** `crop_production` is unchanged: 28 offenders, identical.
  **`plot_inputs` goes 73 → 72** (2019-20 25 → 24):
  - When the cowpea seed rows leave the (2019-20, `Other (Specify)`, Seed, `Other`) cell,
    its reference rises from 4.0 to 19.6. So `cs-19-102031310144` `RG03_R01` (450)
    leaves the warning.
  - The 2,016 kg row `cs-19-307106330251` `RG02_R01` moves to the cowpea cell. It is now
    judged at 672× a reference of 3.0, against 403× of 5.0 before.
- **Pre-existing, not changed here: code 49 (red team F2).** `harmonize_crop` lacks code
  49. So the 66 2019-20 XS G lines coded 49 are dropped from `crop_production` on both
  arms (GH #869 warning), and `plot_inputs` serves their seed rows as crop `'49'`. Their
  `ag_d20b_oth` says `COW PEAS` on 41.

**2024-25 is measured, not relabelled** (outside the ruling; `crop_production` is not
wired). IHS6 adds `49 OTHER (SPECIFY) 2` and `50 COW PEAS (KHOBWE)`; 1,223 + 273 G rows
use code 50. The single-text `COW PEAS` residue on code 48 is 106 + 17 rows. Mapping code
50 to `Cowpea (Khobwe)` would trip `_assert_label_is_text_only` in every wave.

### GH #1712: the six dropped (ruling "Drop the six")
The six columns (`ag_h17_os`, `ag_h26b_os`, `ag_h27_os`, `ag_h36b_os` in 2010-11 H;
`ag_h26_1b`, `ag_h36_1b` in 2019-20 Panel H) are present under their release names and
labels. Each is empty on every row of the one source that carries it, read raw and
through `get_dataframe`. There is no wiring error. `_seed_acquisition_block` no longer
carries them (`_H_EMPTY_IN_RELEASE`), but only while each stays blank on every row of its
file, so a release that fills one brings it back. `CONTENTS.org` cites the empty source.
`test_table_structure -k Malawi` now passes; base keeps its two `plot_inputs` failures.

**The condition reads the file, not the carried rows** (red team D2, kept as is). A
release that fills one of the six only on rows the carrier does not keep brings the
column back, blank on every served row, and `no_all_null_columns` then fails again. That
failure is loud, not silent. A synthetic test pins the rule through
`_seed_acquisition_block` (red team D1):
- blank on every row (NA, `''` or whitespace) → dropped;
- filled on a carried row → served under its own name;
- filled only on a row not carried → served blank;
- `ag_h33`, not one of the six → served however blank.

The red team also saw three columns blank in the served frame. They were checked and
**not dropped**:
- **`ag_h33`** ("source of credit for this seed purchase") is blank on every row of 2010-11
  H (24,009) and 2013-14 H (8,524). It is stored as `''`, so nothing fails on it.
- **`ag_h36b_oth`** ("Specify Unit") is blank on every row of 2016-17 Panel H (4,745) and
  2019-20 Panel H (7,327). Its parent is never coded Other.
- **`ag_h42b_os`** is **not** empty in the release. It has 54 texts (`CUP`, `PAIL`, …),
  all on left-over-seed-only rows with no purchase or gift quantity. The #1190 carrier
  serves acquisitions, so it carries none of the 54. That is #1190's scope, not a wiring
  error.

### Cache cost (hash census `f138a3d61` → `51a918684`, job 41275716)
Malawi only:

| kind | moved |
|---|---|
| `Country._table_cache_hash` | 26 of 26 Malawi (555 probed) |
| `Wave._input_hash` | 156 of 156 Malawi (2,321 probed) |
| `build_transforms_fingerprint` | 0 of 42 |
| `framework_imports_fingerprint` | Malawi only (1 of 38) |

**The `framework_imports_fingerprint` move (red team F4).** Malawi's moves by exactly one
closure part, `local_tools._dvc_working_directory`, which `read_module_d_columns` reaches.
`_ensure_dvc_pulled` and `_dvc_cache_path` were already in Malawi's closure, and
`DVCFS` / `_COUNTRIES_DIR` are not callables. That is the red team's probe at
`af8178463`, re-run at `51a918684` (job 41276124) with the same result. A later edit to
that helper re-warms all of Malawi.

At the head `5093b4939` (job 41277780), against base, the figures are the same, Malawi
only: tables 26 / 555, waves 156 / 2,321, `build_transforms_fingerprint` 0 / 42,
`framework_imports_fingerprint` 1 / 38. Against `9ca3518a3`, the D3 edit in `malawi.py`
moves Malawi's table (26) and wave (156) hashes again and nothing else; it moves no
fingerprint. Cost: one Malawi re-warm.

### Changes since `af8178463` (the red-teamed head)
- `51a918684`: the registration (both keys, the inputs callable, `MULTI_SEP` stamping,
  the `data_info.yml` declaration); the F2 slot pairing; the #1712 drop; tests for all
  three.
- `9ca3518a3`: `CONTENTS.org` rewritten for F1–F4, F8, the registration and the drop;
  `derivations.yml` evidence corrected; the sale-basis, #1457 and #1228 delivered tests
  made key-aware.
- `5093b4939` (red team delta, APPROVE):
  - D1: a synthetic test pins the conditional #1712 drop.
  - D2: CONTENTS states the every-row-of-the-file condition.
  - D3: the `plot_inputs` stamp unions with an existing `Derivation`.
  - D6: the `plot_inputs` entry's cross-referenced assumptions take the
    `crop_production` entry's bases: `questionnaire` for the text, the pairing and the
    season, and `modelling-choice` for single text only.

### Tests
**At the head `5093b4939`** (job 41277780, cold private root): the touched tests and the
sale-basis tests. Files: `test_gh1190_selected_farm_carrier`, `test_derivations`,
`test_gh1228_malawi_cowpea_label`, `test_no_build_input_in_contents_org`,
`test_malawi_sale_basis`, `test_gh1457_malawi_farm_level_sales`.
- **311 passed, 0 skipped, 0 failed.**
- Synthetic gate: 177 passed.
- `test_table_structure -k "Malawi and plot_inputs"`: 6 passed.
- Malawi `plot_inputs` built cold on `9ca3518a3` and on `5093b4939` is frame-equal:
  222,662 rows, with identical columns, dtypes and warnings. D3 moves no served row.
- 0 in-tree parquets.

**At `9ca3518a3`**, the full set: burst job 41276341, cold private root,
`-n 7 --dist=loadfile`. The
64 files are the 37 `tests/test_*malawi*.py` plus 27 that touch Malawi crop and input
tables or the machinery: `test_derivations`, `test_schema_consistency`,
`test_build_transform_hash`, `test_gh1190_selected_farm_carrier`, `test_quantity_screen`,
`test_no_build_input_in_contents_org`, … (full list: `reports/malawi/out/verify-v5/pytest_files.txt`).
- `9ca3518a3`: **2,499 passed, 14 skipped, 0 failed.** The 3 extra skips are 4 cache-gated
  `test_863_transforms` cases (Malawi `crop_production` ×2, `plot_inputs` ×2) that
  pass only on a warm root. They passed in job 41275716's warm root at `51a918684`,
  which differs from `9ca3518a3` only in tests and docs.
- `51a918684` (job 41275716): 2,497 passed, 5 failed, 11 skipped. Four were delivered
  tests that compared `Derivation` exactly (`test_gh1457` ×3, `test_malawi_sale_basis`
  ×1). The fifth was the new #1228 test writing to a read-only array. All five are fixed
  in `9ca3518a3`.
- Base `f138a3d61`: 2,467 passed, 11 skipped.
- Gate at `9ca3518a3` (synthetic, 7 files): 255 passed.
- `tests/test_table_structure.py -k Malawi`: `9ca3518a3` 128 passed, 6 skipped. Base has 2
  failures, `Malawi/plot_inputs` `test_no_fully_null_columns` and `test_feature_is_sane`;
  the #1712 drop removes both.
- Hash census at `9ca3518a3`: 0 of 555 / 2,321 / 42 / 38 moved against `51a918684`, and the
  Malawi-only figures above against `f138a3d61`. 0 in-tree parquets.
- The canonical `data_info.yml` declaration at read time (job 41276868): Uganda, Nigeria
  and Niger `plot_inputs` are built cold on both arms and come out frame-equal (84,351 /
  96,620 / 43,620 rows), with identical columns, dtypes and warnings; no `Derivation`
  column is added.

Not run:
- the full base-against-candidate frame comparison at the head. It is from `51a918684`.
  `9ca3518a3` added only docs and tests (0 hashes moved), and `5093b4939`'s one code
  edit (D3) is in `assemble_plot_inputs`, whose served frame is equal to `9ca3518a3`'s.
  At `5093b4939`, `crop_production` was built and tested but not frame-compared;
- the full 64-file test set at `5093b4939`; it ran at `9ca3518a3`;
- the make backend;
- a warm or fresh-process read, or `Feature()`;
- the other 24 Malawi tables.

F8's 2016-17 Panel same-day denominator is still unreconciled.

Test changes:
- `test_gh1228` covers the slot-pairing fixture (corrected, plus a `text_not_for_code_48`
  case), the key union and stamping, the registry entry, and a source-gated check that
  the key sits on exactly the relabelled rows and joins its inputs (plot rows on
  `plot_id`).
- `test_malawi_sale_basis` and `test_gh1457` count ladder keys apart from the crop-text
  key.
- The `test_gh1190` source inverse names the new definitions. Its fixture is untouched.
- Unchanged from the first round: the `test_malawi_cassava_recode` re-pins, and the D20
  columns added to the invented frames in `test_1111` / `test_gh1190`.

### Left open, for a decision
- **One key or two.** The ruling says "one derivation key". There are two: one rule, one
  function and one inputs callable, registered under a key per table. The key grammar is
  `country::table::name`, and an empty table slot (`Malawi::::cowpea-from-crop-text`)
  would attach the entry to every Malawi table, at 0 rows where it does not apply. The
  red team measured each key only in its own table's `attrs['derivations']`, and
  recommends two. Confirm two, or rule for one.
- 15 of the 27 two-text 2019-20 XS plots name cowpea in `ag_d20_oth`, code 48's own text.
  The pairing makes them unambiguous, but the ruling's "exactly one text" keeps them
  `Other`. On 10 more, only `ag_d20b_oth` says cowpea, which names a code-49 line.
- The 66 code-49 lines (41 `COW PEAS`) are lost from `crop_production`, before and after.
- `ag_h33` and `ag_h36b_oth`: drop them like the six?
- The `test_gh1190` source pin also exempts `assemble_plot_inputs` now (red team D5).
  The fixture README forbids refreshing the baseline, so this is the only option, but it
  narrows the pin.

### Drafted residual issue: Malawi crop text after #1228 (2024-25, code 49, leftovers)
- **2024-25 (IHS6 / IHPS 2024) is not relabelled**, and `crop_production` is not wired
  there.
  - IHS6 Module G has its own code, `50 COW PEAS (KHOBWE)`, on 1,223 XS and 273 Panel G
    rows. Beside it are `48 OTHER (SPECIFY) 1` and `49 OTHER (SPECIFY) 2`, paired with
    D's two texts (160 / 9 plots carry both).
  - The single-text `COW PEAS` residue is 106 / 17 plots on a 48-only plot, and 7 / 0 on
    a code-49 plot.
  - The season answers are exported: `hh_x02`, and Module E's own `ag_e00`.
- **Hazard (red team D4).** The cowpea stamp reads the label as "set from a crop text".
  If code 50 is mapped to `Cowpea (Khobwe)` in the country-level `harmonize_crop`,
  `_assert_label_is_text_only` raises and stops every Malawi `crop_production` and
  `plot_inputs` build, in every wave, not only 2024-25. Wiring IHS6 code 50 therefore
  needs a stamp keyed on the code (or on the crop-text column), not the label, first.
- **Code 49, 2019-20 XS (pre-existing).** `harmonize_crop` has no code 49. So
  `crop_production` drops all 66 G lines coded 49 (GH #869 warning), and `plot_inputs`
  serves their seed rows as crop `'49'`. Their `ag_d20b_oth` says `COW PEAS` on 41,
  `CUCUMBER` 6, `NKHAKA` 6, `MUCUNA` 3, `MPHONDA` 2, `NZAMA` 2, plus six singletons.
- **Left `Other` by the single-text ruling:**
  - the 27 two-text 2019-20 XS plots: on 15 of them code 48's own text says cowpea, on
    10 only code 49's does;
  - one `COWPEAS(NSEULA)`;
  - one `COWPEAS` on a D row without a code-48 slot;
  - 2013-14, which holds no text.

Closes #1712. Refs #1228 (the ruled scope is done; the residual is drafted).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
